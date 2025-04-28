mod book_order;

use book_order::{BookOrder, DepthUpdateError};
use eyre::Result;
use futures_util::{Stream, StreamExt, stream::select_all};
use std::pin::Pin;
use tokio::{net::TcpStream, sync::mpsc, sync::mpsc::error::TryRecvError};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
};

async fn ws_connect(url: &str) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    ws_stream
}

struct DepthUpdate {
    first_update_id: u64,
    last_update_id: u64,
    value: serde_json::Value,
}

fn parse_msg(message: Message) -> Option<DepthUpdate> {
    if let Message::Text(text) = message {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(last_update_id) = json.get("u").and_then(|u| u.as_u64()) {
                if let Some(first_update_id) = json.get("U").and_then(|u| u.as_u64()) {
                    return Some(DepthUpdate {
                        first_update_id,
                        last_update_id,
                        value: json,
                    });
                }
            }
        }
    }
    None
}

async fn init_updates_stream(url: &str, num_of_streams: usize) -> impl Stream<Item = DepthUpdate> {
    let mut streams: Vec<Pin<Box<dyn Stream<Item = DepthUpdate> + Send>>> = Vec::new();
    for _ in 0..num_of_streams {
        let ws = ws_connect(url).await;
        let read = Box::pin(ws.filter_map(|msg| async { msg.ok().and_then(parse_msg) }))
            as Pin<Box<dyn Stream<Item = DepthUpdate> + Send>>;
        streams.push(read);
    }
    select_all(streams)
}

async fn get_snapshot(symbol: &str, limit: i16) -> Result<serde_json::Value> {
    let params = [("symbol", symbol), ("limit", &limit.to_string())];
    let client = reqwest::Client::new();

    let response: serde_json::Value = client
        .get("https://api.binance.com/api/v3/depth")
        .query(&params)
        .send()
        .await?
        .json()
        .await?;

    Ok(response)
}

async fn init_book(min_update_id: u64, symbol: &str, limit: i16) -> Result<BookOrder> {
    loop {
        let snapshot_json = get_snapshot(symbol, limit).await?;
        if let Some(update_id) = snapshot_json.get("lastUpdateId").and_then(|u| u.as_u64()) {
            if update_id >= min_update_id {
                return BookOrder::new(&snapshot_json);
            }
        }
    }
}

fn handle_update(book: &mut BookOrder, update: &DepthUpdate) {
    if let Err(err) = book.depth_update(&update.value) {
        if let Some(depth_err) = err.downcast_ref::<DepthUpdateError>() {
            match depth_err {
                DepthUpdateError::ParsingError => {
                    println!("Ошибка парсинга обновления книги");
                }
                DepthUpdateError::InvalidBook => {
                    println!(
                        "Ошибка: некорректная книга заявок. Книга: {} ",
                        book.last_update_id(),
                    );
                }
            }
        } else {
            println!("Unknown error");
        }
    } else {
        println!(
            "Update applied succefully. New id {}",
            book.last_update_id()
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut streams =
        init_updates_stream("wss://stream.binance.com:9443/ws/btcusdt@depth", 3).await;

    let (tx, mut rx) = mpsc::channel::<DepthUpdate>(1024);
    let mut first_update_id: Option<u64> = None;
    while let Some(update) = streams.next().await {
        first_update_id = Some(update.last_update_id);
        if tx.send(update).await.is_err() {
            println!("Receiver closed, exiting sender task");
            break;
        }
        break;
    }

    if first_update_id.is_none() {
        return Err(eyre::eyre!("Failed to get initial update id"));
    }

    let updates_reciever = tokio::spawn(async move {
        while let Some(update) = streams.next().await {
            if tx.send(update).await.is_err() {
                println!("Receiver closed, exiting sender task");
                break;
            }
        }
    });

    let mut book = init_book(first_update_id.unwrap(), "BTCUSDT", 10).await?;
    println!("Initial book state:\n{}", book);

    tokio::spawn(async move {
        while let Some(update) = rx.recv().await {
            let mut latest_update = update;
            loop {
                match rx.try_recv() {
                    Ok(update) => {
                        if update.first_update_id > book.last_update_id() + 1 {
                            handle_update(&mut book, &latest_update);
                            latest_update = update;
                        } else if update.last_update_id > latest_update.last_update_id {
                            latest_update = update;
                        }
                    }
                    Err(TryRecvError::Empty) => {
                        handle_update(&mut book, &latest_update);
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        break;
                    }
                }
            }
        }
    });

    updates_reciever.await.unwrap();
    Ok(())
}
