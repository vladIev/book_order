mod book_order;
mod depth_updates;
mod updates_processor;
mod updates_provider;

use book_order::{BookOrder, DepthUpdateError};
use depth_updates::DepthUpdate;
use eyre::Result;
use tokio::{sync::mpsc, sync::mpsc::error::TryRecvError};
use updates_provider::UpdatesProvider;

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
    let (tx, mut rx) = mpsc::channel::<DepthUpdate>(1024);
    let updates_receiver = tokio::spawn(async move {
        let mut depth_updates_provider = UpdatesProvider::new("BTCUSDT", 3, tx);
        depth_updates_provider.run().await
    });

    let mut first_update_opt: Option<DepthUpdate> = None;
    while let Some(update) = rx.recv().await {
        first_update_opt = Some(update);
        break;
    }

    if first_update_opt.is_none() {
        return Err(eyre::eyre!("Failed to get initial update id"));
    }

    let first_update = first_update_opt.take().unwrap();
    let mut book = init_book(first_update.first_update_id, "BTCUSDT", 10).await?;
    book.depth_update(&first_update.value)?;
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

    if let Err(result) = updates_receiver.await? {
        println!("Error in updates provider {:#?}", result);
    }
    Ok(())
}
