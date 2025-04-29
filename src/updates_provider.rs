use crate::depth_updates::DepthUpdate;

use eyre::Result;
use futures_util::{Stream, StreamExt, stream::select_all};
use std::pin::Pin;
use tokio::{
    net::{TcpStream, unix::pipe::Receiver},
    sync::{mpsc::Sender, watch},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

pub struct UpdatesProvider {
    symbol: String,
    num_of_sockets: usize,
    tx: Sender<DepthUpdate>,
    shutdown_rx: watch::Receiver<bool>,
}

impl UpdatesProvider {
    pub fn new(
        symbol: &str,
        num_of_sockets: usize,
        tx: Sender<DepthUpdate>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        UpdatesProvider {
            symbol: symbol.to_string(),
            num_of_sockets,
            tx,
            shutdown_rx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut streams = Self::init_updates_stream(&self.symbol, self.num_of_sockets).await?;

        loop {
            tokio::select! {
                update_opt = streams.next() => {
                    match update_opt {
                        Some(update) => {
                            if Self::hanlde_incoming_update(&self.tx, update).await.is_err() {
                                println!("Receiver closed, exiting sender task");
                                break;
                            }
                        }
                        None => {
                            println!("Stream ended");
                            break;
                        }
                    }
                }

                changed = self.shutdown_rx.changed() => {
                    if changed.is_ok() && *self.shutdown_rx.borrow() {
                        println!("Terminating provider");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn hanlde_incoming_update(tx: &Sender<DepthUpdate>, update: DepthUpdate) -> Result<()> {
        tx.send(update)
            .await
            .map_err(|e| eyre::eyre!("Send error: {}", e))
    }

    async fn ws_connect(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let (ws_stream, _) = connect_async(url).await?;
        Ok(ws_stream)
    }

    async fn init_updates_stream(
        symbol: &str,
        num_of_streams: usize,
    ) -> Result<impl Stream<Item = DepthUpdate>> {
        let url = format!(
            "wss://stream.binance.com:9443/ws/{}@depth",
            symbol.to_lowercase()
        );

        let mut streams: Vec<Pin<Box<dyn Stream<Item = DepthUpdate> + Send>>> = Vec::new();

        for _ in 0..num_of_streams {
            let ws = Self::ws_connect(&url).await?;
            let read = Box::pin(
                ws.filter_map(|msg| async { msg.ok().and_then(DepthUpdate::from_message) }),
            ) as Pin<Box<dyn Stream<Item = DepthUpdate> + Send>>;
            streams.push(read);
        }
        Ok(select_all(streams))
    }
}
