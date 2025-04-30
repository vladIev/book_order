use crate::depth_updates::DepthUpdate;

use eyre::Result;
use futures_util::{Stream, StreamExt, stream::select_all};
use std::{pin::Pin, time::Duration};
use tokio::{
    sync::{mpsc::Sender, watch},
    time::sleep,
};
use tokio_tungstenite::connect_async;

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

    async fn init_updates_stream(
        symbol: &str,
        num_of_streams: usize,
    ) -> Result<impl Stream<Item = DepthUpdate>> {
        let retry_interval = Duration::from_secs(3);
        let mut retries = 5;
        let url = format!(
            "wss://stream.binance.com:9443/ws/{}@depth",
            symbol.to_lowercase()
        );

        let mut streams: Vec<Pin<Box<dyn Stream<Item = DepthUpdate> + Send>>> = Vec::new();

        while streams.len() < num_of_streams && retries > 0 {
            println!("Connecting to {}", url);
            match connect_async(&url).await {
                Err(e) => {
                    eprintln!("Failed to establish connection to {}. Error {:?}", url, e);
                    sleep(retry_interval).await;
                    retries -= 1;
                }
                Ok((ws, _)) => {
                    let read =
                        Box::pin(ws.filter_map(|msg| async {
                            msg.ok().and_then(DepthUpdate::from_message)
                        }));
                    streams.push(read);
                    println!("Connection established");
                }
            }
        }

        match streams.len() {
            0 => return Err(eyre::eyre!("Failed to establish any connections")),
            n if n < num_of_streams => {
                println!(
                    "Only {} of {} streams established. Proceeding anyway.",
                    n, num_of_streams
                );
            }
            _ => {}
        }
        Ok(select_all(streams))
    }
}
