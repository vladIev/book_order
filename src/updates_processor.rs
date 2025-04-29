use crate::book_order::{BookOrder, DepthUpdateError};
use crate::depth_updates::DepthUpdate;
use eyre::Result;
use tokio::sync::mpsc::{Receiver, error::TryRecvError};

pub struct UpdateProcessor {
    symbol: String,
    rx: Receiver<DepthUpdate>,
    // TODO: smart pointer for book
    // TOOD: printing job
}

impl UpdateProcessor {
    pub fn new(symbol: &str, rx: Receiver<DepthUpdate>) -> Self {
        UpdateProcessor {
            symbol: symbol.to_string(),
            rx,
        }
    }

    pub async fn run(&mut self, snapshot_limit: usize) -> Result<()> {
        let first_update = self
            .rx
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("Failed to get initial update id"))?;

        let mut book =
            Self::init_book(first_update.first_update_id, &self.symbol, snapshot_limit).await?;

        book.depth_update(&first_update.value)?;
        println!("Initial book state:\n{}", book);

        self.reader_cycle(book).await
    }

    async fn reader_cycle(&mut self, mut book: BookOrder) -> Result<()> {
        while let Some(update) = self.rx.recv().await {
            let mut latest_update = update;
            loop {
                match self.rx.try_recv() {
                    Ok(update) => {
                        if update.first_update_id > book.last_update_id() + 1 {
                            Self::handle_update(&mut book, &latest_update)?;
                            latest_update = update;
                        } else if update.last_update_id > latest_update.last_update_id {
                            latest_update = update;
                        }
                    }
                    Err(TryRecvError::Empty) => {
                        Self::handle_update(&mut book, &latest_update)?;
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_update(book: &mut BookOrder, update: &DepthUpdate) -> Result<()> {
        if let Err(err) = book.depth_update(&update.value) {
            if let Some(depth_err) = err.downcast_ref::<DepthUpdateError>() {
                match depth_err {
                    DepthUpdateError::ParsingError => {
                        println!("Depth update parsing error");
                    }
                    DepthUpdateError::InvalidBook => {
                        return Err(eyre::eyre!("Invalid book state"));
                    }
                }
            }
        } else {
            println!(
                "Update applied succefully. New id {}",
                book.last_update_id()
            );
        }

        Ok(())
    }

    async fn init_book(min_update_id: u64, symbol: &str, limit: usize) -> Result<BookOrder> {
        loop {
            let snapshot_json = Self::get_snapshot(symbol, limit).await?;
            if let Some(update_id) = snapshot_json.get("lastUpdateId").and_then(|u| u.as_u64()) {
                if update_id >= min_update_id {
                    return BookOrder::new(&snapshot_json);
                }
            }
        }
    }

    async fn get_snapshot(symbol: &str, limit: usize) -> Result<serde_json::Value> {
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
}
