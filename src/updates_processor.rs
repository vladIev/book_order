use crate::book_order::{BookOrder, DepthUpdateError, UpdateId};
use crate::depth_updates::DepthUpdate;

use eyre::Result;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep, timeout};

pub struct UpdateProcessor {
    symbol: String,
    rx: Receiver<DepthUpdate>,
    shutdown_rx: watch::Receiver<bool>,
    book: Arc<Mutex<Option<BookOrder>>>,
    book_printer: Option<JoinHandle<()>>,
}

impl UpdateProcessor {
    pub fn new(
        symbol: &str,
        rx: Receiver<DepthUpdate>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        UpdateProcessor {
            symbol: symbol.to_string(),
            rx,
            shutdown_rx,
            book: Arc::new(Mutex::new(None)),
            book_printer: None,
        }
    }

    pub async fn run(
        &mut self,
        snapshot_limit: usize,
        book_printing_interval: Duration,
    ) -> Result<()> {
        println!("Processor is waiting for the first event");
        let first_update = timeout(Duration::from_secs(30), self.rx.recv()).await;
        match first_update {
            Ok(Some(update)) => {
                println!(
                    "Requesting snapshot. Min snaphost updat id: {}",
                    update.first_update_id
                );
                let symbol = self.symbol.clone();
                self.init_book(&update, &symbol, snapshot_limit).await?;
            }
            Ok(None) => {
                return Ok(());
            }
            Err(_) => return Err(eyre::eyre!("Failed to get initial update id")),
        }

        if self.book_printer.is_none() {
            self.start_book_printer_job(book_printing_interval).await;
        }

        self.reader_cycle().await
    }

    async fn reader_cycle(&mut self) -> Result<()> {
        let mut current_book_id: UpdateId =
            self.with_book(|book| Ok(book.last_update_id())).await?;

        loop {
            tokio::select! {
                update_opt = timeout(Duration::from_secs(5), self.rx.recv()) => {
                    match update_opt {
                        Ok(Some(update)) => {
                            current_book_id = self.pick_latest_and_apply(update, current_book_id).await?;
                        }
                        Ok(None) => {
                            println!("Updates channel closed");
                            break;
                        }
                        Err(_)=>{println!("Timed out on new update");}
                    }
                }

                changed = self.shutdown_rx.changed() => {
                    if changed.is_ok() && *self.shutdown_rx.borrow() {
                        println!("Terminating processor");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn pick_latest_and_apply(
        &mut self,
        update: DepthUpdate,
        mut current_book_id: UpdateId,
    ) -> Result<UpdateId> {
        let mut latest = update;
        while let Ok(update) = self.rx.try_recv() {
            if update.first_update_id > current_book_id + 1 {
                current_book_id = self
                    .with_book(|book| Ok(Self::apply_update(book, &latest)?))
                    .await?;

                latest = update;
            } else if update.last_update_id > latest.last_update_id {
                latest = update;
            }
        }

        current_book_id = self
            .with_book(|book| Ok(Self::apply_update(book, &latest)?))
            .await?;

        Ok(current_book_id)
    }

    async fn with_book<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut BookOrder) -> Result<R>,
    {
        let mut lock = self.book.lock().await;
        let book = lock
            .as_mut()
            .ok_or_else(|| eyre::eyre!("Book is not initialized"))?;
        f(book)
    }

    async fn init_book(
        &mut self,
        first_update: &DepthUpdate,
        symbol: &str,
        limit: usize,
    ) -> Result<()> {
        loop {
            let snapshot_json = Self::get_snapshot(symbol, limit).await?;
            if let Some(update_id) = snapshot_json.get("lastUpdateId").and_then(|u| u.as_u64()) {
                if update_id >= first_update.first_update_id {
                    println!("Snapshot obtained. Building book order");
                    let mut book_lock = self.book.lock().await;
                    *book_lock = Some(BookOrder::new(symbol, &snapshot_json)?);
                    println!("Book is ready");
                    book_lock
                        .as_mut()
                        .unwrap()
                        .depth_update(&first_update.value)?;
                    break;
                } else {
                    println!("Snapshot it too old. Retrying");
                }
            }
        }

        Ok(())
    }

    async fn start_book_printer_job(&mut self, interval: Duration) {
        self.book_printer = Some({
            let book: Arc<Mutex<Option<BookOrder>>> = Arc::clone(&self.book);
            tokio::spawn(async move {
                loop {
                    let cloned_book = {
                        let book_lock = book.lock().await;
                        book_lock.as_ref().cloned()
                    };
                    if let Some(book) = cloned_book {
                        println!("\n{}\n", book);
                    }
                    sleep(interval).await;
                }
            })
        })
    }

    fn apply_update(book: &mut BookOrder, update: &DepthUpdate) -> Result<UpdateId> {
        match book.depth_update(&update.value) {
            Err(err) => match err.downcast_ref::<DepthUpdateError>() {
                Some(DepthUpdateError::ParsingError) => {
                    eprintln!("Depth update parsing error");
                }
                Some(DepthUpdateError::InvalidBook) => {
                    return Err(eyre::eyre!("Invalid book state"));
                }
                None => {
                    return Err(err);
                }
            },
            Ok(()) => {}
        }

        Ok(book.last_update_id())
    }

    async fn get_snapshot(symbol: &str, limit: usize) -> Result<serde_json::Value> {
        let params = [
            ("symbol", symbol.to_uppercase()),
            ("limit", limit.to_string()),
        ];
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
