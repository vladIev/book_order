mod book_order;
mod depth_updates;
mod updates_processor;
mod updates_provider;

use depth_updates::DepthUpdate;
use eyre::Result;
use tokio::sync::mpsc;
use tokio::time::Duration;
use updates_processor::UpdateProcessor;
use updates_provider::UpdatesProvider;

#[tokio::main]
async fn main() -> Result<()> {
    let (updates_tx, updates_rx) = mpsc::channel::<DepthUpdate>(1024);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let mut updates_receiver = {
        let shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut depth_updates_provider =
                UpdatesProvider::new("BTCUSDT", 3, updates_tx, shutdown_rx);
            depth_updates_provider.run().await
        })
    };

    let updates_processor = {
        let shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut processor: UpdateProcessor =
                UpdateProcessor::new("BTCUSDT", updates_rx, shutdown_rx);
            loop {
                match processor.run(100, Duration::from_secs(5)).await {
                    Ok(_) => break,
                    Err(e) => eprintln!("Processor error: {:?}", e),
                }
            }
        })
    };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Ctrl+C received. Sending shutdown signal...");
            shutdown_tx.send(true)?;
        }
        result = &mut updates_receiver => {
            match result {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => eprintln!("Error in updates provider: {:?}", e),
                Err(join_err) => eprintln!("Updates provider task panicked: {:?}", join_err),
            }
        }
    }

    if let Err(e) = updates_processor.await {
        eprintln!("Updates processor task panicked: {:?}", e);
    }

    Ok(())
}
