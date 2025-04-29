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
    let (tx, rx) = mpsc::channel::<DepthUpdate>(1024);
    let updates_receiver = tokio::spawn(async move {
        let mut depth_updates_provider = UpdatesProvider::new("BTCUSDT", 3, tx);
        depth_updates_provider.run().await
    });

    let updates_processor = {
        tokio::spawn(async move {
            let mut processor: UpdateProcessor = UpdateProcessor::new("BTCUSDT", rx);
            loop {
                if let Err(_e) = processor.run(100, Duration::new(5, 0)).await {
                    println!("Invalid book state. Reiniting...");
                } else {
                    break;
                }
            }
        })
    };

    if let Err(result) = updates_receiver.await? {
        println!("Error in updates provider {:#?}", result);
    }
    updates_processor.await?;
    Ok(())
}
