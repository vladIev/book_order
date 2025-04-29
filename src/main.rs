mod book_order;
mod depth_updates;
mod updates_processor;
mod updates_provider;

use depth_updates::DepthUpdate;
use eyre::Result;
use tokio::sync::mpsc;
use updates_processor::UpdateProcessor;
use updates_provider::UpdatesProvider;

#[tokio::main]
async fn main() -> Result<()> {
    let (tx, rx) = mpsc::channel::<DepthUpdate>(1024);
    let updates_receiver = tokio::spawn(async move {
        let mut depth_updates_provider = UpdatesProvider::new("BTCUSDT", 3, tx);
        depth_updates_provider.run().await
    });

    let updates_processor = tokio::spawn(async move {
        let mut depth_updates_processor = UpdateProcessor::new("BTCUSDT", rx);
        loop {
            if let Ok(e) = depth_updates_processor.run(100).await {
                println!("Invalid book state. Reiniting...");
            } else {
                break;
            }
        }
    });

    if let Err(result) = updates_receiver.await? {
        println!("Error in updates provider {:#?}", result);
    }
    updates_processor.await?;
    Ok(())
}
