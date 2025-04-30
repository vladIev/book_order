mod book_order;
mod depth_updates;
mod updates_processor;
mod updates_provider;

use book_order::DepthUpdateError;
use clap::Parser;
use depth_updates::DepthUpdate;
use eyre::Result;
use tokio::sync::mpsc;
use tokio::time::Duration;
use updates_processor::UpdateProcessor;
use updates_provider::UpdatesProvider;

#[derive(Parser, Debug)]
#[command(name = "my_app")]
struct Args {
    /// Trading symbol like BTCUSDT
    #[arg(short('s'), long)]
    symbol: String,

    /// Number of web sockets to open. Max = 3
    #[arg(long, default_value_t = 3)]
    streams: usize,

    /// Number of price levels in snapshot
    #[arg(short('l'), long, default_value_t = 100)]
    limit: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.streams > 3 {
        return Err(eyre::eyre!("To many streams. Max 3"));
    }
    if args.limit > 5000 {
        return Err(eyre::eyre!("Limit is too big. Max 5000"));
    }
    let (updates_tx, updates_rx) = mpsc::channel::<DepthUpdate>(1024);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let mut updates_receiver = {
        let shutdown_rx = shutdown_rx.clone();
        let symbol = args.symbol.clone();
        let num_of_sockets = args.streams;
        tokio::spawn(async move {
            let mut depth_updates_provider =
                UpdatesProvider::new(&symbol, num_of_sockets, updates_tx, shutdown_rx);
            depth_updates_provider.run().await
        })
    };

    let updates_processor = {
        let shutdown_rx = shutdown_rx.clone();
        let symbol = args.symbol.clone();
        let limit = args.limit;
        tokio::spawn(async move {
            let mut processor: UpdateProcessor =
                UpdateProcessor::new(&symbol, updates_rx, shutdown_rx);
            loop {
                let rc = processor.run(limit, Duration::from_secs(5)).await;

                match rc {
                    Ok(_) => break,
                    Err(e) => {
                        if let Some(DepthUpdateError::InvalidBook) =
                            e.downcast_ref::<DepthUpdateError>()
                        {
                            println!("Invalid book — restarting processor...");
                            continue;
                        }

                        eprintln!("Unexpected processor error: {:?}", e);
                        return Err(e);
                    }
                }
            }

            Ok(())
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
