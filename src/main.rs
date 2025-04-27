mod utils;

use eyre::Result;
use std::collections::BTreeMap;
use std::collections::HashMap;

type Price = u64;
type Volume = u64;
struct BookOrder {
    last_update_id: u128,
    bids: BTreeMap<Price, Volume>,
    asks: BTreeMap<Price, Volume>,
}

async fn get_snapshot(symbol: &str, limit: i16) -> Result<HashMap<String, serde_json::Value>> {
    let params = [("symbol", symbol), ("limit", &limit.to_string())];
    let client = reqwest::Client::new();

    let response: HashMap<String, serde_json::Value> = client
        .get("https://api.binance.com/api/v3/depth")
        .query(&params)
        .send()
        .await?
        .json()
        .await?;

    Ok(response)
}

#[tokio::main]
async fn main() -> Result<()> {
    let snapshot_json = get_snapshot("BTCUSDT", 10).await?;
    println!("{snapshot_json:#?}");
    Ok(())
}
