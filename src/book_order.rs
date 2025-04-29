use core::f64;
use eyre::OptionExt;
use eyre::Result;
use serde_json;
use std::collections::BTreeMap;
use std::fmt;
use std::i64;

type Price = i64;
type Volume = i64;
pub type UpdateId = u64;
type OrdersTree = BTreeMap<Price, Volume>;

const SCALE: i64 = 10i64.pow(8);
const OVERFLOW_THRESHOLD: f64 = (i64::MAX as f64) / (SCALE as f64);

pub fn f64_to_i64(value: f64) -> Result<i64> {
    if value > OVERFLOW_THRESHOLD {
        return Err(eyre::eyre!(
            "Value overflow while trying to convert to internal format"
        ));
    }
    Ok((value * SCALE as f64).round() as i64)
}

pub fn i64_to_f64(internal_value: i64) -> f64 {
    (internal_value as f64) / (SCALE as f64)
}

#[derive(Debug)]

pub enum DepthUpdateError {
    ParsingError,
    InvalidBook,
}

impl fmt::Display for DepthUpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DepthUpdateError::ParsingError => write!(f, "Failed to parse update"),
            DepthUpdateError::InvalidBook => write!(f, "Invalid book state. Book update required"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BookOrder {
    last_update_id: UpdateId,
    bids: OrdersTree,
    asks: OrdersTree,
    symbol: String,
}

impl BookOrder {
    pub fn new(symbol: &str, snapshot: &serde_json::Value) -> Result<Self> {
        let bids_json = snapshot
            .get("bids")
            .ok_or_eyre("No 'bids' field in snapshot")?;

        let asks_json = snapshot
            .get("asks")
            .ok_or_eyre("No 'asks' field in snapshot")?;

        let last_update_id = snapshot
            .get("lastUpdateId")
            .ok_or_eyre("No 'lastUpdateId' field in snapshot")?
            .as_u64()
            .ok_or_eyre("Failed to convert lastUpdateId to u64")?;

        let bids = Self::build_orders(bids_json)?;
        let asks = Self::build_orders(asks_json)?;

        Ok(BookOrder {
            last_update_id,
            bids,
            asks,
            symbol: symbol.to_string(),
        })
    }

    pub fn last_update_id(&self) -> UpdateId {
        self.last_update_id
    }

    pub fn depth_update(&mut self, update_json: &serde_json::Value) -> Result<()> {
        let last_update = update_json
            .get("u")
            .ok_or_else(|| eyre::eyre!(DepthUpdateError::ParsingError))?
            .as_u64()
            .ok_or_else(|| eyre::eyre!(DepthUpdateError::ParsingError))?;

        if last_update <= self.last_update_id {
            return Ok(());
        }

        let first_update = update_json
            .get("U")
            .ok_or_else(|| eyre::eyre!(DepthUpdateError::ParsingError))?
            .as_u64()
            .ok_or_else(|| eyre::eyre!(DepthUpdateError::ParsingError))?;

        if first_update > self.last_update_id + 1 {
            return Err(eyre::eyre!(DepthUpdateError::InvalidBook));
        }

        let bids = update_json
            .get("b")
            .ok_or_else(|| eyre::eyre!(DepthUpdateError::ParsingError))?
            .as_array()
            .ok_or_else(|| eyre::eyre!(DepthUpdateError::ParsingError))?;

        let asks = update_json
            .get("a")
            .ok_or_else(|| eyre::eyre!(DepthUpdateError::ParsingError))?
            .as_array()
            .ok_or_else(|| eyre::eyre!(DepthUpdateError::ParsingError))?;

        self.update_orders(asks, bids)?;

        self.last_update_id = last_update;

        Ok(())
    }

    fn update_orders(
        &mut self,
        asks_updates: &Vec<serde_json::Value>,
        bids_update: &Vec<serde_json::Value>,
    ) -> Result<()> {
        let mut asks_tmp: Vec<(Price, Volume)> = Vec::new();
        for order in asks_updates {
            let data = Self::extract_price_and_volume(order)?;
            asks_tmp.push(data);
        }

        let mut bids_tmp: Vec<(Price, Volume)> = Vec::new();
        for order in bids_update {
            let data = Self::extract_price_and_volume(order)?;
            bids_tmp.push(data);
        }

        for (price, volume) in bids_tmp {
            if volume != 0 {
                self.bids.insert(price, volume);
            } else {
                self.bids.remove(&price);
            }
        }

        for (price, volume) in asks_tmp {
            if volume != 0 {
                self.asks.insert(price, volume);
            } else {
                self.asks.remove(&price);
            }
        }

        Ok(())
    }

    fn extract_price_and_volume(order_json: &serde_json::Value) -> Result<(Price, Volume)> {
        let error_message = "Failed to parse price or volume from json";
        let order_data = order_json.as_array().ok_or_eyre(error_message)?;

        if order_data.len() != 2 {
            return Err(eyre::eyre!(error_message));
        }

        let price: f64 = order_data[0]
            .as_str()
            .ok_or_eyre(error_message)?
            .parse()
            .map_err(|_| eyre::eyre!(error_message))?;

        let volume: f64 = order_data[1]
            .as_str()
            .ok_or_eyre(error_message)?
            .parse()
            .map_err(|_| eyre::eyre!(error_message))?;
        let internal_price = f64_to_i64(price)?;
        let internal_volume = f64_to_i64(volume)?;

        Ok((internal_price, internal_volume))
    }

    fn build_orders(orders_json: &serde_json::Value) -> Result<OrdersTree> {
        let error_message = "Failed to parse orders";
        let mut map: OrdersTree = BTreeMap::new();

        let orders = orders_json.as_array().ok_or_eyre(error_message)?;

        for order in orders {
            let (price, volume) = Self::extract_price_and_volume(order)?;

            assert!(!map.contains_key(&price));
            map.insert(price, volume);
        }

        Ok(map)
    }
}

impl fmt::Display for BookOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Symbol {}    Book last update id: {}",
            self.symbol, self.last_update_id
        )?;
        let header = format!(
            "{:<15}{:<15}  |  {:<15}{:<15}",
            "BID_PRICE", "BID_VOLUME", "ASK_PRICE", "ASK_VOLUME"
        );
        writeln!(f, "{}", header)?;
        writeln!(f, "{}", "=".repeat(header.len()))?;

        let mut bids_it = self.bids.iter().rev();
        let mut asks_it = self.asks.iter();
        loop {
            let bid = bids_it.next();
            let ask = asks_it.next();
            match (bid, ask) {
                (Some(bid), Some(ask)) => writeln!(
                    f,
                    "{:<15}{:<15}  |  {:<15}{:<15}",
                    i64_to_f64(*bid.0),
                    i64_to_f64(*bid.1),
                    i64_to_f64(*ask.0),
                    i64_to_f64(*ask.1)
                )?,
                (Some(bid), None) => writeln!(
                    f,
                    "{:<15}{:<15}  |  {:<15}{:<15}",
                    i64_to_f64(*bid.0),
                    i64_to_f64(*bid.1),
                    "",
                    ""
                )?,
                (None, Some(ask)) => writeln!(
                    f,
                    "{:<15}{:<15}  |  {:<15}{:<15}",
                    "",
                    "",
                    i64_to_f64(*ask.0),
                    i64_to_f64(*ask.1)
                )?,
                (None, None) => break,
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod float_conversions_tests {
        use super::*;

        #[test]
        fn test_f64_to_i64() {
            let values = [
                0.12345678,
                0.0,
                1.0,
                0.1,
                0.00000001,
                12345.6789,
                -0.12345678,
                -1.0,
            ];
            let expected = [
                12345678,
                0,
                100_000_000,
                10_000_000,
                1,
                1_234_567_890_000,
                -12345678,
                -100_000_000,
            ];

            let actual: Vec<Result<i64>> = values.iter().map(|x| f64_to_i64(*x)).collect();

            for (act, exp) in actual.into_iter().zip(expected.iter()) {
                assert_eq!(act.unwrap(), *exp);
            }
        }

        #[test]
        fn test_i64_to_f64() {
            let values = [
                12345678,
                0,
                100_000_000,
                10_000_000,
                1,
                1_234_567_890_000,
                -12345678,
                -100_000_000,
            ];
            let expected = [
                0.12345678,
                0.0,
                1.0,
                0.1,
                0.00000001,
                12345.6789,
                -0.12345678,
                -1.0,
            ];
            let actual: Vec<f64> = values.iter().map(|x| i64_to_f64(*x)).collect();
            for (act, exp) in actual.iter().zip(expected.iter()) {
                assert_eq!(act, exp)
            }
        }

        #[test]
        fn test_f64_to_i64_overflow() {
            let value = OVERFLOW_THRESHOLD + 1.0;
            let result = f64_to_i64(value);
            assert!(result.is_err());
        }
    }

    mod book_order_tests {
        use super::*;

        #[test]
        fn test_book_order_from_snapshot() {
            let input = r#"{
                "bids": [
                    ["94729.47000000","5.39833000"],
                    ["94729.46000000","0.00030000"],
                    ["94729.41000000","0.00006000"]
                ],
                "asks": [
                    ["94729.48000000","4.30144000"],
                    ["94729.49000000","0.00197000"],
                    ["94730.20000000","0.00006000"],
                    ["94730.49000000","0.00006000"]
                ],
                "lastUpdateId": 67621829690
            }"#;

            let expected_update_id = 67621829690 as u64;

            let mut expected_bids: OrdersTree = BTreeMap::new();
            expected_bids.insert(
                f64_to_i64(94729.47000000).unwrap(),
                f64_to_i64(5.39833000).unwrap(),
            );
            expected_bids.insert(
                f64_to_i64(94729.46000000).unwrap(),
                f64_to_i64(0.00030000).unwrap(),
            );
            expected_bids.insert(
                f64_to_i64(94729.41000000).unwrap(),
                f64_to_i64(0.00006000).unwrap(),
            );

            let mut expected_asks: OrdersTree = BTreeMap::new();
            expected_asks.insert(
                f64_to_i64(94729.48000000).unwrap(),
                f64_to_i64(4.30144000).unwrap(),
            );
            expected_asks.insert(
                f64_to_i64(94729.49000000).unwrap(),
                f64_to_i64(0.00197000).unwrap(),
            );
            expected_asks.insert(
                f64_to_i64(94730.20000000).unwrap(),
                f64_to_i64(0.00006000).unwrap(),
            );
            expected_asks.insert(
                f64_to_i64(94730.49000000).unwrap(),
                f64_to_i64(0.00006000).unwrap(),
            );

            let json: serde_json::Value =
                serde_json::from_str(input).expect("Failed to load test json");
            let book =
                BookOrder::new("BTCUSDT", &json).expect("Failed to create book from snapshot");
            assert_eq!(expected_update_id, book.last_update_id);
            assert_eq!(expected_asks, book.asks);
            assert_eq!(expected_bids, book.bids);
            assert_eq!("BTCUSDT", book.symbol);
        }

        #[test]
        fn test_book_order_update_succeed() {
            let input = r#"{
                "bids": [
                    ["94729.47000000","5.39833000"],
                    ["94729.46000000","0.00030000"],
                    ["94729.41000000","0.00006000"]
                ],
                "asks": [
                    ["94729.48000000","4.30144000"],
                    ["94729.49000000","0.00197000"],
                    ["94730.20000000","0.00006000"],
                    ["94730.49000000","0.00006000"]
                ],
                "lastUpdateId": 5
            }"#;

            let update = r#"{
                "e": "depthUpdate",
                "E": 0,
                "s": "BTCUSDT",
                "U": 4,
                "u": 6,
                "b": [
                    [
                        "94729.47000000",      
                        "10.00"
                    ],
                    [
                    "94725.00000000",
                    "15.00"
                    ]
                ],
                "a": [
                    [
                    "94730.20000000",
                    "100.00"  
                    ],
                    [
                    "94731.20000000",
                    "1.0"
                    ]
                ]
            }"#;

            let snapshot_json: serde_json::Value =
                serde_json::from_str(input).expect("Failed to load test json");
            let update_json: serde_json::Value =
                serde_json::from_str(update).expect("Failed to load test json");

            let mut expected_bids: OrdersTree = BTreeMap::new();
            expected_bids.insert(
                f64_to_i64(94729.47000000).unwrap(),
                f64_to_i64(10.0).unwrap(),
            );
            expected_bids.insert(
                f64_to_i64(94729.46000000).unwrap(),
                f64_to_i64(0.00030000).unwrap(),
            );
            expected_bids.insert(
                f64_to_i64(94729.41000000).unwrap(),
                f64_to_i64(0.00006000).unwrap(),
            );
            expected_bids.insert(
                f64_to_i64(94725.00000000).unwrap(),
                f64_to_i64(15.00).unwrap(),
            );

            let mut expected_asks: OrdersTree = BTreeMap::new();
            expected_asks.insert(
                f64_to_i64(94729.48000000).unwrap(),
                f64_to_i64(4.30144000).unwrap(),
            );
            expected_asks.insert(
                f64_to_i64(94729.49000000).unwrap(),
                f64_to_i64(0.00197000).unwrap(),
            );
            expected_asks.insert(
                f64_to_i64(94730.20000000).unwrap(),
                f64_to_i64(100.0).unwrap(),
            );
            expected_asks.insert(
                f64_to_i64(94730.49000000).unwrap(),
                f64_to_i64(0.00006000).unwrap(),
            );
            expected_asks.insert(
                f64_to_i64(94731.20000000).unwrap(),
                f64_to_i64(1.0).unwrap(),
            );

            let mut book = BookOrder::new("BTCUSDT", &snapshot_json)
                .expect("Failed to create book from snapshot");
            book.depth_update(&update_json).expect("Failed to update");
            assert_eq!(expected_asks, book.asks);
            assert_eq!(expected_bids, book.bids);
        }

        #[test]
        fn test_book_order_ignore_stale_update() {
            let input = r#"{
                "bids": [
                    ["94729.47000000","5.39833000"]
                ],
                "asks": [
                    ["94729.48000000","4.30144000"]
                ],
                "lastUpdateId": 5
            }"#;

            let update = r#"{
                "e": "depthUpdate",
                "E": 0,
                "s": "BTCUSDT",
                "U": 3,
                "u": 4,
                "b": [
                    [
                        "94729.47000000",
                        "10.00"
                    ]
                ],
                "a": [
                    [
                    "94730.20000000",
                    "100.00"
                    ]
                ]
            }"#;

            let snapshot_json: serde_json::Value =
                serde_json::from_str(input).expect("Failed to load test json");
            let update_json: serde_json::Value =
                serde_json::from_str(update).expect("Failed to load test json");

            let mut expected_bids: OrdersTree = BTreeMap::new();
            expected_bids.insert(
                f64_to_i64(94729.47000000).unwrap(),
                f64_to_i64(5.39833000).unwrap(),
            );

            let mut expected_asks: OrdersTree = BTreeMap::new();
            expected_asks.insert(
                f64_to_i64(94729.48000000).unwrap(),
                f64_to_i64(4.30144000).unwrap(),
            );

            let mut book = BookOrder::new("BTCUSDT", &snapshot_json)
                .expect("Failed to create book from snapshot");
            let result = book.depth_update(&update_json);
            assert!(result.is_ok());
            assert_eq!(expected_asks, book.asks);
            assert_eq!(expected_bids, book.bids);
        }

        #[test]
        fn test_book_order_remove_level() {
            let input = r#"{
                "bids": [
                    ["94729.47000000","5.39833000"],
                    ["94730.00000000","5.39833000"]
                ],
                "asks": [
                    ["94729.47000000","4.30144000"],
                    ["94729.48000000","4.30144000"]
                ],
                "lastUpdateId": 5
            }"#;

            let update = r#"{
                "e": "depthUpdate",
                "E": 0,
                "s": "BTCUSDT",
                "U": 3,
                "u": 6,
                "b": [
                    [
                        "94730.00000000",
                        "0.0"
                    ]
                ],
                "a": [
                    [
                    "94729.47000000",
                    "0.00"
                    ]
                ]
            }"#;

            let snapshot_json: serde_json::Value =
                serde_json::from_str(input).expect("Failed to load test json");
            let update_json: serde_json::Value =
                serde_json::from_str(update).expect("Failed to load test json");

            let mut expected_bids: OrdersTree = BTreeMap::new();
            expected_bids.insert(
                f64_to_i64(94729.47000000).unwrap(),
                f64_to_i64(5.39833000).unwrap(),
            );

            let mut expected_asks: OrdersTree = BTreeMap::new();
            expected_asks.insert(
                f64_to_i64(94729.48000000).unwrap(),
                f64_to_i64(4.30144000).unwrap(),
            );

            let mut book = BookOrder::new("BTCUSDT", &snapshot_json)
                .expect("Failed to create book from snapshot");
            let result = book.depth_update(&update_json);
            assert!(result.is_ok());
            assert_eq!(expected_asks, book.asks);
            assert_eq!(expected_bids, book.bids);
        }

        #[test]
        fn test_book_order_print() {
            let input = r#"{
                "bids": [
                    ["94729.47000000","5.39833000"],
                    ["94729.46000000","0.00030000"],
                    ["94729.41000000","0.00006000"]
                ],
                "asks": [
                    ["94729.48000000","4.30144000"],
                    ["94729.49000000","0.00197000"],
                    ["94730.20000000","0.00006000"],
                    ["94730.49000000","0.00006000"]
                ],
                "lastUpdateId": 67621829690
            }"#;
            let expected_string =
                String::from("Symbol BTCUSDT    Book last update id: 67621829690\n")
                    + "BID_PRICE      BID_VOLUME       |  ASK_PRICE      ASK_VOLUME     \n"
                    + "=================================================================\n"
                    + "94729.47       5.39833          |  94729.48       4.30144        \n"
                    + "94729.46       0.0003           |  94729.49       0.00197        \n"
                    + "94729.41       0.00006          |  94730.2        0.00006        \n"
                    + "                                |  94730.49       0.00006        \n";

            let json: serde_json::Value =
                serde_json::from_str(input).expect("Failed to load test json");
            let book =
                BookOrder::new("BTCUSDT", &json).expect("Failed to create book from snapshot");
            println!("{}", book);
            let output = format!("{}", book);
            assert_eq!(expected_string, output);
        }
    }
}
