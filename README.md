# Book order
This project implements a real-time order book tracker for a selected trading pair on Binance. 

## Build
cargo build
cargo run

## Usage
```book_order [OPTIONS]

Options:
  -s, --symbol <SYMBOL>    Trading symbol like BTCUSDT [default: BTCUSDT]
      --streams <STREAMS>  Number of web sockets to open. Max = 3 [default: 3]
  -l, --limit <LIMIT>      Number of price levels in snapshot [default: 100]
  -h, --help               Print help
```

## Logic of Operation
1.1 Retrieving the Snapshot via HTTP

    Fetch the current order book depth using Binance REST API.

    This snapshot will serve as the starting point for building the order book.

1.2 Subscribing to Incremental Updates via WebSocket

    Connect to the [Diff Depth Stream]
    https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#diff-depth-stream

    Subscribe to the update stream for the selected pair (e.g., BTCUSDT).

    Merge the snapshot and incoming updates into a single up-to-date order book according to the official algorithm.

1.3 Additional WebSocket Connections

    Open a few more connections (no more than 2) to the same L2 update stream.

    Select the freshest updates from the streams to implement basic arbitrage and obtain the most accurate (up-to-date) data.

1.4 Periodic Order Book Output

    Every ~5 seconds, print the current full state of the order book to the console.

    Important: this must be the result of applying updates to the snapshot, not just a dump of incoming data.

## Components
[book_order.rc](https://github.com/vladIev/book_order/blob/master/src/book_order.rs) - Stores price levels and implements the logic for maintaining the order book.
[updates_provider.rc](https://github.com/vladIev/book_order/blob/master/src/updates_provider.rs) - Starts num_of_sockets subscriptions for depth updates for the given symbol and sends received updates through the tx channel.
[updates_provider.rc](https://github.com/vladIev/book_order/blob/master/src/updates_provider.rs) - Handles depth updates received from `updates_provider.rc`.
