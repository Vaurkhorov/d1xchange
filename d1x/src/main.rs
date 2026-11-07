mod types;

use d1x_protocol::{Request, Response};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::sync::mpsc::error::SendError;
use tokio::{select, signal, task, time};
use types::{ConnectionChannels, Query, QueryResponse, Stock};

const TICK_INTERVAL_MILLISECS: u64 = 10;
const MARKET_OUTPUT_COLOUR: Color = Color::Yellow;
/// How long a connection may sit with a partial, unfinished message buffered
/// before it is disconnected. Resets whenever more bytes arrive.
const PARTIAL_MESSAGE_TIMEOUT: Duration = Duration::from_secs(5);
/// Capacity of a connection's reply channel.
const REPLY_CHANNEL_CAPACITY: usize = 32;
/// Capacity of a connection's event (broadcast) channel. Bigger than the
/// reply channel since it absorbs bursty fan-out, not just one connection's
/// own requests.
const EVENT_CHANNEL_CAPACITY: usize = 2 * REPLY_CHANNEL_CAPACITY;

#[tokio::main]
async fn main() {
    let mut stdout = StandardStream::stdout(ColorChoice::Always);

    let (server_tx, mut market_rx) = mpsc::channel::<(usize, Query)>(32);

    let mut stocks: HashMap<String, Stock> = HashMap::new();
    let mut initial_stocks = vec![Stock::new("V", "Vulyenne")];

    let mut tick_interval = time::interval(time::Duration::from_millis(TICK_INTERVAL_MILLISECS));
    tick_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    while let Some(stock) = initial_stocks.pop() {
        stocks.insert(stock.get_symbol().to_string(), stock);
    }

    // a unique ID is mapped to each connection
    let mut connections: HashMap<usize, mpsc::Sender<QueryResponse>> = HashMap::new();
    let mut event_connections: HashMap<usize, mpsc::Sender<QueryResponse>> = HashMap::new();
    let mut subscriptions: HashMap<String, HashSet<usize>> = HashMap::new();
    let server = task::spawn(serve(server_tx));

    'market_loop: loop {
        tick_interval.tick().await;
        loop {
            for stock in stocks.values_mut() {
                let symbol = stock.get_symbol().to_string();
                let executed_trades = stock.resolve();

                for trade in &executed_trades {
                    market_speak(
                        format!("Market says> Trade executed for {}: {:#?}", stock.get_name(), &trade),
                        &mut stdout,
                        false,
                    );

                    if let Some(buyer_tx) = connections.get(&trade.get_buyer_id()) {
                        if let Err(e) = buyer_tx.send(QueryResponse::ExecutedTrade(*trade)).await {
                            market_speak(
                                format!("Error while sending trade to buyer: {:#?}", e),
                                &mut stdout,
                                true,
                            );
                        }
                    } else {
                        market_speak(
                            format!("Buyer with id {} not connected.", trade.get_buyer_id()),
                            &mut stdout,
                            true,
                        );
                    }

                    if let Some(seller_tx) = connections.get(&trade.get_seller_id()) {
                        if let Err(e) = seller_tx.send(QueryResponse::ExecutedTrade(*trade)).await {
                            market_speak(
                                format!("Error while sending trade to seller: {:#?}", e),
                                &mut stdout,
                                true,
                            );
                        }
                    } else {
                        market_speak(
                            format!("Seller with id {} not connected.", trade.get_seller_id()),
                            &mut stdout,
                            true,
                        );
                    }

                    if has_subscribers(&subscriptions, &symbol) {
                        broadcast(
                            &symbol,
                            QueryResponse::TradeBroadcast {
                                symbol: symbol.clone(),
                                price: trade.get_price(),
                                quantity: trade.get_quantity(),
                                sequence: trade.get_sequence(),
                            },
                            &subscriptions,
                            &event_connections,
                            &mut stdout,
                        );
                    }
                }

                if !executed_trades.is_empty() && has_subscribers(&subscriptions, &symbol) {
                    broadcast(
                        &symbol,
                        order_book_snapshot(&symbol, stock),
                        &subscriptions,
                        &event_connections,
                        &mut stdout,
                    );
                }
            }

            match market_rx.try_recv() {
                Ok((id, query)) => {
                    let status = resolve_query(
                        id,
                        query,
                        &mut connections,
                        &mut event_connections,
                        &mut subscriptions,
                        &mut stocks,
                        &mut stdout,
                    )
                    .await;
                    if let Err(e) = status {
                        market_speak(format!("Error: {:#?}", e), &mut stdout, true);
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    break;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    market_speak("Server disconnected, market shutting down.".to_string(), &mut stdout, false);
                    break 'market_loop;
                }
            }
        }
    }

    if let Err(server_status) = server.await {
        eprintln!("Error: {:#?}", server_status);
    } else {
        println!("Bbye!");
    }
}

async fn resolve_query(
    id: usize,
    query: Query,
    connections: &mut HashMap<usize, mpsc::Sender<QueryResponse>>,
    event_connections: &mut HashMap<usize, mpsc::Sender<QueryResponse>>,
    subscriptions: &mut HashMap<String, HashSet<usize>>,
    stocks: &mut HashMap<String, Stock>,
    stdout: &mut StandardStream,
) -> Result<(), SendError<QueryResponse>> {
    // If there is a new connection, add it, otherwise check if the ID exists first.
    let socket_tx = match query {
        Query::Connect(ConnectionChannels { reply_tx, event_tx }) => {
            connections.insert(id, reply_tx);
            event_connections.insert(id, event_tx);
            let t = connections.get(&id).expect("This key was just added, it must exist.");
            t.send(QueryResponse::Connected).await?;
            return Ok(());
        }
        Query::Disconnect => {
            connections.remove(&id);
            event_connections.remove(&id);
            for subscribers in subscriptions.values_mut() {
                subscribers.remove(&id);
            }
            return Ok(());
        }
        _ => {
            match connections.get(&id) {
                Some(socket_tx) => socket_tx,
                None => {
                    market_speak(format!("Query from unknown id {}.", id), stdout, true);
                    return Ok(());
                }
            }
        }
    };

    match query {
        Query::Connect(_) => {
            unreachable!("Connection should already have been handled.");
        }
        Query::Disconnect => {
            unreachable!("Disconnection should already have been handled.");
        }
        Query::Buy(symbol, order) => {
            if let Some(stock) = stocks.get_mut(&symbol) {
                stock.add_buy_order(order);
                // Broadcast before acking the submitter: the book was already
                // mutated, so subscribers must learn about it regardless of
                // whether the submitter is still around to receive its ack.
                if has_subscribers(subscriptions, &symbol) {
                    broadcast(&symbol, order_book_snapshot(&symbol, stock), subscriptions, event_connections, stdout);
                }
                socket_tx.send(QueryResponse::OrderPosted).await?;
            } else {
                socket_tx.send(QueryResponse::SymbolNotFound).await?;
            }
        }
        Query::Sell(symbol, order) => {
            if let Some(stock) = stocks.get_mut(&symbol) {
                stock.add_sell_order(order);
                if has_subscribers(subscriptions, &symbol) {
                    broadcast(&symbol, order_book_snapshot(&symbol, stock), subscriptions, event_connections, stdout);
                }
                socket_tx.send(QueryResponse::OrderPosted).await?;
            } else {
                socket_tx.send(QueryResponse::SymbolNotFound).await?;
            }
        }
        Query::Ohlc(symbol) => {
            if let Some(stock) = stocks.get(&symbol) {
                let (open, high, low, close) = stock.get_ohlc();
                socket_tx.send(QueryResponse::Ohlc(open, high, low, close)).await?;
            } else {
                socket_tx.send(QueryResponse::SymbolNotFound).await?;
            }
        }
        Query::BuyOrders(symbol) => {
            if let Some(stock) = stocks.get(&symbol) {
                socket_tx.send(QueryResponse::QueriedOrders(stock.get_buy_orders())).await?;
            } else {
                socket_tx.send(QueryResponse::SymbolNotFound).await?;
            }
        }
        Query::SellOrders(symbol) => {
            if let Some(stock) = stocks.get(&symbol) {
                socket_tx.send(QueryResponse::QueriedOrders(stock.get_sell_orders())).await?;
            } else {
                socket_tx.send(QueryResponse::SymbolNotFound).await?;
            }
        }
        Query::Subscribe(symbol) => {
            if let Some(stock) = stocks.get(&symbol) {
                subscriptions.entry(symbol.clone()).or_insert_with(HashSet::new).insert(id);
                socket_tx.send(QueryResponse::Subscribed(symbol.clone())).await?;
                socket_tx.send(order_book_snapshot(&symbol, stock)).await?;
            } else {
                socket_tx.send(QueryResponse::SymbolNotFound).await?;
            }
        }
        Query::Unsubscribe(symbol) => {
            if stocks.contains_key(&symbol) {
                if let Some(subscribers) = subscriptions.get_mut(&symbol) {
                    subscribers.remove(&id);
                }
                socket_tx.send(QueryResponse::Unsubscribed(symbol)).await?;
            } else {
                socket_tx.send(QueryResponse::SymbolNotFound).await?;
            }
        }
    }

    Ok(())
}

/// Whether anyone is currently subscribed to `symbol`. Lets callers skip
/// building a broadcast payload (e.g. an order book snapshot) when nobody
/// would receive it.
fn has_subscribers(subscriptions: &HashMap<String, HashSet<usize>>, symbol: &str) -> bool {
    subscriptions.get(symbol).is_some_and(|subscribers| !subscribers.is_empty())
}

/// Builds an `OrderBookChanged` snapshot of `stock`'s current top-of-book.
fn order_book_snapshot(symbol: &str, stock: &Stock) -> QueryResponse {
    QueryResponse::OrderBookChanged {
        symbol: symbol.to_string(),
        buy_orders: stock.get_buy_orders(),
        sell_orders: stock.get_sell_orders(),
    }
}

/// Pushes `response` to every connection subscribed to `symbol`, through
/// their event channel. Uses `try_send`, not `send().await`: a slow or
/// stuck subscriber must not be able to block delivery to anyone else, or
/// stall the market tick loop this is called from.
fn broadcast(
    symbol: &str,
    response: QueryResponse,
    subscriptions: &HashMap<String, HashSet<usize>>,
    event_connections: &HashMap<usize, mpsc::Sender<QueryResponse>>,
    stdout: &mut StandardStream,
) {
    let Some(subscriber_ids) = subscriptions.get(symbol) else {
        return;
    };

    for subscriber_id in subscriber_ids {
        if let Some(event_tx) = event_connections.get(subscriber_id) {
            match event_tx.try_send(response.clone()) {
                Ok(()) => {}
                // Expected under load: the subscriber's event channel is
                // backed up, so this event is dropped in favor of not
                // blocking anyone else. The next event will get another
                // chance. Not logged - this can happen every tick for a
                // slow subscriber, and that's by design, not an error.
                Err(mpsc::error::TrySendError::Full(_)) => {}
                // Unexpected: the receiver is gone but Query::Disconnect
                // hasn't cleaned up this subscription yet. Rare, so worth
                // logging rather than dropping silently.
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    market_speak(
                        format!("Event channel for subscriber {} is closed but not yet cleaned up", subscriber_id),
                        stdout,
                        true,
                    );
                }
            }
        }
    }
}

/// Prints a message to the terminal with a different colour for the market.
///
/// This colour is defined by `MARKET_OUTPUT_COLOUR`.
fn market_speak(message: String, stdout: &mut StandardStream, error: bool) {
    if let Err(e) = stdout.set_color(ColorSpec::new().set_fg(Some(MARKET_OUTPUT_COLOUR))) {
        println!(
            "Could not set terminal color({:#?}). The next statement is probably from the Market:",
            e
        );
    }

    if error {
        eprintln!("{}", message);
    } else {
        println!("{}", message);
    }

    if let Err(e) = stdout.reset() {
        println!(
            "Could not set terminal color({:#?}). Market output ends.",
            e
        );
    }
}

pub async fn serve(tx: mpsc::Sender<(usize, Query)>) -> Result<(), std::io::Error> {
    let mut next_id = 1;
    let mut connection_future_set = task::JoinSet::new();
    
    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    let (shutdown_signal_tx, shutdown_signal_rx) = watch::channel(false);
        
    loop {
        select! {
            sigint = signal::ctrl_c() => {
                if let Err(e) = sigint {
                    eprintln!("Error while waiting for ctrl-c: {:#?}, stopping server.", e);
                }
                break;
            }

            socket_result = listener.accept() => {
                let (mut socket, _) = match socket_result {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("Error while accepting connection: {:#?}", e);
                        continue;
                    }
                };
        
                let conn_id: usize = next_id;
                next_id += 1;

                let (reply_tx, reply_rx) = mpsc::channel::<QueryResponse>(REPLY_CHANNEL_CAPACITY);
                let (event_tx, event_rx) = mpsc::channel::<QueryResponse>(EVENT_CHANNEL_CAPACITY);

                if let Err(e) = tx.send((conn_id, Query::Connect(ConnectionChannels { reply_tx, event_tx }))).await {
                    eprintln!("Encountered error while sending {:#?}", e);
                    if let Err(e) = socket.shutdown().await {
                        eprintln!("Error while shutting down socket: {:#?}", e);
                    }
                    continue;
                }

                connection_future_set.spawn(connection_handler(conn_id, tx.clone(), reply_rx, event_rx, socket, shutdown_signal_rx.clone()));
            }
        }
    }

    match shutdown_signal_tx.send(true) {
        Ok(()) => {
            let results = connection_future_set.join_all().await;
            for result in results {
                if let Err((id, e)) = result {
                    eprintln!("Connection with id {} returned error: {:#?}", id, e);
                }
            }
        },
        Err(e) => {
            eprintln!("Error while sending shutdown signal: {:#?}, forcing shutdown on sockets.", e);
            
            // I don't see a need to manually shutdown the sockets here.
            // connection_future_set.shutdown().await;
        },
    }

    Ok(())
}

/// Thin wrapper around `run_connection` that guarantees the market is
/// notified of this connection's end on every exit path (clean close,
/// read/write errors, the idle timeout, or server shutdown), from one
/// place, regardless of which branch of `run_connection`'s loop triggered it.
async fn connection_handler(
    id: usize,
    tx: mpsc::Sender<(usize, Query)>,
    reply_rx: mpsc::Receiver<QueryResponse>,
    event_rx: mpsc::Receiver<QueryResponse>,
    socket: TcpStream,
    shutdown_signal: watch::Receiver<bool>,
) -> Result<(), (usize, std::io::Error)> {
    let result = run_connection(id, tx.clone(), reply_rx, event_rx, socket, shutdown_signal).await;

    // Best-effort: by the time we're here the handler is already exiting, and
    // if the market itself is gone there's nothing left to notify anyway.
    if let Err(e) = tx.send((id, Query::Disconnect)).await {
        eprintln!("Error notifying market of disconnect for id {}: {:#?}", id, e);
    }

    result
}

async fn run_connection(
    id: usize,
    tx: mpsc::Sender<(usize, Query)>,
    mut reply_rx: mpsc::Receiver<QueryResponse>,
    mut event_rx: mpsc::Receiver<QueryResponse>,
    mut socket: TcpStream,
    mut shutdown_signal: watch::Receiver<bool>,
) -> Result<(), (usize, std::io::Error)> {
    let mut socket_buffer = [0u8; 1024];
    let mut read_buffer: Vec<u8> = Vec::new();
    let mut idle_deadline = Box::pin(time::sleep(PARTIAL_MESSAGE_TIMEOUT));
    loop {
        select! {
            query_response = reply_rx.recv() => {
                let response = match query_response {
                    Some(r) => r.to_wire(),
                    None => {
                        // The market should not be closed before sockets.
                        let response = serde_json::to_string(&Response::MarketClosed).expect("Response serialization should not fail.");
                        socket.write(response.as_bytes()).await.map_err(|e| (id, e))?;
                        socket.shutdown().await.map_err(|e| (id, e))?;
                        continue;
                    }
                };

                let response = serde_json::to_string(&response).expect("Response serialization should not fail.");
                if let Err(e) = socket.write_all(response.as_bytes()).await {
                    eprintln!("Error while writing to socket: {:#?}", &e);
                    break Err((id, e));
                }
            }
            event = event_rx.recv() => {
                // `None` just means event_tx was dropped (e.g. on disconnect
                // cleanup or full server shutdown); reply_rx's None arm
                // already handles the MarketClosed notification, so there's
                // nothing further to do here on None.
                if let Some(event) = event {
                    let response = serde_json::to_string(&event.to_wire()).expect("Response serialization should not fail.");
                    if let Err(e) = socket.write_all(response.as_bytes()).await {
                        eprintln!("Error while writing to socket: {:#?}", &e);
                        break Err((id, e));
                    }
                }
            }
            socket_query = socket.read(&mut socket_buffer) => {
                let requests = match socket_query {
                    Ok(0) => {
                        break Ok(());
                    }
                    Ok(n) => {
                        println!("Received: {}", String::from_utf8_lossy(&socket_buffer[0..n]));
                        read_buffer.extend_from_slice(&socket_buffer[0..n]);

                        let requests = match d1x_protocol::drain_json_values::<Request>(&mut read_buffer) {
                            Ok(requests) => requests,
                            Err(()) => {
                                println!("Error parsing JSON, dropping buffered bytes.");
                                let response = serde_json::to_string(&Response::MalformedRequest).expect("Response serialization should not fail.");
                                socket.write(response.as_bytes()).await.map_err(|e| (id, e))?;
                                continue;
                            }
                        };

                        if !read_buffer.is_empty() {
                            idle_deadline.as_mut().reset(time::Instant::now() + PARTIAL_MESSAGE_TIMEOUT);
                        }

                        requests
                    }
                    Err(e) => {
                        eprintln!("Error while reading from socket: {:#?}", e);
                        break Err((id, e));
                    }
                };

                for request in requests {
                    let query = Query::from_request(id, request);
                    if let Err(e) = tx.send((id, query)).await {
                        eprintln!("Error while sending query: {:#?}", e);
                        return Ok(());
                    }
                }
            }
            () = &mut idle_deadline, if !read_buffer.is_empty() => {
                println!("Connection {} left a message unfinished for too long, disconnecting.", id);
                socket.shutdown().await.map_err(|e| (id, e))?;
                break Ok(());
            }
            _ = shutdown_signal.changed() => {
                socket.shutdown().await.map_err(|e| (id, e))?;
                break Ok(());
            }
        }
    }
}

