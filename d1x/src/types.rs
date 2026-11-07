use chrono::{DateTime, Utc};
use d1x_protocol::{OrderLevel, Request, Response};
use std::collections::HashMap;
use tokio::sync::mpsc;

// 10 raised to the number of decimals to keep for prices.
const PRICE_PRECISION_FACTOR: f64 = 1e2;
/// Number of unique prices that are checked for in the order book.
const NO_OF_PRICES_QUERIED: usize = 5;

/// Holds details for a stock and its orders.
pub struct Stock {
    /// The symbol of the stock (e.g., "ORT").
    ///
    /// This is used for querying the stock, and so must be unique.
    symbol: String,
    /// The full name of the stock (e.g., "Orchard de Rosa et Tulipan")
    name: String,
    /// Buy orders for the stock.
    buy_orders: Vec<Order>,
    /// Sell orders for the stock.
    sell_orders: Vec<Order>,
    /// Open, high, low, close prices for the stock.
    ohlc: Ohlc,
    /// The sequence number to assign to the next trade for this stock.
    ///
    /// Lets a `TradeBroadcast` subscriber detect gaps caused by best-effort,
    /// droppable delivery: a jump in sequence numbers means a trade was missed.
    next_trade_sequence: u64,
}

impl Stock {
    /// Creates a new stock with the given symbol and name.
    pub fn new(symbol: &str, name: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            name: name.to_string(),
            buy_orders: Vec::new(),
            sell_orders: Vec::new(),
            ohlc: Ohlc::new(),
            next_trade_sequence: 0,
        }
    }

    /// Returns the symbol of the stock.
    pub fn get_symbol(&self) -> &str {
        &self.symbol
    }

    /// Returns the name of the stock.
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Adds a buy order to the stock.
    pub fn add_buy_order(&mut self, order: Order) {
        self.buy_orders.push(order);
        self.sort_orders();
    }

    /// Adds a sell order to the stock.
    pub fn add_sell_order(&mut self, order: Order) {
        self.sell_orders.push(order);
        self.sort_orders();
    }

    /// Returns pending buy orders for the stock, sorted in descending order of price.
    pub fn get_buy_orders(&self) -> Vec<(f64, usize)> {
        let mut pricelist = HashMap::<usize, usize>::new();

        for order in &self.buy_orders {
            let price = order.get_unadjusted_price();
            let quantity = order.get_quantity();

            if let Some(existing_price) = pricelist.get(&price) {
                pricelist.insert(price, existing_price + quantity);
            } else {
                if pricelist.len() >= NO_OF_PRICES_QUERIED {
                    break;
                }
                pricelist.insert(price, quantity);
            }
        }

        let mut pricelist: Vec<(f64, usize)> = pricelist
            .iter()
            .map(|(price, quantity)| ((*price as f64) / PRICE_PRECISION_FACTOR, *quantity))
            .collect();
        pricelist.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .expect("prices are f64s and should be comparable.")
        });
        pricelist
    }

    /// Returns pending sell orders for the stock, sorted in ascending order of price.
    pub fn get_sell_orders(&self) -> Vec<(f64, usize)> {
        let mut pricelist = HashMap::<usize, usize>::new();

        for order in &self.sell_orders {
            let price = order.get_unadjusted_price();
            let quantity = order.get_quantity();

            if let Some(existing_price) = pricelist.get(&price) {
                pricelist.insert(price, existing_price + quantity);
            } else {
                if pricelist.len() >= NO_OF_PRICES_QUERIED {
                    break;
                }
                pricelist.insert(price, quantity);
            }
        }

        let mut pricelist: Vec<(f64, usize)> = pricelist
            .iter()
            .map(|(price, quantity)| ((*price as f64) / PRICE_PRECISION_FACTOR, *quantity))
            .collect();
        pricelist.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .expect("prices are f64s and should be comparable.")
        });
        pricelist
    }

    /// Resolves trades between buy and sell orders.
    pub fn resolve(&mut self) -> Vec<Trade> {
        let mut trades = Vec::new();

        for buy_order in &mut self.buy_orders {
            if let Some(lowest_sell_offer) = self.sell_orders.first() {
                if buy_order.get_price() < lowest_sell_offer.get_price() {
                    // Highest buy bid is less than lowest sell offer
                    break;
                }
            } else {
                // No sell orders left
                break;
            }

            for sell_order in &mut self.sell_orders {
                if sell_order.get_quantity() == 0 {
                    // These might be left over after being resolved.
                    continue;
                }

                if buy_order.get_price() >= sell_order.get_price() {
                    let price = if sell_order.get_time() < buy_order.get_time() {
                        sell_order.get_price()
                    } else {
                        buy_order.get_price()
                    };
                    let quantity = buy_order.get_quantity().min(sell_order.get_quantity());

                    buy_order.resolve(quantity);
                    sell_order.resolve(quantity);
                    trades.push(Trade::new(
                        buy_order.creator_id,
                        sell_order.creator_id,
                        price,
                        quantity,
                        self.next_trade_sequence,
                    ));
                    self.next_trade_sequence += 1;
                    self.ohlc.update(price);

                    if buy_order.get_quantity() == 0 {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        self.buy_orders.retain(|order| order.get_quantity() > 0);
        self.sell_orders.retain(|order| order.get_quantity() > 0);

        trades
    }

    /// Sorts buy and sell orders by price.
    fn sort_orders(&mut self) {
        self.buy_orders.sort_by(|a, b| {
            b.price
                .partial_cmp(&a.price)
                .expect("prices are f64s and should be comparable.")
        });
        self.sell_orders.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .expect("prices are f64s and should be comparable.")
        });
    }

    /// Returns the open, high, low, close prices for the stock.
    pub fn get_ohlc(&self) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
        self.ohlc.get()
    }
}

/// An order to buy or sell a stock.
pub struct Order {
    /// The ID of the creator of the order.
    creator_id: usize,
    /// The price per stock.
    price: usize,
    /// The quantity of the order.
    quantity: usize,
    /// The time the order was created.
    ///
    /// The price listed on the order that was created earlier is considered while resolving orders.
    time: DateTime<Utc>,
}

impl Order {
    /// Creates a new order with the given creator ID, price, and quantity.
    pub fn new(creator_id: usize, price: f64, quantity: usize) -> Self {
        let price = (price * PRICE_PRECISION_FACTOR) as usize;

        Self {
            creator_id,
            price,
            quantity,
            time: Utc::now(),
        }
    }

    /// Returns the total value of the order.
    pub fn get_value(&self) -> f64 {
        (self.price as f64) * (self.quantity as f64) / PRICE_PRECISION_FACTOR
    }

    /// Returns the price per stock of the order.
    pub fn get_price(&self) -> f64 {
        self.price as f64 / PRICE_PRECISION_FACTOR
    }

    /// Returns the price per stock WITHOUT adjusting for the precision factor.
    fn get_unadjusted_price(&self) -> usize {
        self.price
    }

    /// Returns the quantity of the order.
    pub fn get_quantity(&self) -> usize {
        self.quantity
    }

    /// Returns the time the order was created.
    pub fn get_time(&self) -> DateTime<Utc> {
        self.time
    }

    /// Reduces the quantity of the order by the given amount.
    pub fn resolve(&mut self, quantity: usize) {
        self.quantity -= quantity;
    }
}

#[derive(Debug, Copy, Clone)]
/// A log of a resolved trade between a buyer and a seller.
pub struct Trade {
    /// The ID of the buyer.
    buyer_id: usize,
    /// The ID of the seller.
    seller_id: usize,
    /// The price per stock.
    price: f64,
    /// The quantity of the trade.
    quantity: usize,
    /// This trade's position in its stock's trade sequence. Lets a
    /// `TradeBroadcast` subscriber detect a gap from dropped delivery.
    sequence: u64,
}

impl Trade {
    /// Creates a new trade with the given buyer ID, seller ID, price, quantity, and sequence number.
    fn new(buyer_id: usize, seller_id: usize, price: f64, quantity: usize, sequence: u64) -> Self {
        Self {
            buyer_id,
            seller_id,
            price,
            quantity,
            sequence,
        }
    }

    pub fn get_buyer_id(&self) -> usize {
        self.buyer_id
    }

    pub fn get_seller_id(&self) -> usize {
        self.seller_id
    }

    pub fn get_price(&self) -> f64 {
        self.price
    }

    pub fn get_quantity(&self) -> usize {
        self.quantity
    }

    pub fn get_sequence(&self) -> u64 {
        self.sequence
    }
}

/// Open, high, low, close prices for a stock.
pub struct Ohlc {
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
}

impl Ohlc {
    /// Creates a blank OHLC struct.
    ///
    /// Values are all set to `None` until the first update.
    fn new() -> Self {
        Self {
            open: None,
            high: None,
            low: None,
            close: None,
        }
    }

    /// Updates the OHLC values according to the latest trade price provided.
    fn update(&mut self, latest_price: f64) {
        if self.open.is_none() {
            self.open = Some(latest_price);
        }

        self.high = Some(self.high.unwrap_or(latest_price).max(latest_price));
        self.low = Some(self.low.unwrap_or(latest_price).min(latest_price));
        self.close = Some(latest_price);
    }

    /// Returns the open, high, low, close prices.
    pub fn get(&self) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
        (self.open, self.high, self.low, self.close)
    }
}

/// The two outgoing channels a connection is reachable through: `reply_tx`
/// for direct replies to its own requests (guaranteed delivery), and
/// `event_tx` for broadcast events it's subscribed to (best-effort, may
/// drop under load so a slow subscriber can't stall delivery to anyone else).
pub struct ConnectionChannels {
    pub reply_tx: mpsc::Sender<QueryResponse>,
    pub event_tx: mpsc::Sender<QueryResponse>,
}

/// A query to the market.
pub enum Query {
    /// Post a buy order for the stock.
    Buy(String, Order),
    /// Post a sell order for the stock.
    Sell(String, Order),
    /// Query the OHLC prices for the stock.
    Ohlc(String),
    /// Query the pending buy orders for the stock.
    BuyOrders(String),
    /// Query the pending sell orders for the stock.
    SellOrders(String),
    /// Subscribe to events (trades, order book changes) for the stock.
    Subscribe(String),
    /// Unsubscribe from events for the stock.
    Unsubscribe(String),
    /// New connection
    Connect(ConnectionChannels),
    /// The connection has ended; forget it and any subscriptions it held.
    Disconnect,
}

impl Query {
    /// Builds an internal `Query` from a wire-level `Request`, attaching the id of the connection it arrived on.
    pub fn from_request(id: usize, request: Request) -> Self {
        match request {
            Request::Buy { symbol, price, quantity } => Query::Buy(symbol, Order::new(id, price, quantity)),
            Request::Sell { symbol, price, quantity } => Query::Sell(symbol, Order::new(id, price, quantity)),
            Request::Ohlc { symbol } => Query::Ohlc(symbol),
            Request::BuyOrders { symbol } => Query::BuyOrders(symbol),
            Request::SellOrders { symbol } => Query::SellOrders(symbol),
            Request::Subscribe { symbol } => Query::Subscribe(symbol),
            Request::Unsubscribe { symbol } => Query::Unsubscribe(symbol),
        }
    }
}

/// A response from the market to a query.
#[derive(Debug, Clone)]
pub enum QueryResponse {
    // Successes
    /// Socket tx stored.
    Connected,
    /// The order was successfully posted.
    OrderPosted,
    /// A vector of pending orders for the stock.
    ///
    /// It contains a limited number of unique prices and their quantities. The number of unique prices is defined by `NO_OF_PRICES_QUERIED`.
    QueriedOrders(Vec<(f64, usize)>),
    /// The open, high, low, close prices for the stock.
    Ohlc(Option<f64>, Option<f64>, Option<f64>, Option<f64>),
    /// Receipt of a completed trade.
    ExecutedTrade(Trade),
    /// Acknowledges a subscription to a stock's events.
    Subscribed(String),
    /// Acknowledges an unsubscription from a stock's events.
    Unsubscribed(String),
    /// A trade occurred on a subscribed stock (anonymized, no buyer/seller id).
    TradeBroadcast { symbol: String, price: f64, quantity: usize, sequence: u64 },
    /// The order book for a subscribed stock changed.
    OrderBookChanged { symbol: String, buy_orders: Vec<(f64, usize)>, sell_orders: Vec<(f64, usize)> },

    // Errors
    /// The symbol provided was not found.
    SymbolNotFound,
}

impl QueryResponse {
    /// Converts an internal `QueryResponse` into its wire-level `Response` representation.
    pub fn to_wire(&self) -> Response {
        match self {
            QueryResponse::Connected => Response::Connected,
            QueryResponse::OrderPosted => Response::OrderPosted,
            QueryResponse::QueriedOrders(orders) => Response::QueriedOrders {
                orders: orders
                    .iter()
                    .map(|(price, quantity)| OrderLevel { price: *price, quantity: *quantity })
                    .collect(),
            },
            QueryResponse::Ohlc(open, high, low, close) => Response::Ohlc {
                open: *open,
                high: *high,
                low: *low,
                close: *close,
            },
            QueryResponse::ExecutedTrade(trade) => Response::ExecutedTrade {
                buyer_id: trade.buyer_id,
                seller_id: trade.seller_id,
                price: trade.price,
                quantity: trade.quantity,
            },
            QueryResponse::Subscribed(symbol) => Response::Subscribed { symbol: symbol.clone() },
            QueryResponse::Unsubscribed(symbol) => Response::Unsubscribed { symbol: symbol.clone() },
            QueryResponse::TradeBroadcast { symbol, price, quantity, sequence } => Response::TradeBroadcast {
                symbol: symbol.clone(),
                price: *price,
                quantity: *quantity,
                sequence: *sequence,
            },
            QueryResponse::OrderBookChanged { symbol, buy_orders, sell_orders } => Response::OrderBookChanged {
                symbol: symbol.clone(),
                buy_orders: buy_orders
                    .iter()
                    .map(|(price, quantity)| OrderLevel { price: *price, quantity: *quantity })
                    .collect(),
                sell_orders: sell_orders
                    .iter()
                    .map(|(price, quantity)| OrderLevel { price: *price, quantity: *quantity })
                    .collect(),
            },
            QueryResponse::SymbolNotFound => Response::SymbolNotFound,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests trade resolution, checking the returned logs and stored pending orders.
    #[test]
    fn test_resolve_trade() {
        let mut stock = Stock::new("ORT", "Orchard de Rosa et Tulipan");
        let buy_order = Order::new(1, 150.5, 10);
        let sell_order = Order::new(2, 150.0, 5);

        stock.add_buy_order(buy_order);
        stock.add_sell_order(sell_order);

        let trades = stock.resolve();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].buyer_id, 1);
        assert_eq!(trades[0].seller_id, 2);
        assert_eq!(trades[0].price, 150.5);
        assert_eq!(trades[0].quantity, 5);
        assert_eq!(trades[0].sequence, 0);

        // Verify remaining orders
        assert_eq!(stock.get_buy_orders()[0].1, 5);
        assert!(stock.get_sell_orders().is_empty());
    }

    /// Trade sequence numbers increment per stock, across separate resolve() calls,
    /// so a TradeBroadcast subscriber can detect a gap from dropped delivery.
    #[test]
    fn test_trade_sequence_increments() {
        let mut stock = Stock::new("ORT", "Orchard de Rosa et Tulipan");

        stock.add_buy_order(Order::new(1, 150.0, 5));
        stock.add_sell_order(Order::new(2, 150.0, 5));
        let trades = stock.resolve();
        assert_eq!(trades[0].sequence, 0);

        stock.add_buy_order(Order::new(3, 150.0, 5));
        stock.add_sell_order(Order::new(4, 150.0, 5));
        let trades = stock.resolve();
        assert_eq!(trades[0].sequence, 1);
    }

    /// Tests whether OHLC is updated correctly.
    #[test]
    fn test_ohlc_update() {
        let mut ohlc = Ohlc::new();
        ohlc.update(150.0);
        ohlc.update(155.0);
        ohlc.update(145.0);
        ohlc.update(148.0);

        let (open, high, low, close) = ohlc.get();
        assert_eq!(open, Some(150.0));
        assert_eq!(high, Some(155.0));
        assert_eq!(low, Some(145.0));
        assert_eq!(close, Some(148.0));
    }

    /// Tests buy queries.
    #[test]
    fn test_query_buy_orders() {
        let mut stock = Stock::new("ORT", "Orchard de Rosa et Tulipan");
        stock.add_buy_order(Order::new(1, 150.0, 10));
        stock.add_buy_order(Order::new(2, 155.0, 5));
        stock.add_buy_order(Order::new(3, 150.0, 15));

        let buy_orders = stock.get_buy_orders();
        assert_eq!(buy_orders.len(), 2); // Only unique prices are kept
        assert_eq!(buy_orders[0], (155.0, 5)); // Highest price first
        assert_eq!(buy_orders[1], (150.0, 25)); // Combined quantities
    }

    #[test]
    fn test_query_sell_orders() {
        let mut stock = Stock::new("ORT", "Orchard de Rosa et Tulipan");
        stock.add_sell_order(Order::new(1, 145.0, 10));
        stock.add_sell_order(Order::new(2, 140.0, 5));
        stock.add_sell_order(Order::new(3, 145.0, 15));

        let sell_orders = stock.get_sell_orders();
        assert_eq!(sell_orders.len(), 2); // Only unique prices are kept
        assert_eq!(sell_orders[0], (140.0, 5)); // Lowest price first
        assert_eq!(sell_orders[1], (145.0, 25)); // Combined quantities
    }

    #[test]
    fn from_request_maps_subscribe_and_unsubscribe() {
        let query = Query::from_request(1, Request::Subscribe { symbol: "V".to_string() });
        assert!(matches!(query, Query::Subscribe(symbol) if symbol == "V"));

        let query = Query::from_request(1, Request::Unsubscribe { symbol: "V".to_string() });
        assert!(matches!(query, Query::Unsubscribe(symbol) if symbol == "V"));
    }

    #[test]
    fn to_wire_maps_event_responses() {
        assert!(matches!(
            QueryResponse::Subscribed("V".to_string()).to_wire(),
            Response::Subscribed { symbol } if symbol == "V"
        ));
        assert!(matches!(
            QueryResponse::Unsubscribed("V".to_string()).to_wire(),
            Response::Unsubscribed { symbol } if symbol == "V"
        ));

        let wire = QueryResponse::TradeBroadcast { symbol: "V".to_string(), price: 150.0, quantity: 10, sequence: 3 }.to_wire();
        assert!(matches!(
            wire,
            Response::TradeBroadcast { symbol, price, quantity, sequence } if symbol == "V" && price == 150.0 && quantity == 10 && sequence == 3
        ));

        let wire = QueryResponse::OrderBookChanged {
            symbol: "V".to_string(),
            buy_orders: vec![(150.0, 10)],
            sell_orders: vec![],
        }
        .to_wire();
        match wire {
            Response::OrderBookChanged { symbol, buy_orders, sell_orders } => {
                assert_eq!(symbol, "V");
                assert_eq!(buy_orders.len(), 1);
                assert_eq!(buy_orders[0].price, 150.0);
                assert_eq!(buy_orders[0].quantity, 10);
                assert!(sell_orders.is_empty());
            }
            _ => panic!("expected Response::OrderBookChanged"),
        }
    }
}
