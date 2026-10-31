use serde::{Deserialize, Serialize};

/// A request sent from a client to the market over the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    Buy {
        symbol: String,
        price: f64,
        quantity: usize,
    },
    Sell {
        symbol: String,
        price: f64,
        quantity: usize,
    },
    Ohlc {
        symbol: String,
    },
    BuyOrders {
        symbol: String,
    },
    SellOrders {
        symbol: String,
    },
}

/// A response sent from the market to a client over the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "response", rename_all = "snake_case")]
pub enum Response {
    Connected,
    OrderPosted,
    QueriedOrders {
        orders: Vec<OrderLevel>,
    },
    Ohlc {
        open: Option<f64>,
        high: Option<f64>,
        low: Option<f64>,
        close: Option<f64>,
    },
    ExecutedTrade {
        buyer_id: usize,
        seller_id: usize,
        price: f64,
        quantity: usize,
    },
    SymbolNotFound,
    MarketClosed,
    MalformedRequest,
}

/// The aggregated quantity pending at a single price level in an order book.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderLevel {
    pub price: f64,
    pub quantity: usize,
}
