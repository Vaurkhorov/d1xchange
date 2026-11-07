use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The largest a buffered, not-yet-complete message is allowed to grow before
/// it is treated as corrupt rather than merely slow to arrive.
///
/// This bounds per-connection memory use: without it, a connection that never
/// finishes sending a JSON value (deliberately or due to a stalled network)
/// would let its read buffer grow without limit.
pub const MAX_MESSAGE_BYTES: usize = 64 * 1024;

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
    Subscribe {
        symbol: String,
    },
    Unsubscribe {
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
    Subscribed {
        symbol: String,
    },
    Unsubscribed {
        symbol: String,
    },
    TradeBroadcast {
        symbol: String,
        price: f64,
        quantity: usize,
        /// This trade's position in its symbol's trade sequence. A gap from
        /// the last sequence number seen means a `TradeBroadcast` was
        /// dropped (delivery is best-effort).
        sequence: u64,
    },
    OrderBookChanged {
        symbol: String,
        buy_orders: Vec<OrderLevel>,
        sell_orders: Vec<OrderLevel>,
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

/// Extracts as many complete JSON values of type `T` as are present in `buffer`,
/// draining the bytes they occupied.
///
/// TCP gives no message boundaries, so a single `read()` can return multiple
/// concatenated values, a partial value, or both. Any trailing partial value is
/// left in `buffer` for the next call, unless it has grown past
/// `MAX_MESSAGE_BYTES`. If the buffered bytes are invalid JSON (not merely
/// incomplete) or exceed that limit, `buffer` is cleared and `Err` is
/// returned, since there is no reliable way to know where a corrupt value ends.
pub fn drain_json_values<T: DeserializeOwned>(buffer: &mut Vec<u8>) -> Result<Vec<T>, ()> {
    let mut values = Vec::new();

    loop {
        let mut stream = serde_json::Deserializer::from_slice(buffer).into_iter::<T>();
        match stream.next() {
            Some(Ok(value)) => {
                let consumed = stream.byte_offset();
                drop(stream);
                buffer.drain(0..consumed);
                values.push(value);
            }
            Some(Err(e)) if e.is_eof() => {
                if buffer.len() > MAX_MESSAGE_BYTES {
                    buffer.clear();
                    return Err(());
                }
                break;
            }
            Some(Err(_)) => {
                buffer.clear();
                return Err(());
            }
            None => {
                buffer.clear();
                break;
            }
        }
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drains_concatenated_values() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(br#"{"type":"ohlc","symbol":"V"}{"type":"buy_orders","symbol":"V"#);

        // First value is complete; second is still partial.
        let values = drain_json_values::<Request>(&mut buffer).unwrap();
        assert_eq!(values.len(), 1);
        assert!(matches!(values[0], Request::Ohlc { .. }));
        assert_eq!(buffer, br#"{"type":"buy_orders","symbol":"V"#);

        // Completing the second value should now parse it.
        buffer.extend_from_slice(br#""}"#);
        let values = drain_json_values::<Request>(&mut buffer).unwrap();
        assert_eq!(values.len(), 1);
        assert!(matches!(values[0], Request::BuyOrders { .. }));
        assert!(buffer.is_empty());
    }

    #[test]
    fn rejects_oversized_incomplete_value() {
        let mut buffer = Vec::new();
        // An endless, never-closed string value: always "incomplete", never valid.
        buffer.extend_from_slice(br#"{"type":"buy","symbol":""#);
        buffer.extend(std::iter::repeat(b'a').take(MAX_MESSAGE_BYTES));

        assert!(drain_json_values::<Request>(&mut buffer).is_err());
        assert!(buffer.is_empty());
    }

    #[test]
    fn rejects_invalid_json() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(b"not json");
        assert!(drain_json_values::<Request>(&mut buffer).is_err());
        assert!(buffer.is_empty());
    }

    #[test]
    fn subscribe_requests_round_trip() {
        let subscribe = Request::Subscribe { symbol: "V".to_string() };
        let json = serde_json::to_string(&subscribe).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, Request::Subscribe { symbol } if symbol == "V"));

        let unsubscribe = Request::Unsubscribe { symbol: "V".to_string() };
        let json = serde_json::to_string(&unsubscribe).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, Request::Unsubscribe { symbol } if symbol == "V"));
    }

    #[test]
    fn event_responses_round_trip() {
        let responses = vec![
            Response::Subscribed { symbol: "V".to_string() },
            Response::Unsubscribed { symbol: "V".to_string() },
            Response::TradeBroadcast { symbol: "V".to_string(), price: 150.0, quantity: 10, sequence: 0 },
            Response::OrderBookChanged {
                symbol: "V".to_string(),
                buy_orders: vec![OrderLevel { price: 150.0, quantity: 10 }],
                sell_orders: vec![],
            },
        ];

        for response in responses {
            let json = serde_json::to_string(&response).unwrap();
            let parsed: Response = serde_json::from_str(&json).unwrap();
            assert_eq!(format!("{:?}", parsed), format!("{:?}", response));
        }
    }
}
