pub enum Command {
    Buy(String, f64, usize),
    Sell(String, f64, usize),
    Ohlc(String),
    BuyOrders(String),
    SellOrders(String),
}

impl Command {
    pub fn get_json_message(&self) -> String {
        match self {
            Command::Buy(symbol, price, quantity) => {
                format!(r#"{{"type": "buy", "symbol": "{}", "price": {}, "quantity": {}}}"#, symbol, price, quantity)
            },
            Command::Sell(symbol, price, quantity) => {
                format!(r#"{{"type": "sell", "symbol": "{}", "price": {}, "quantity": {}}}"#, symbol, price, quantity)
            },
            Command::Ohlc(symbol) => {
                format!(r#"{{"type": "ohlc", "symbol": "{}"}}"#, symbol)
            },
            Command::BuyOrders(symbol) => {
                format!(r#"{{"type": "buy_orders", "symbol": "{}"}}"#, symbol)
            },
            Command::SellOrders(symbol) => {
                format!(r#"{{"type": "sell_orders", "symbol": "{}"}}"#, symbol)
            },
        }
    }
}