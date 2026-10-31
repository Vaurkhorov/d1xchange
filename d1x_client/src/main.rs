use d1x_protocol::{Request, Response};
use std::io;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::task;

#[tokio::main]
async fn main() {
    let mut buffer = String::new();
    let mut socket: Option<WriteHalf<TcpStream>> = None;
    let mut join_handle: Option<tokio::task::JoinHandle<()>> = None;

    loop {
        println!("Enter command:");
        if !input(&mut buffer) {continue};
        let command = buffer.trim();

        let request = match command {
            "connect" => {
                if socket.is_some() {
                    eprintln!("Already connected");
                    continue;
                }

                println!("Enter IP address with port:");
                if !input(&mut buffer) {continue};
                let ip = buffer.trim().to_string();

                socket = Some(match TcpStream::connect(ip.trim()).await {
                    Ok(s) => {
                        let (reader, writer) = split(s);

                        join_handle = Some(task::spawn(read_from_socket(reader)));

                        writer
                    },
                    Err(e) => {
                        eprintln!("Error connecting to server: {:#?}", e);
                        continue;
                    }
                });

                println!("Connected to server.");
                continue;
            }
            "disconnect" => {
                match socket {
                    Some(mut s) => {
                        if let Err(e) = s.shutdown().await {
                            eprintln!("Error disconnecting from server: {:#?}", e);
                            eprintln!("Forcing the socket closed.");
                        }

                        if let Some(jh) = join_handle {
                            jh.abort();
                        }

                        join_handle = None;
                        socket = None;
                    },
                    None => {
                        eprintln!("Not connected");
                        continue;
                    }
                }
                println!("Disconnected from server.");
                continue;
            }
            "quit" => {
                println!("BBye!");
                break;
            }

            "buy" => {
                println!("Enter stock symbol to buy:");
                if !input(&mut buffer) {continue};
                let symbol = buffer.trim().to_string();

                println!("Enter price to buy at:");
                if !input(&mut buffer) {continue};
                let price: f64 = match buffer.trim().parse() {
                    Ok(p) => p,
                    Err(_) => {
                        eprintln!("Invalid price, you need to enter a decimal float.");
                        continue;
                    }
                };

                println!("Enter quantity to buy:");
                if !input(&mut buffer) {continue};
                let quantity: usize = match buffer.trim().parse() {
                    Ok(p) => p,
                    Err(_) => {
                        eprintln!("Invalid quantity, you need to enter a decimal integer.");
                        continue;
                    }
                };

                Request::Buy { symbol, price, quantity }
            }
            "sell" => {
                println!("Enter stock symbol to sell:");
                if !input(&mut buffer) {continue};
                let symbol = buffer.trim().to_string();

                println!("Enter price to sell at:");
                if !input(&mut buffer) {continue};
                let price: f64 = match buffer.trim().parse() {
                    Ok(p) => p,
                    Err(_) => {
                        eprintln!("Invalid price, you need to enter a decimal float.");
                        continue;
                    }
                };

                println!("Enter quantity to sell:");
                if !input(&mut buffer) {continue};
                let quantity: usize = match buffer.trim().parse() {
                    Ok(p) => p,
                    Err(_) => {
                        eprintln!("Invalid quantity, you need to enter a decimal integer.");
                        continue;
                    }
                };

                Request::Sell { symbol, price, quantity }
            }
            "ohlc" => {
                println!("Enter stock symbol to get OHLC for:");
                if !input(&mut buffer) {continue};
                let symbol = buffer.trim().to_string();

                Request::Ohlc { symbol }
            }
            "buy orders" => {
                println!("Enter stock symbol to get buy orders for:");
                if !input(&mut buffer) {continue};
                let symbol = buffer.trim().to_string();

                Request::BuyOrders { symbol }
            }
            "sell orders" => {
                println!("Enter stock symbol to get sell orders for:");
                if !input(&mut buffer) {continue};
                let symbol = buffer.trim().to_string();

                Request::SellOrders { symbol }
            }

            _ => {
                eprintln!("Unknown command: {}", command);
                continue;
            },
        };

        if let Some(ref mut socket) = socket {
            let message = serde_json::to_string(&request).expect("Request serialization should not fail.");
            if let Err(e) = socket.write(message.as_bytes()).await {
                eprintln!("Error sending message: {:#?}", e);
            }
        } else {
            eprintln!("Not connected to any server yet.");
        }
    }
}

fn input(buffer: &mut String) -> bool {
    let stdin = io::stdin();
    buffer.clear();

    if let Err(e) = stdin.read_line(buffer) {
        eprintln!("Error reading input: {:#?}", e);
        return false;
    }

    if buffer.trim() == "" {
        println!("No input, going back to main.");
        return false;
    }

    true
}

async fn read_from_socket(mut reader: ReadHalf<TcpStream>) {
    let mut buffer = [0u8; 1024];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => {
                market_speak("Server closed connection.".to_string(), false);
                break;
            },
            Ok(n) => {
                match serde_json::from_slice::<Response>(&buffer[0..n]) {
                    Ok(response) => market_speak(format!("Received: {:#?}", response), false),
                    Err(_) => market_speak(format!("Received unparseable data: {}", String::from_utf8_lossy(&buffer[0..n])), true),
                }
            },
            Err(e) => {
                market_speak(format!("Error reading from socket: {:#?}", e), true);
                continue;
            }
        }
    }
}

fn market_speak(message: String, error: bool) {
    println!("---------------------------------");
    println!("Message from the market:");
    if error {
        eprintln!("{}", message);
    } else {
        println!("{}", message);
    }
    println!("---------------------------------");
    println!("Enter command:");
}
