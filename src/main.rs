mod protocol;

use protocol::parse;
use protocol::RespParseError;
use protocol::RespValue;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let stream = listener.accept().await;
        let db = db.clone();

        match stream {
            Ok((mut stream, _)) => {
                println!("Accepted new connection");

                tokio::spawn(async move {
                    let mut buffer = Vec::new();
                    let mut read_buf = [0u8; 512];

                    loop {
                        match stream.read(&mut read_buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                buffer.extend_from_slice(&read_buf[..n]);

                                loop {
                                    let input = match std::str::from_utf8(&buffer) {
                                        Ok(s) => s,
                                        Err(_) => break,
                                    };

                                    match parse(input) {
                                        Ok((value, remaining)) => {
                                            buffer = remaining.as_bytes().to_vec();
                                            handle_command(value, &mut stream, &db).await;
                                        }
                                        Err(RespParseError::Incomplete) => break,
                                        Err(_) => {
                                            eprintln!("-ERR invalid input\r\n");
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Read error: {}", e);
                                break;
                            }
                        }
                    }
                });
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
            }
        }
    }
}

async fn handle_command(value: RespValue, stream: &mut tokio::net::TcpStream, db: &Arc<Mutex<HashMap<String, String>>>) {
    match value {
        RespValue::Array(elements) => {
            if let Some(RespValue::BulkString(Some(cmd))) = elements.get(0) {
                match cmd.to_uppercase().as_str() {
                    "PING" => {
                        let response =
                            if let Some(RespValue::BulkString(Some(message))) = elements.get(1) {
                                format!("+{}\r\n", message)
                            } else {
                                "+PONG\r\n".to_string()
                            };
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                    "ECHO" => {
                        if let Some(RespValue::BulkString(Some(message))) = elements.get(1) {
                            let response = format!("${}\r\n{}\r\n", message.len(), message);
                            let _ = stream.write_all(response.as_bytes()).await;
                        } else {
                            let _ = stream
                                .write_all(b"-ERR wrong number of arguments for 'echo'\r\n")
                                .await;
                        }
                    }
                    "SET" => {
                        if elements.len() != 3 {
                            let _ = stream.write_all(b"-ERR wrong number of arguments for 'set'\r\n").await;
                            return;
                        }

                        if let (Some(RespValue::BulkString(Some(key))), Some(RespValue::BulkString(Some(val)))) =
                            (elements.get(1), elements.get(2))
                        {
                            let mut db = db.lock().await;
                            db.insert(key.clone(), val.clone());
                            let _ = stream.write_all(b"+OK\r\n").await;
                        } else {
                            let _ = stream.write_all(b"-ERR invalid arguments for 'set'\r\n").await;
                        }
                    }

                    "GET" => {
                        if elements.len() != 2 {
                            let _ = stream.write_all(b"-ERR wrong number of arguments for 'get'\r\n").await;
                            return;
                        }

                        if let Some(RespValue::BulkString(Some(key))) = elements.get(1) {
                            let db = db.lock().await;
                            if let Some(val) = db.get(key) {
                                let response = format!("${}\r\n{}\r\n", val.len(), val);
                                let _ = stream.write_all(response.as_bytes()).await;
                            } else {
                                let _ = stream.write_all(b"$-1\r\n").await; // RESP nil
                            }
                        } else {
                            let _ = stream.write_all(b"-ERR invalid argument for 'get'\r\n").await;
                        }
                    }

                    _ => {
                        let _ = stream.write_all(b"-ERR unknown command\r\n").await;
                    }
                }
            } else {
                let _ = stream.write_all(b"-ERR malformed command\r\n").await;
            }
        }
        _ => {
            let _ = stream.write_all(b"-ERR protocol error\r\n").await;
        }
    }
}
