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
use std::time::Duration;
use std::time::Instant;

#[derive(Clone)]
struct ValueEntry {
    value: String,
    expires_at: Option<Instant>,
}

type Db = Arc<Mutex<HashMap<String, ValueEntry>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db: Db = Arc::new(Mutex::new(HashMap::new()));

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

async fn handle_command(value: RespValue, stream: &mut tokio::net::TcpStream, db: &Db) {
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
                        if elements.len() < 3 {
                            let _ = stream
                                .write_all(b"-ERR wrong number of arguments for 'set'\r\n")
                                .await;
                            return;
                        }

                        let key = match elements.get(1) {
                            Some(RespValue::BulkString(Some(k))) => k.clone(),
                            _ => {
                                let _ = stream.write_all(b"-ERR invalid key\r\n").await;
                                return;
                            }
                        };

                        let val = match elements.get(2) {
                            Some(RespValue::BulkString(Some(v))) => v.clone(),
                            _ => {
                                let _ = stream.write_all(b"-ERR invalid value\r\n").await;
                                return;
                            }
                        };

                        let mut expires_at = None;
                        if elements.len() >= 5 {
                            if let (
                                Some(RespValue::BulkString(Some(opt))),
                                Some(RespValue::BulkString(Some(seconds_str))),
                            ) = (elements.get(3), elements.get(4))
                            {
                                if opt.to_uppercase() == "PX" {
                                    if let Ok(miliseconds) = seconds_str.parse::<u64>() {
                                        expires_at = Some(
                                            Instant::now() + Duration::from_millis(miliseconds),
                                        );
                                    } else {
                                        let _ =
                                            stream.write_all(b"-ERR invalid expire time\r\n").await;
                                        return;
                                    }
                                }
                            }
                        }

                        let mut db = db.lock().await;
                        db.insert(
                            key,
                            ValueEntry {
                                value: val,
                                expires_at,
                            },
                        );

                        let _ = stream.write_all(b"+OK\r\n").await;
                    }
                    "GET" => {
                        if elements.len() != 2 {
                            let _ = stream
                                .write_all(b"-ERR wrong number of arguments for 'get'\r\n")
                                .await;
                            return;
                        }

                        let key = match elements.get(1) {
                            Some(RespValue::BulkString(Some(k))) => k,
                            _ => {
                                let _ = stream.write_all(b"-ERR invalid key\r\n").await;
                                return;
                            }
                        };

                        let mut db = db.lock().await;

                        if let Some(entry) = db.get(key) {
                            if let Some(expiry) = entry.expires_at {
                                if Instant::now() > expiry {
                                    db.remove(key);
                                    let _ = stream.write_all(b"$-1\r\n").await;
                                    return;
                                }
                            }

                            let response = format!("${}\r\n{}\r\n", entry.value.len(), entry.value);
                            let _ = stream.write_all(response.as_bytes()).await;
                        } else {
                            let _ = stream.write_all(b"$-1\r\n").await;
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
