mod protocol;
mod command;

use protocol::parse;
use command::parse_command;
use command::Command;

use tokio::net::TcpListener;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
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

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Arc::new(Mutex::new(HashMap::<String, ValueEntry>::new()));

    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let db = db.clone();

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

                                    match parse_command(value) {
                                        Ok(cmd) => {
                                            handle_command(cmd, &db, &mut stream).await;
                                        }
                                        Err(_) => {
                                            let _ = stream.write_all(b"-ERR invalid command\r\n").await;
                                        }
                                    }
                                }
                                Err(protocol::RespParseError::Incomplete) => break,
                                Err(_) => {
                                    let _ = stream.write_all(b"-ERR parse error\r\n").await;
                                    break;
                                }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }
}

async fn handle_command(
    cmd: Command,
    db: &Arc<Mutex<HashMap<String, ValueEntry>>>,
    stream: &mut tokio::net::TcpStream,
) {
    match cmd {
        Command::Ping(Some(msg)) => {
            let _ = stream.write_all(format!("+{}\r\n", msg).as_bytes()).await;
        }
        Command::Ping(None) => {
            let _ = stream.write_all(b"+PONG\r\n").await;
        }
        Command::Echo(msg) => {
            let _ = stream.write_all(format!("${}\r\n{}\r\n", msg.len(), msg).as_bytes()).await;
        }
        Command::Set { key, value, px } => {
            let expires_at = px.map(|ms| Instant::now() + Duration::from_millis(ms));
            let mut db = db.lock().await;
            db.insert(key, ValueEntry { value, expires_at });
            let _ = stream.write_all(b"+OK\r\n").await;
        }
        Command::Get(key) => {
            let mut db = db.lock().await;
            if let Some(entry) = db.get(&key) {
                if let Some(expiry) = entry.expires_at {
                    if Instant::now() > expiry {
                        db.remove(&key);
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
        Command::Unknown(cmd) => {
            let msg = format!("-ERR unknown command '{}'\r\n", cmd);
            let _ = stream.write_all(msg.as_bytes()).await;
        }
    }
}

