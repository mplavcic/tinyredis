use crate::protocol::{RespValue, RespParseError};

#[derive(Debug)]
pub enum Command {
    Ping(Option<String>),
    Echo(String),
    Get(String),
    Set {
        key: String,
        value: String,
        px: Option<u64>,
    },
    Unknown(String),
}

pub fn parse_command(value: RespValue) -> Result<Command, RespParseError> {
    match value {
        RespValue::Array(items) => {
            let cmd = match items.get(0) {
                Some(RespValue::BulkString(Some(s))) => s.to_uppercase(),
                _ => return Err(RespParseError::InvalidFormat),
            };

            match cmd.as_str() {
                "PING" => {
                    let msg = items.get(1).and_then(|v| match v {
                        RespValue::BulkString(Some(s)) => Some(s.clone()),
                        _ => None,
                    });
                    Ok(Command::Ping(msg))
                }
                "ECHO" => {
                    if let Some(RespValue::BulkString(Some(msg))) = items.get(1) {
                        Ok(Command::Echo(msg.clone()))
                    } else {
                        Err(RespParseError::InvalidFormat)
                    }
                }
                "GET" => {
                    if let Some(RespValue::BulkString(Some(key))) = items.get(1) {
                        Ok(Command::Get(key.clone()))
                    } else {
                        Err(RespParseError::InvalidFormat)
                    }
                }
                "SET" => {
                    let key = match items.get(1) {
                        Some(RespValue::BulkString(Some(k))) => k.clone(),
                        _ => return Err(RespParseError::InvalidFormat),
                    };
                    let value = match items.get(2) {
                        Some(RespValue::BulkString(Some(v))) => v.clone(),
                        _ => return Err(RespParseError::InvalidFormat),
                    };

                    let mut px = None;
                    if items.len() >= 5 {
                        if let (
                            Some(RespValue::BulkString(Some(opt))),
                            Some(RespValue::BulkString(Some(ms))),
                        ) = (items.get(3), items.get(4))
                        {
                            if opt.to_uppercase() == "PX" {
                                px = ms.parse::<u64>().ok();
                            }
                        }
                    }

                    Ok(Command::Set { key, value, px })
                }
                other => Ok(Command::Unknown(other.to_string())),
            }
        }
        _ => Err(RespParseError::InvalidFormat),
    }
}
