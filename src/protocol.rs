/// To communicate with the Redis server, Redis clients use a protocol called Redis Serialization Protocol (RESP)
/// RESP can serialize different data types including integers, strings, and arrays.
/// It also features an error-specific type. A client sends a request to the Redis server as an array of strings.
/// The array's contents are the command and its arguments that the server should execute.
/// The server's reply type is command-specific.
/// RESP is binary-safe and uses prefixed length to transfer bulk data so it does not require processing bulk data
/// transferred from one process to another.
use std::str::FromStr;

#[derive(Debug, PartialEq)]
pub enum RespValue {
    /// Simple strings are encoded as a plus (+) character, followed by a string.
    /// The string mustn't contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
    SimpleString(String),
    /// RESP has specific data types for errors. Simple errors, or simply just errors, are similar to simple strings,
    /// but their first character is the minus (-) character. The difference between simple strings and errors in RESP
    /// is that clients should treat errors as exceptions, whereas the string encoded in the error type is the error
    /// message itself. The first upper-case word after the -, up to the first space or newline, represents the kind
    /// of error returned. For example:
    ///     -ERR unknown command 'asdf'
    ///     -WRONGTYPE Operation against a key holding the wrong kind of value
    Error(String),
    /// This type is a CRLF-terminated string that represents a signed, base-10, 64-bit integer.
    /// RESP encodes integers in the following way:
    /// :[<+|->]<value>\r\n
    /// The colon (:) as the first byte.
    /// An optional plus (+) or minus (-) as the sign.
    /// One or more decimal digits (0..9) as the integer's unsigned, base-10 value.
    /// The CRLF terminator.
    Integer(i64),
    /// A bulk string represents a single binary string. RESP encodes bulk strings in the following way:
    /// $<length>\r\n<data>\r\n
    /// The dollar sign ($) as the first byte.
    /// One or more decimal digits (0..9) as the string's length, in bytes, as an unsigned, base-10 value.
    /// The CRLF terminator.
    /// The data.
    /// A final CRLF.
    /// So the string "hello" is encoded as follows:
    ///     $5\r\nhello\r\n
    BulkString(Option<String>),
    /// Clients send commands to the Redis server as RESP arrays. RESP Arrays' encoding uses the following format:
    ///     *<number-of-elements>\r\n<element-1>...<element-n>
    /// An asterisk (*) as the first byte.
    /// One or more decimal digits (0..9) as the number of elements in the array as an unsigned, base-10 value.
    /// The CRLF terminator.
    /// An additional RESP type for every element of the array.
    /// The encoding of an array consisting of the two bulk strings "hello" and "world" is:
    ///     *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
    Array(Vec<RespValue>),
}

#[derive(Debug)]
pub enum RespParseError {
    Incomplete,
    InvalidFormat,
}

pub fn parse(input: &str) -> Result<(RespValue, &str), RespParseError> {
    let bytes = input.as_bytes();

    match bytes.first() {
        Some(b'+') => parse_simple_string(&input[1..]),
        Some(b'-') => parse_error(&input[1..]),
        Some(b':') => parse_integer(&input[1..]),
        Some(b'$') => parse_bulk_string(&input[1..]),
        Some(b'*') => parse_array(&input[1..]),
        _ => Err(RespParseError::InvalidFormat),
    }
}

fn parse_simple_string(input: &str) -> Result<(RespValue, &str), RespParseError> {
    if let Some(pos) = input.find("\r\n") {
        let val = &input[..pos];
        let rest = &input[(pos + 2)..];
        Ok((RespValue::SimpleString(val.to_string()), rest))
    } else {
        Err(RespParseError::Incomplete)
    }
}

fn parse_error(input: &str) -> Result<(RespValue, &str), RespParseError> {
    if let Some(pos) = input.find("\r\n") {
        let val = &input[..pos];
        let rest = &input[(pos + 2)..];
        Ok((RespValue::Error(val.to_string()), rest))
    } else {
        Err(RespParseError::Incomplete)
    }
}

fn parse_integer(input: &str) -> Result<(RespValue, &str), RespParseError> {
    if let Some(pos) = input.find("\r\n") {
        let num = i64::from_str(&input[..pos]).map_err(|_| RespParseError::InvalidFormat)?;
        let rest = &input[(pos + 2)..];
        Ok((RespValue::Integer(num), rest))
    } else {
        Err(RespParseError::Incomplete)
    }
}

fn parse_bulk_string(input: &str) -> Result<(RespValue, &str), RespParseError> {
    if let Some(pos) = input.find("\r\n") {
        let len: isize = input[..pos]
            .parse()
            .map_err(|_| RespParseError::InvalidFormat)?;
        let rest = &input[(pos + 2)..];

        if len == -1 {
            return Ok((RespValue::BulkString(None), rest));
        }

        let len = len as usize;
        if rest.len() < len + 2 {
            return Err(RespParseError::Incomplete);
        }

        let val = &rest[..len];
        if &rest[len..len + 2] != "\r\n" {
            return Err(RespParseError::InvalidFormat);
        }

        Ok((
            RespValue::BulkString(Some(val.to_string())),
            &rest[(len + 2)..],
        ))
    } else {
        Err(RespParseError::Incomplete)
    }
}

fn parse_array(input: &str) -> Result<(RespValue, &str), RespParseError> {
    if let Some(pos) = input.find("\r\n") {
        let len: isize = input[..pos]
            .parse()
            .map_err(|_| RespParseError::InvalidFormat)?;
        let mut rest = &input[(pos + 2)..];

        if len == -1 {
            return Ok((RespValue::Array(vec![]), rest));
        }

        let mut items = Vec::with_capacity(len as usize);

        for _ in 0..len {
            let (val, new_rest) = parse(rest)?;
            items.push(val);
            rest = new_rest;
        }

        Ok((RespValue::Array(items), rest))
    } else {
        Err(RespParseError::Incomplete)
    }
}
