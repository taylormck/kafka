#![allow(unused_imports)]
use bytes::{Buf, BufMut};
use std::{
    io::{Read, Write},
    net::TcpListener,
    ops::RangeBounds,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::ToSocketAddrs,
};

#[derive(Copy, Clone, Debug)]
struct RequestHeader {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
}

#[repr(i16)]
enum ErrorCode {
    UnsupportedVersion = 35,
}

#[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        tokio::spawn(async move {
            match stream {
                Ok(mut stream) => {
                    while stream.peek(&mut [0; 1]).is_ok() {
                        let mut len_buf = [0_u8; 4];

                        stream.read_exact(&mut len_buf).unwrap();
                        let len = i32::from_be_bytes(len_buf) as usize;

                        let mut request = vec![0_u8; len];
                        stream.read_exact(&mut request).unwrap();
                        let mut request = request.as_slice();

                        let header = RequestHeader {
                            api_key: request.get_i16(),
                            api_version: request.get_i16(),
                            correlation_id: request.get_i32(),
                        };

                        let mut body = Vec::new();

                        match header.api_key {
                            18 => {
                                body.put_i32(header.correlation_id);

                                let error_code = match !(0..=4).contains(&header.api_version) {
                                    true => ErrorCode::UnsupportedVersion as i16,
                                    false => 0_i16,
                                };
                                body.put_i16(error_code);

                                body.put_i8(2); // num api key records + 1
                                body.put_i16(18); // match the api key
                                body.put_i16(0); // Minimum supported API version
                                body.put_i16(4); // Max supported API version
                                body.put_i8(0); // TAG_BUFFER length
                                body.put_i32(420); // throttle time in ms
                                body.put_i8(0); // TAG_BUFFER length
                            }
                            // NOTE: This is just to make clippy stop getting mad
                            17 => {}
                            _ => {}
                        }

                        let mut response = Vec::new();
                        response.put_i32(body.len().try_into().unwrap());
                        response.put(&body[..]);

                        stream.write_all(&response).unwrap();
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        });
    }
}
