#![allow(unused_imports)]
use bytes::{Buf, BufMut};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::{
    io::{Read, Write},
    ops::RangeBounds,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Copy, Clone, Debug)]
struct RequestHeader {
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
}

#[repr(i16)]
enum ErrorCode {
    UnsupportedVersion = 35,
}

#[repr(i16)]
#[derive(Copy, Clone, Debug, FromPrimitive)]
enum ApiKey {
    ApiVersions = 18,
    Fetch = 1,
    Produce = 0,
    None = -1,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:9092").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut len_buf = [0_u8; 4];

            loop {
                if let Err(err) = stream.read_exact(&mut len_buf).await {
                    eprintln!("Failed to read from socket; err = {:?}", err);
                    return;
                }

                let len = i32::from_be_bytes(len_buf) as usize;
                let mut request = vec![0_u8; len];

                if let Err(err) = stream.read_exact(&mut request).await {
                    eprintln!("Failed to read from socket; err = {:?}", err);
                    return;
                }

                let mut request = request.as_slice();

                let header = RequestHeader {
                    api_key: match FromPrimitive::from_i16(request.get_i16()) {
                        Some(api_key) => api_key,
                        _ => ApiKey::None,
                    },
                    api_version: request.get_i16(),
                    correlation_id: request.get_i32(),
                };

                let mut body = Vec::new();
                body.put_i32(header.correlation_id);

                match header.api_key {
                    ApiKey::ApiVersions => {
                        let error_code = match !(0..=4).contains(&header.api_version) {
                            true => ErrorCode::UnsupportedVersion as i16,
                            false => 0_i16,
                        };
                        body.put_i16(error_code);

                        body.put_i8(3); // num api key records + 1

                        // APIVersions
                        body.put_i16(18); //  ApiVersions api key
                        body.put_i16(0); // Minimum supported API version
                        body.put_i16(4); // Max supported API version
                        body.put_i8(0); // TAG_BUFFER length

                        // FETCH
                        body.put_i16(1); // Fetch api key
                        body.put_i16(0); // Minimum supported fetch version
                        body.put_i16(16); // Max supported fetch version
                        body.put_i8(0); // TAG_BUFFER length

                        body.put_i32(420); // throttle time in ms
                        body.put_i8(0); // TAG_BUFFER length
                    }
                    ApiKey::Fetch => {
                        body.put_u8(0); // TAG_BUFFER
                        body.put_i32(420); // throttle time in ms

                        let error_code = match !(0..=16).contains(&header.api_version) {
                            true => ErrorCode::UnsupportedVersion as i16,
                            false => 0_i16,
                        };
                        body.put_i16(error_code);
                        body.put_i32(0); // session_id

                        body.put_u8(1); // n + 1 bytes follow

                        body.put_u8(0); // TAG_BUFFER
                    }
                    ApiKey::Produce => {
                        todo!();
                    }
                    ApiKey::None => {}
                }

                let mut response = Vec::new();
                response.put_i32(body.len().try_into().unwrap());
                response.put(&body[..]);

                if let Err(err) = stream.write_all(&response).await {
                    eprintln!("Failed to write to socket; err = {:?}", err);
                    return;
                }
            }
        });
    }
}
