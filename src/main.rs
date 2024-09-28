#![allow(unused_imports)]
use bytes::{Buf, BufMut, Bytes};
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

use kafka_starter_rust::{requests, ApiKey, KafkaError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:9092").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut len_buf = [0_u8; 4];

            loop {
                let request_length =
                    match requests::read_request_length(&mut len_buf, &mut stream).await {
                        Ok(len) => len,
                        Err(err) => {
                            eprintln!("Failed to read from socket; err = {:?}", err);
                            return;
                        }
                    };

                let mut request_buffer = vec![0_u8; request_length];

                if let Err(err) = stream.read_exact(&mut request_buffer).await {
                    eprintln!("Failed to read from socket; err = {:?}", err);
                    return;
                }

                let mut request_buffer = request_buffer.as_slice();

                let header = requests::read_request_header(&mut request_buffer);
                let response = match requests::process_request(&header, &mut request_buffer) {
                    Ok(res) => res,
                    Err(err) => {
                        eprintln!("Failed to write to socket; err = {:?}", err);
                        return;
                    }
                };

                if let Err(err) = stream.write_all(&response).await {
                    eprintln!("Failed to write to socket; err = {:?}", err);
                    return;
                }
            }
        });
    }
}
