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

use kafka_starter_rust::stream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:9092").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            stream::process_stream(&mut stream).await;
        });
    }
}
