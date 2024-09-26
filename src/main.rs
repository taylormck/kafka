#![allow(unused_imports)]
use bytes::{Buf, BufMut};
use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut len_buf = [0_u8; 4];

                stream.read_exact(&mut len_buf).unwrap();
                let len = i32::from_be_bytes(len_buf) as usize;

                let mut request = vec![0_u8; len];
                stream.read_exact(&mut request).unwrap();
                let mut request = request.as_slice();

                let _request_api_key = request.get_i16();
                let _request_api_version = request.get_i16();
                let correlation_id = request.get_i32();

                let mut response = Vec::with_capacity(8);
                response.put_i32(0);
                response.put_i32(correlation_id);

                stream.write_all(&response).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
