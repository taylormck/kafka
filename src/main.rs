#![allow(unused_imports)]
use std::{io::Write, net::TcpListener};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

                let buf: [u8; 8] = [0, 0, 0, 8, 0, 0, 0, 7];
                stream.write_all(&buf).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
