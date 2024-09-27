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

#[derive(Debug)]
struct RequestHeader {
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    client_id: Bytes,
}

#[derive(Copy, Clone, Debug)]
enum KafkaError {
    UnsupportedVersion = 35,
    UnknownTopic = 100,
}

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
                    client_id: {
                        let client_id_len: usize = request.get_u16().into();
                        request.copy_to_bytes(client_id_len)
                    },
                };
                request.advance(1); // TAG_BUFFER

                let mut body = Vec::new();
                body.put_i32(header.correlation_id);

                match header.api_key {
                    ApiKey::ApiVersions => {
                        let error_code = match !(0..=4).contains(&header.api_version) {
                            true => KafkaError::UnsupportedVersion as i16,
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
                        // Read request
                        let _max_wait_ms = request.get_i32();
                        let _min_bytes = request.get_i32();
                        let _max_bytes = request.get_i32();
                        let _isolation_level = request.get_i8();
                        let _session_id = request.get_i32();
                        let _session_epoch = request.get_i32();

                        let topic_count = request.get_u8().saturating_sub(1);
                        let mut topics = Vec::with_capacity(topic_count.into());

                        for _ in 0..topic_count {
                            // topic_id is actually a UUID, but we don't need
                            // to decode it, so we just treat it like a big int.
                            let topic_id = request.get_u128();

                            let partition_count = request.get_u8().saturating_sub(1);

                            // NOTE: we'll need this once we start processing the data.
                            // let mut partitions = Vec::with_capacity(partition_count);

                            for _ in 0..partition_count {
                                let _partition = request.get_i32();
                                let _current_leader_epoch = request.get_i32();
                                let _fetch_offset = request.get_i64();
                                let _last_fetched_epoch = request.get_i32();
                                let _log_start_offset = request.get_i64();
                                let _partition_max_bytes = request.get_i32();
                                request.advance(1); // TAG_BUFFER
                            }
                            request.advance(1); // TAG_BUFFER

                            topics.push(topic_id);
                        }

                        let forgotten_topics_data_count = request.get_u8().saturating_sub(1);

                        for _ in 0..forgotten_topics_data_count {
                            let _topic_id = request.get_u128();
                            let _partitions = request.get_i32();
                            request.advance(1); // TAG_BUFFER
                        }

                        let _rack_id_len = request.get_u8() - 1;
                        request.advance(1); // TAG_BUFFER

                        body.put_u8(0); // TAG_BUFFER
                        body.put_i32(420); // throttle time in ms

                        let error_code = match !(0..=16).contains(&header.api_version) {
                            true => KafkaError::UnsupportedVersion as i16,
                            false => 0_i16,
                        };
                        body.put_i16(error_code);
                        body.put_i32(0); // session_id

                        body.put_u8(topics.len() as u8 + 1); // number of responses, but the tag buffer

                        for topic_id in topics {
                            body.put_u128(topic_id);

                            // NOTE: these are dummy values for now.
                            body.put_u8(2); // partitions
                            body.put_u32(0); // index
                            body.put_u16(KafkaError::UnknownTopic as u16);
                            body.put_u64(0);
                            body.put_u64(0);
                            body.put_u64(0);
                            body.put_u8(0);
                            body.put_u32(0);
                            body.put_u8(1);

                            // Double TAG_BUFFER
                            body.put_u8(0);
                            body.put_u8(0);
                        }

                        body.put_u8(0); // TAG_BUFFER
                    }
                    ApiKey::Produce => {
                        // todo!();
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
