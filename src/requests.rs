use bytes::{Buf, BufMut, Bytes};
use num_traits::FromPrimitive;

use tokio::io::{AsyncRead, AsyncReadExt /*, AsyncWrite, AsyncWriteExt*/};

use crate::{ApiKey, KafkaError};

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Bytes,
}

pub async fn read_request_length(
    length_buffer: &mut [u8; 4],
    stream: &mut (impl AsyncRead + std::marker::Unpin),
) -> Result<usize, Box<dyn std::error::Error>> {
    if let Err(err) = stream.read_exact(length_buffer).await {
        return Err(Box::new(err));
    }

    Ok(i32::from_be_bytes(*length_buffer) as usize)
}

pub fn read_request_header(buffer: &mut dyn Buf) -> RequestHeader {
    let header = RequestHeader {
        api_key: match FromPrimitive::from_i16(buffer.get_i16()) {
            Some(api_key) => api_key,
            _ => ApiKey::None,
        },
        api_version: buffer.get_i16(),
        correlation_id: buffer.get_i32(),
        client_id: {
            let client_id_len: usize = buffer.get_u16().into();
            buffer.copy_to_bytes(client_id_len)
        },
    };
    buffer.advance(1); // TAG_BUFFER

    header
}

pub fn process_request(
    header: &RequestHeader,
    request_buffer: &mut dyn Buf,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut body_buffer = Vec::new();
    body_buffer.put_i32(header.correlation_id);

    match header.api_key {
        ApiKey::ApiVersions => {
            let error_code = match !(0..=4).contains(&header.api_version) {
                true => KafkaError::UnsupportedVersion as i16,
                false => 0_i16,
            };
            body_buffer.put_i16(error_code);

            body_buffer.put_i8(3); // num api key records + 1

            // APIVersions
            body_buffer.put_i16(18); //  ApiVersions api key
            body_buffer.put_i16(0); // Minimum supported API version
            body_buffer.put_i16(4); // Max supported API version
            body_buffer.put_i8(0); // TAG_BUFFER length

            // FETCH
            body_buffer.put_i16(1); // Fetch api key
            body_buffer.put_i16(0); // Minimum supported fetch version
            body_buffer.put_i16(16); // Max supported fetch version
            body_buffer.put_i8(0); // TAG_BUFFER length

            body_buffer.put_i32(420); // throttle time in ms
            body_buffer.put_i8(0); // TAG_BUFFER length
        }
        ApiKey::Fetch => {
            // Read request
            //
            let _max_wait_ms = request_buffer.get_i32();
            let _min_bytes = request_buffer.get_i32();
            let _max_bytes = request_buffer.get_i32();
            let _isolation_level = request_buffer.get_i8();
            let _session_id = request_buffer.get_i32();
            let _session_epoch = request_buffer.get_i32();

            let topic_count = request_buffer.get_u8().saturating_sub(1);
            let mut topics = Vec::with_capacity(topic_count.into());

            for _ in 0..topic_count {
                // topic_id is actually a UUID, but we don't need
                // to decode it, so we just treat it like a big int.
                let topic_id = request_buffer.get_u128();

                let partition_count = request_buffer.get_u8().saturating_sub(1);

                // NOTE: we'll need this once we start processing the data.
                // let mut partitions = Vec::with_capacity(partition_count);

                for _ in 0..partition_count {
                    let _partition = request_buffer.get_i32();
                    let _current_leader_epoch = request_buffer.get_i32();
                    let _fetch_offset = request_buffer.get_i64();
                    let _last_fetched_epoch = request_buffer.get_i32();
                    let _log_start_offset = request_buffer.get_i64();
                    let _partition_max_bytes = request_buffer.get_i32();
                    request_buffer.advance(1); // TAG_BUFFER
                }
                request_buffer.advance(1); // TAG_BUFFER

                topics.push(topic_id);
            }

            let forgotten_topics_data_count = request_buffer.get_u8().saturating_sub(1);

            for _ in 0..forgotten_topics_data_count {
                let _topic_id = request_buffer.get_u128();
                let _partitions = request_buffer.get_i32();
                request_buffer.advance(1); // TAG_BUFFER
            }

            let _rack_id_len = request_buffer.get_u8() - 1;
            request_buffer.advance(1); // TAG_BUFFER

            body_buffer.put_u8(0); // TAG_BUFFER
            body_buffer.put_i32(420); // throttle time in ms

            let error_code = match !(0..=16).contains(&header.api_version) {
                true => KafkaError::UnsupportedVersion as i16,
                false => 0_i16,
            };
            body_buffer.put_i16(error_code);
            body_buffer.put_i32(0); // session_id

            body_buffer.put_u8(topics.len() as u8 + 1); // number of responses, but the tag buffer

            for topic_id in topics {
                body_buffer.put_u128(topic_id);

                // NOTE: these are dummy values for now.
                body_buffer.put_u8(2); // partitions
                body_buffer.put_u32(0); // index
                body_buffer.put_u16(KafkaError::UnknownTopic as u16);
                body_buffer.put_u64(0);
                body_buffer.put_u64(0);
                body_buffer.put_u64(0);
                body_buffer.put_u8(0);
                body_buffer.put_u32(0);
                body_buffer.put_u8(1);

                // Double TAG_BUFFER
                body_buffer.put_u8(0);
                body_buffer.put_u8(0);
            }

            body_buffer.put_u8(0); // TAG_BUFFER
        }
        ApiKey::Produce => {
            // todo!();
        }
        ApiKey::None => {}
    }

    let mut response = Vec::new();
    response.put_i32(body_buffer.len().try_into().unwrap());
    response.put(&body_buffer[..]);
    Ok(response)
}
