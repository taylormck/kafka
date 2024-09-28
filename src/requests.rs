use crate::ApiKey;
use bytes::{Buf, BufMut, Bytes};
use num_traits::FromPrimitive;
use tokio::io::{AsyncRead, AsyncReadExt};

mod api_versions;
mod fetch;

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
        ApiKey::ApiVersions => api_versions::process(&header.api_version, &mut body_buffer),
        ApiKey::Fetch => fetch::process(&header.api_version, request_buffer, &mut body_buffer),
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
