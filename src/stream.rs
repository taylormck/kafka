use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::requests;

pub async fn process_stream(stream: &mut (impl AsyncRead + AsyncWrite + std::marker::Unpin)) {
    let mut len_buf = [0_u8; 4];

    loop {
        let request_length = match requests::read_request_length(&mut len_buf, stream).await {
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
}
