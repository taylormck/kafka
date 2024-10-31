use crate::KafkaError;
use bytes::{Buf, BufMut};
use std::{fs::File, io::BufReader};

const METADATA_FILE_LOCATION: &str =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

pub fn process(_api_version: i16, input: &mut (impl Buf + ?Sized), output: &mut impl BufMut) {
    let num_topics = input.get_u8().saturating_sub(1);
    let mut topics = vec![];

    for _ in 0..num_topics {
        topics.push(Topic::read_from_buffer(input));
    }

    let _response_partition_limit = input.get_i32();

    // Because the cursor is maybe a NULLABLE_BYTES or COMPACT_NULLABLE_BYTES
    // or some other bullshit, we only need to read -1 as a u8.
    let cursor = input.get_u8();
    assert_eq!(cursor, 0xff);

    let metadata_log_file = File::open(METADATA_FILE_LOCATION).unwrap();
    let metadata_size = metadata_log_file.metadata().unwrap().len();
    let mut reader = BufReader::new(metadata_log_file);

    input.advance(1); // TAG_BUFFER

    output.put_u8(0); // TAG BUFFER
    output.put_i32(0); // throttle_time

    output.put_u8(topics.len() as u8 + 1);
    for topic in topics {
        output.put_i16(KafkaError::UnknownTopicOrPartition as i16); // error_code
        write_compact_string(output, &topic.name);

        output.put_u128(0); // Topic ID
        output.put_u8(0); // bool is_internal

        output.put_u8(1); // num_partitions + 1
                          // write partitions
        output.put_i32(0); // topic_authorized_operations
        output.put_u8(0); // TAG_BUFFER
    }

    // Because the cursor is maybe a NULLABLE_BYTES or COMPACT_NULLABLE_BYTES
    // or some other bullshit, we only need to write -1 as a u8.
    output.put_u8(0xff);

    output.put_u8(0); // final TAG_BUFFER
}

struct Topic {
    pub name: String,
}

impl Topic {
    pub fn read_from_buffer(input: &mut (impl Buf + ?Sized)) -> Self {
        let name = read_compact_string(input);
        input.advance(1); // TAG_BUFFER

        Self { name }
    }
}

// struct Cursor {
//     _topic_name: String,
//     _partition_index: i32,
// }
//
// impl Cursor {
//     pub fn read_from_buffer(input: &mut (impl Buf + ?Sized)) -> Self {
//         let topic_name = read_compact_string(input);
//         let partition_index = input.get_i32();
//         input.advance(1); // TAG_BUFFER
//
//         Self {
//             _topic_name: topic_name,
//             _partition_index: partition_index,
//         }
//     }
// }

fn read_compact_string(input: &mut (impl Buf + ?Sized)) -> String {
    let string_length = input.get_u8().saturating_sub(1);
    let mut buffer = vec![];

    for _ in 0..string_length {
        buffer.push(input.get_u8());
    }
    String::from_utf8_lossy(&buffer).to_string()
}

fn write_compact_string(output: &mut impl BufMut, content: &str) {
    let topic_name_bytes: Vec<u8> = content.bytes().collect();
    output.put_u8(topic_name_bytes.len() as u8 + 1);
    output.put(&topic_name_bytes[..]);
}
