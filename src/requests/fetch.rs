use crate::KafkaError;
use bytes::{Buf, BufMut};

pub fn process(api_version: &i16, input: &mut dyn Buf, output: &mut dyn BufMut) {
    let _max_wait_ms = input.get_i32();
    let _min_bytes = input.get_i32();
    let _max_bytes = input.get_i32();
    let _isolation_level = input.get_i8();
    let _session_id = input.get_i32();
    let _session_epoch = input.get_i32();

    let topic_count = input.get_u8().saturating_sub(1);
    let mut topics = Vec::with_capacity(topic_count.into());

    for _ in 0..topic_count {
        // topic_id is actually a UUID, but we don't need
        // to decode it, so we just treat it like a big int.
        let topic_id = input.get_u128();

        let partition_count = input.get_u8().saturating_sub(1);

        // NOTE: we'll need this once we start processing the data.
        // let mut partitions = Vec::with_capacity(partition_count);

        for _ in 0..partition_count {
            let _partition = input.get_i32();
            let _current_leader_epoch = input.get_i32();
            let _fetch_offset = input.get_i64();
            let _last_fetched_epoch = input.get_i32();
            let _log_start_offset = input.get_i64();
            let _partition_max_bytes = input.get_i32();
            input.advance(1); // TAG_BUFFER
        }
        input.advance(1); // TAG_BUFFER

        topics.push(topic_id);
    }

    let forgotten_topics_data_count = input.get_u8().saturating_sub(1);

    for _ in 0..forgotten_topics_data_count {
        let _topic_id = input.get_u128();
        let _partitions = input.get_i32();
        input.advance(1); // TAG_BUFFER
    }

    let _rack_id_len = input.get_u8() - 1;
    input.advance(1); // TAG_BUFFER

    output.put_u8(0); // TAG_BUFFER
    output.put_i32(420); // throttle time in ms

    let error_code = match !(0..=16).contains(api_version) {
        true => KafkaError::UnsupportedVersion as i16,
        false => 0_i16,
    };
    output.put_i16(error_code);
    output.put_i32(0); // session_id

    output.put_u8(topics.len() as u8 + 1); // number of responses, but the tag buffer

    for topic_id in topics {
        output.put_u128(topic_id);

        // NOTE: these are dummy values for now.
        output.put_u8(2); // partitions
        output.put_u32(0); // index
        output.put_u16(KafkaError::UnknownTopic as u16);
        output.put_u64(0);
        output.put_u64(0);
        output.put_u64(0);
        output.put_u8(0);
        output.put_u32(0);
        output.put_u8(1);

        // Double TAG_BUFFER
        output.put_u8(0);
        output.put_u8(0);
    }

    output.put_u8(0); // TAG_BUFFER
}
