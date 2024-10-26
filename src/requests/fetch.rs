use crate::KafkaError;
use bytes::{Buf, BufMut};

pub fn process(api_version: &i16, input: &mut (impl Buf + ?Sized), output: &mut impl BufMut) {
    let _max_wait_ms = input.get_i32();
    let _min_bytes = input.get_i32();
    let _max_bytes = input.get_i32();
    let _isolation_level = input.get_i8();
    let _session_id = input.get_i32();
    let _session_epoch = input.get_i32();

    let topic_count = input.get_u8().saturating_sub(1);
    let mut topics = vec![];

    for _ in 0..topic_count {
        let topic = Topic::from_bytes(input);
        topics.push(topic);
    }

    let forgotten_topics_data_count = input.get_u8().saturating_sub(1);

    for _ in 0..forgotten_topics_data_count {
        let _topic_id = input.get_u128();
        let _partitions = input.get_i32();
        input.advance(1); // TAG_BUFFER
    }

    let _rack_id_len = input.get_u8().saturating_sub(1);
    input.advance(1); // TAG_BUFFER

    output.put_u8(0); // TAG_BUFFER
    output.put_i32(0); // throttle time in ms

    let error_code = match !(0..=16).contains(api_version) {
        true => KafkaError::UnsupportedVersion as i16,
        false => 0_i16,
    };

    output.put_i16(error_code);
    output.put_i32(0); // session_id

    output.put_u8(topics.len() as u8 + 1);

    for topic in topics {
        topic.write_bytes(output);
    }

    output.put_u8(0); // TAG_BUFFER
}

#[derive(Clone, Debug)]
struct Topic {
    id: u128,
    partitions: Vec<Partition>,
}

impl Topic {
    fn from_bytes(input: &mut (impl Buf + ?Sized)) -> Topic {
        // topic_id is actually a UUID, but we don't need
        // to decode it, so we just treat it like a big int.
        let id = input.get_u128();

        let partition_count = input.get_u8().saturating_sub(1);
        let mut partitions = vec![];

        for _ in 0..partition_count {
            partitions.push(Partition::from_bytes(input));
        }

        input.advance(1); // TAG_BUFFER

        Self { id, partitions }
    }

    fn write_bytes(&self, output: &mut impl BufMut) {
        output.put_u128(self.id);
        output.put_u8(self.partitions.len() as u8 + 1);

        for partition in self.partitions.iter() {
            partition.write_bytes(output);
        }

        output.put_u8(0); // TAG_BUFFER
    }
}

#[derive(Clone, Debug)]
struct Partition {
    partition: i32,
    _current_leader_epoch: i32,
    _fetch_offset: i64,
    _last_fetched_epoch: i32,
    _log_start_offset: i64,
    _partition_max_bytes: i32,
}

impl Partition {
    fn from_bytes(input: &mut (impl Buf + ?Sized)) -> Partition {
        let partition = Self {
            partition: input.get_i32(),
            _current_leader_epoch: input.get_i32(),
            _fetch_offset: input.get_i64(),
            _last_fetched_epoch: input.get_i32(),
            _log_start_offset: input.get_i64(),
            _partition_max_bytes: input.get_i32(),
        };

        input.advance(1); // TAG_BUFFER

        partition
    }

    fn write_bytes(&self, output: &mut impl BufMut) {
        output.put_i32(self.partition); // index
        output.put_i16(0); // error_code
        output.put_i64(0); // high_watermark
        output.put_i64(0); // last_stable_offset
        output.put_i64(0); // log_start_offset
        output.put_u8(1); // aborted_transactions count + 1
                          // aborted_transactions
        output.put_i32(0); // preferred_read_replica
        output.put_u8(0); // records size
        output.put_u8(0); // TAG_BUFFER
    }
}
