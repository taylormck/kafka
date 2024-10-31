use bytes::Buf;

pub fn get_topic_metadata(mut metadata_input: impl Buf, _topic_name: &str) {
    // NOTE: We assume metadata_input is a proper metadata log file's data
    let base_offset = metadata_input.get_i64();
    let batch_length = metadata_input.get_i32();
}
