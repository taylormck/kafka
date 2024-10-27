use crate::KafkaError;
use bytes::BufMut;

pub fn process(api_version: i16, output: &mut dyn BufMut) {
    let error_code = match !(0..=4).contains(&api_version) {
        true => KafkaError::UnsupportedVersion as i16,
        false => 0_i16,
    };

    output.put_i16(error_code);

    output.put_i8(4); // num api key records + 1

    // APIVersions
    output.put_i16(18); //  ApiVersions api key
    output.put_i16(0); // Minimum supported API version
    output.put_i16(4); // Max supported API version
    output.put_i8(0); // TAG_BUFFER length

    // FETCH
    output.put_i16(1); // Fetch api key
    output.put_i16(0); // Minimum supported fetch version
    output.put_i16(16); // Max supported fetch version
    output.put_i8(0); // TAG_BUFFER length

    // DescribeTopicPartitions
    output.put_i16(75); // DescribeTopicPartitions api key
    output.put_i16(0); // min supported version
    output.put_i16(0); // max supported version
    output.put_i8(0); // TAG_BUFFER length

    output.put_i32(420); // throttle time in ms
    output.put_i8(0); // TAG_BUFFER length
}
