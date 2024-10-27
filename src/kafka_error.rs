#[derive(Copy, Clone, Debug)]
pub enum KafkaError {
    UnsupportedVersion = 35,
    UnknownTopic = 100,
    UnknownTopicOrPartition = 3,
}
