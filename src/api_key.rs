use num_derive::{FromPrimitive, ToPrimitive};

#[derive(Copy, Clone, Debug, FromPrimitive, ToPrimitive)]
pub enum ApiKey {
    ApiVersions = 18,
    Fetch = 1,
    Produce = 0,
    DescribeTopicPartitions = 75,
    None = -1,
}
