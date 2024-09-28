use num_derive::FromPrimitive;

#[derive(Copy, Clone, Debug, FromPrimitive)]
pub enum ApiKey {
    ApiVersions = 18,
    Fetch = 1,
    Produce = 0,
    None = -1,
}
