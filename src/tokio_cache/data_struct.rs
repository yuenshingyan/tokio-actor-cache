use tokio::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HashSetState {
    pub expiration: Option<Instant>,
    pub call_cnt: u64,
    pub last_accessed: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ValueEx<V> {
    pub val: V,
    pub expiration: Option<Instant>,
    pub call_cnt: u64,
    pub last_accessed: Instant,
}
