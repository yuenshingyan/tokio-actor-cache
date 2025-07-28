use tokio::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ValueEx<V> {
    pub val: V,
    pub expiration: Option<Instant>,
}
