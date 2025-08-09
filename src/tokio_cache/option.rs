#[derive(Clone, Copy)]
pub enum ExpirationPolicy {
    LFU(usize),
    LRU(usize),
    None,
}