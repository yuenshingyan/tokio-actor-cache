use thiserror::Error;

#[derive(Error, Debug)]
pub enum TokioActorCacheError {
    #[error("unknown data store error")]
    InconsistentLen,
    #[error("unknown data store error")]
    Receive,
    #[error("unknown data store error")]
    Send,
}
