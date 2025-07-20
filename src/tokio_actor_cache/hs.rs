use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;

use crate::tokio_actor_cache::data_struct::ValueEx;
use crate::tokio_actor_cache::error::TokioActorCacheError;

use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, interval};

#[derive(Debug)]
pub enum HashSetCmd<V> {
    Clear,
    Remove {
        val: V,
        resp_tx: oneshot::Sender<bool>,
    },
    Get {
        resp_tx: oneshot::Sender<HashSet<V>>,
    },
    Insert {
        val: V,
        duration: Option<Duration>,
    },
}

#[derive(Debug, Clone)]
pub struct HashSetCache<V> {
    pub tx: Sender<HashSetCmd<V>>,
}

impl<V> HashSetCache<V> {
    pub async fn clear(&self) -> Result<(), TokioActorCacheError> {
        let clear_cmd = HashSetCmd::Clear;
        self.tx
            .send(clear_cmd)
            .await
            .map_err(|_| return TokioActorCacheError::Send)
    }

    pub async fn remove(&self, val: V) -> Result<bool, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let remove_cmd = HashSetCmd::Remove { val, resp_tx };
        self.tx
            .send(remove_cmd)
            .await
            .map_err(|_| return TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn get(&self) -> Result<HashSet<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(HashSetCmd::Get { resp_tx })
            .await
            .map_err(|_| return TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn insert(
        &self,
        val: V,
        duration: Option<Duration>,
    ) -> Result<(), TokioActorCacheError> {
        self.tx
            .send(HashSetCmd::Insert { val, duration })
            .await
            .map_err(|_| return TokioActorCacheError::Send)
    }

    pub async fn new(buffer: usize) -> Self
    where
        V: Clone + Eq + Hash + Debug + Send + std::marker::Send + 'static,
    {
        let mut hs = HashSet::new();

        let (tx, mut rx) = mpsc::channel(buffer);

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Expire key-val.
                        hs.retain(|val_ex: &ValueEx<V>| match val_ex.expiration {
                            Some(exp) => Instant::now() < exp,
                            None => true,
                        });
                    }
                    command = rx.recv() => {
                        if let Some(cmd) = command {
                            match cmd {
                                HashSetCmd::<V>::Clear => {
                                    hs.clear();
                                }
                                HashSetCmd::<V>::Remove { val, resp_tx } => {
                                    let is_exist = hs.iter().any(|val_ex| val_ex.val == val);
                                    if is_exist {
                                        hs.retain(|val_ex| val_ex.val == val);
                                    }

                                    if let Err(_) = resp_tx.send(is_exist) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashSetCmd::<V>::Insert { val, duration } => {
                                    let expiration = duration.and_then(|d| Some(Instant::now() + d));
                                    let val_ex = ValueEx { val, expiration };
                                    hs.insert(val_ex);
                                }
                                HashSetCmd::<V>::Get { resp_tx } => {
                                    let val = hs
                                        .iter()
                                        .map(|h| h.val.clone())
                                        .collect::<HashSet<V>>()
                                        .clone();
                                    if let Err(_) = resp_tx.send(val) {
                                        println!("the receiver dropped");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Self { tx }
    }
}
