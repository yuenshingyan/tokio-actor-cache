use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;

use crate::tokio_actor_cache::data_struct::ValueEx;
use crate::tokio_actor_cache::error::TokioActorCacheError;

use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Instant};

#[derive(Debug)]
pub enum HashMapCmd<K, V> {
    Clear,
    Remove {
        key: K,
        resp_tx: oneshot::Sender<Option<V>>,
    },
    ContainsKey {
        key: K,
        resp_tx: oneshot::Sender<bool>,
    },
    Get {
        key: K,
        resp_tx: oneshot::Sender<Option<V>>,
    },
    Insert {
        key: K,
        val: V,
        duration: Option<Duration>,
    },
}

#[derive(Debug, Clone)]
pub struct HashMapCache<K, V> {
    pub tx: Sender<HashMapCmd<K, V>>,
}

impl<K, V> HashMapCache<K, V> {
    pub async fn clear(&self) -> Result<(), TokioActorCacheError> {
        let clear_cmd = HashMapCmd::Clear;
        self.tx
            .send(clear_cmd)
            .await
            .map_err(|_| return TokioActorCacheError::Send)
    }

    pub async fn remove(&self, key: K) -> Result<Option<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let remove_cmd = HashMapCmd::Remove { key, resp_tx };
        self.tx
            .send(remove_cmd)
            .await
            .map_err(|_| return TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn contains_key(&self, key: K) -> Result<bool, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let contains_key_cmd = HashMapCmd::ContainsKey { key, resp_tx };
        self.tx
            .send(contains_key_cmd)
            .await
            .map_err(|_| return TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn get(&self, key: K) -> Result<Option<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let get_cmd = HashMapCmd::Get { key, resp_tx };
        self.tx
            .send(get_cmd)
            .await
            .map_err(|_| return TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn insert(
        &self,
        key: K,
        val: V,
        duration: Option<Duration>,
    ) -> Result<(), TokioActorCacheError> {
        let insert_cmd = HashMapCmd::Insert { key, val, duration };
        self.tx
            .send(insert_cmd)
            .await
            .map_err(|_| return TokioActorCacheError::Send)
    }

    pub async fn new(buffer: usize) -> Self
    where
        K: Debug + Clone + Eq + Hash + Send + std::marker::Send + 'static,
        V: Debug + Clone + Eq + Hash + Send + std::marker::Send + 'static,
    {
        let mut hm = HashMap::<K, ValueEx<V>>::new();

        let (tx, mut rx) = mpsc::channel(buffer);

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(100));
            loop {
                tokio::select! {

                    // Expire key-val.
                    _ = ticker.tick() => {
                        hm.retain(|_k, val_ex| match val_ex.expiration {
                            Some(exp) => Instant::now() < exp,
                            None => true,
                        });
                    }

                    // Handle commands.
                    command = rx.recv() => {
                        if let Some(cmd) = command {
                            match cmd {
                                HashMapCmd::<K, V>::Clear => {
                                    hm.clear();
                                }
                                HashMapCmd::<K, V>::Remove { key, resp_tx } => {
                                    let val = hm.remove(&key).and_then(|val_ex| Some(val_ex.val));
                                    if let Err(_) = resp_tx.send(val) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::ContainsKey {key, resp_tx } => {
                                    let is_contains_key = hm.contains_key(&key);
                                    if let Err(_) = resp_tx.send(is_contains_key) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::Insert { key, val, duration } => {
                                    let expiration = duration.and_then(|d| Some(Instant::now() + d));
                                    let val_ex = ValueEx { val, expiration };
                                    hm.insert(key, val_ex);
                                }
                                HashMapCmd::<K, V>::Get { key, resp_tx } => {
                                    let val = hm.get(&key).and_then(|val_ex| Some(val_ex.val.clone()));
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
