use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;

use crate::tokio_actor_cache::data_struct::ValueEx;
use crate::tokio_actor_cache::error::TokioActorCacheError;

use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, interval};

#[derive(Debug)]
pub enum VecCmd<V> {
    TTL {
        val: V,
        resp_tx: oneshot::Sender<Option<Duration>>,
    },
    Clear,
    Remove {
        val: V,
        resp_tx: oneshot::Sender<bool>,
    },
    Contains {
        val: V,
        resp_tx: oneshot::Sender<bool>,
    },
    GetAll {
        resp_tx: oneshot::Sender<Vec<V>>,
    },
    Push {
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    },
}

#[derive(Debug, Clone)]
pub struct VecCache<V> {
    pub tx: Sender<VecCmd<V>>,
}

impl<V> VecCache<V> {
    pub async fn ttl(&self, val: V) -> Result<Option<Duration>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let ttl_cmd = VecCmd::TTL { val, resp_tx };
        self.tx
            .try_send(ttl_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn clear(&self) -> Result<(), TokioActorCacheError> {
        let clear_cmd = VecCmd::Clear;
        self.tx
            .try_send(clear_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn remove(&self, val: V) -> Result<bool, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let remove_cmd = VecCmd::Remove { val, resp_tx };
        self.tx
            .try_send(remove_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn contains(&self, val: V) -> Result<bool, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .try_send(VecCmd::Contains { val, resp_tx })
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn get_all(&self) -> Result<Vec<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .try_send(VecCmd::GetAll { resp_tx })
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx.await.map_err(|_| TokioActorCacheError::Receive)
    }

    pub async fn push(
        &self,
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    ) -> Result<(), TokioActorCacheError> {
        self.tx
            .try_send(VecCmd::Push { val, ex, nx })
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(buffer: usize) -> Self
    where
        V: Clone + Eq + Hash + Debug + Send + 'static,
    {
        let mut vec = Vec::new();

        let (tx, mut rx) = mpsc::channel(buffer);

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Expire key-val.
                        vec.retain(|val_ex: &ValueEx<V>| match val_ex.expiration {
                            Some(exp) => Instant::now() < exp,
                            None => true,
                        });
                    }
                    command = rx.recv() => {
                        if let Some(cmd) = command {
                            match cmd {
                                VecCmd::<V>::TTL { val, resp_tx } => {
                                    let mut ttl = None;
                                    for val_ex in &vec {
                                        if val_ex.val == val {
                                            ttl = val_ex.expiration.and_then(|ex| {
                                                ex.checked_duration_since(Instant::now())
                                            });
                                        }
                                    }
                                    if let Err(_) = resp_tx.send(ttl) {
                                        println!("the receiver dropped");
                                    }
                                }
                                VecCmd::<V>::Clear => {
                                    vec.clear();
                                }
                                VecCmd::<V>::Remove { val, resp_tx } => {
                                    let is_exist = vec.iter().any(|val_ex| val_ex.val == val);
                                    if is_exist {
                                        vec.retain(|val_ex| val_ex.val != val);
                                    }

                                    if let Err(_) = resp_tx.send(is_exist) {
                                        println!("the receiver dropped");
                                    }
                                }
                                VecCmd::<V>::Contains { val, resp_tx } => {
                                    let is_exist = vec.iter().any(|val_ex| val_ex.val == val);
                                    if let Err(_) = resp_tx.send(is_exist) {
                                        println!("the receiver dropped");
                                    }
                                }
                                VecCmd::<V>::Push { val, ex, nx } => {
                                    let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                    let val_ex = ValueEx { val, expiration };
                                    if nx.is_some() && nx == Some(true) && !vec.contains(&val_ex) {
                                        vec.push(val_ex);
                                    } else {
                                        vec.push(val_ex);
                                    }
                                }
                                VecCmd::<V>::GetAll { resp_tx } => {
                                    let val = vec
                                        .clone()
                                        .into_iter()
                                        .map(|val_ex| val_ex.val)
                                        .collect::<Vec<V>>();
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
