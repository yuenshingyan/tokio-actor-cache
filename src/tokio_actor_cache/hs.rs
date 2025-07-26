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
    TTL {
        vals: Vec<V>,
        resp_tx: oneshot::Sender<Vec<Option<Duration>>>,
    },
    Clear,
    Remove {
        vals: Vec<V>,
        resp_tx: oneshot::Sender<Vec<bool>>,
    },
    Contains {
        vals: Vec<V>,
        resp_tx: oneshot::Sender<Vec<bool>>,
    },
    GetAll {
        resp_tx: oneshot::Sender<HashSet<V>>,
    },
    MInsert {
        vals: Vec<V>,
        ex: Option<Duration>,
        nx: Option<bool>,
    },
    Insert {
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    },
}

#[derive(Debug, Clone)]
pub struct HashSetCache<V> {
    pub tx: Sender<HashSetCmd<V>>,
}

impl<V> HashSetCache<V>
where 
    V: Clone,
{
    pub async fn ttl(&self, vals: &[V]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        let ttl_cmd = HashSetCmd::TTL { vals, resp_tx };
        self.tx
            .try_send(ttl_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn clear(&self) -> Result<(), TokioActorCacheError> {
        let clear_cmd = HashSetCmd::Clear;
        self.tx
            .try_send(clear_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn remove(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        let remove_cmd = HashSetCmd::Remove { vals, resp_tx };
        self.tx
            .try_send(remove_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn contains(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        self.tx
            .try_send(HashSetCmd::Contains { vals, resp_tx })
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn get_all(&self) -> Result<HashSet<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .try_send(HashSetCmd::GetAll { resp_tx })
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn minsert(
        &self,
        vals: &[V],
        ex: Option<Duration>,
        nx: Option<bool>,
    ) -> Result<(), TokioActorCacheError> {
        let vals = vals.to_vec();
        self.tx
            .try_send(HashSetCmd::MInsert { vals, ex, nx })
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn insert(
        &self,
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    ) -> Result<(), TokioActorCacheError> {
        self.tx
            .try_send(HashSetCmd::Insert { val, ex, nx })
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(buffer: usize) -> Self
    where
        V: Clone + Eq + Hash + Debug + Send + 'static,
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
                                HashSetCmd::<V>::TTL { vals, resp_tx } => {
                                    let ttl = vals.into_iter().map(|val| {
                                        let res = hs.iter().find(|val_ex| {
                                            val_ex.val == val
                                        });
                                        res.and_then(|val_ex| {
                                            val_ex.expiration.and_then(|ex| {
                                                ex.checked_duration_since(Instant::now())
                                            })
                                        })
                                    }).collect::<Vec<Option<Duration>>>();
                                    if let Err(_) = resp_tx.send(ttl) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashSetCmd::<V>::Clear => {
                                    hs.clear();
                                }
                                HashSetCmd::<V>::Remove { vals, resp_tx } => {
                                    let is_exist = vals.into_iter().map(|val| {
                                        let is_exist = hs.iter().any(|val_ex| val_ex.val == val);
                                        if is_exist {
                                            hs.retain(|val_ex| val_ex.val != val);
                                            true
                                        } else {
                                            false
                                        }
                                    }).collect::<Vec<bool>>();

                                    if let Err(_) = resp_tx.send(is_exist) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashSetCmd::<V>::Contains { vals, resp_tx } => {
                                    let is_exist = vals.into_iter().map(|val| {
                                        hs.iter().any(|val_ex| val_ex.val == val)
                                    }).collect::<Vec<bool>>();
                                    if let Err(_) = resp_tx.send(is_exist) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashSetCmd::<V>::GetAll { resp_tx } => {
                                    let val = hs
                                        .iter()
                                        .map(|h| h.val.clone())
                                        .collect::<HashSet<V>>()
                                        .clone();
                                    if let Err(_) = resp_tx.send(val) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashSetCmd::<V>::MInsert { vals, ex, nx } => {
                                    let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                    for val in vals {
                                        let val_ex = ValueEx { val, expiration };
                                        if nx.is_some() && nx == Some(true) && !hs.contains(&val_ex) {
                                            hs.insert(val_ex);
                                        } else {
                                            hs.insert(val_ex);
                                        }
                                    }
                                }
                                HashSetCmd::<V>::Insert { val, ex, nx } => {
                                    let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                    let val_ex = ValueEx { val, expiration };
                                    if nx.is_some() && nx == Some(true) && !hs.contains(&val_ex) {
                                        hs.insert(val_ex);
                                    } else {
                                        hs.insert(val_ex);
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
