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
        resp_tx: oneshot::Sender<Vec<V>>,
    },
    MPush {
        vals: Vec<V>,
        ex: Vec<Option<Duration>>,
        nx: Vec<Option<bool>>,
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

impl<V> VecCache<V>
where
    V: Clone,
{
    pub async fn ttl(&self, vals: &[V]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        let ttl_cmd = VecCmd::TTL { vals, resp_tx };
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

    pub async fn remove(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        let remove_cmd = VecCmd::Remove { vals, resp_tx };
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
            .try_send(VecCmd::Contains { vals, resp_tx })
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

    pub async fn mpush(
        &self,
        vals: &[V],
        ex: &[Option<Duration>],
        nx: &[Option<bool>],
    ) -> Result<(), TokioActorCacheError> {
        if vals.len() != ex.len() || ex.len() != nx.len() {
            return Err(TokioActorCacheError::InconsistentLen)
        }
        
        let vals = vals.to_vec();
        let ex = ex.to_vec();
        let nx = nx.to_vec();
        self.tx
            .try_send(VecCmd::MPush { vals, ex, nx })
            .map_err(|_| TokioActorCacheError::Send)
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
                                VecCmd::<V>::TTL { vals, resp_tx } => {
                                    let ttl = vals.into_iter().map(|val| {
                                        let res = vec.iter().find(|val_ex| {
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
                                VecCmd::<V>::Clear => {
                                    vec.clear();
                                }
                                VecCmd::<V>::Remove { vals, resp_tx } => {
                                    let is_exist = vals.into_iter().map(|val| {
                                        let is_exist = vec.iter().any(|val_ex| val_ex.val == val);
                                        if is_exist {
                                            vec.retain(|val_ex| val_ex.val != val);
                                            true
                                        } else {
                                            false
                                        }
                                    }).collect::<Vec<bool>>();
                                    if let Err(_) = resp_tx.send(is_exist) {
                                        println!("the receiver dropped");
                                    }
                                }
                                VecCmd::<V>::Contains { vals, resp_tx } => {
                                    let is_exist = vals.into_iter().map(|val| {
                                        vec.iter().any(|val_ex| val_ex.val == val)
                                    }).collect::<Vec<bool>>();
                                    if let Err(_) = resp_tx.send(is_exist) {
                                        println!("the receiver dropped");
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
                                VecCmd::<V>::MPush { vals, ex, nx } => {
                                    for ((val, ex), nx) in vals.into_iter().zip(ex).zip(nx) {
                                        let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                        let val_ex = ValueEx { val, expiration };
                                        if nx.is_some() && nx == Some(true) && !vec.contains(&val_ex) {
                                            vec.push(val_ex);
                                        } else {
                                            vec.push(val_ex);
                                        }
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
                            }
                        }
                    }
                }
            }
        });

        Self { tx }
    }
}
