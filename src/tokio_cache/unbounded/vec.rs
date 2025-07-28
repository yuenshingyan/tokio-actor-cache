use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::time::Duration;

use crate::tokio_cache::compute::hash_id;
use crate::tokio_cache::data_struct::ValueEx;
use crate::tokio_cache::error::TokioActorCacheError;

use tokio::sync::mpsc::UnboundedSender;
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
pub struct VecCacheCluster<V> {
    pub nodes: HashMap<u64, VecCache<V>>,
}

impl<V> VecCacheCluster<V>
where
    V: Clone + Debug + Eq + Hash + Send + 'static + Display,
{
    pub async fn ttl(&self, vals: &[V]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
        let vals = vals.to_vec();
        let mut res = Vec::new();
        for val in vals {
            let node = self.get_node(val.clone())?;
            let (resp_tx, resp_rx) = oneshot::channel();
            let ttl_cmd = VecCmd::TTL {
                vals: vec![val],
                resp_tx,
            };
            node.tx
                .send(ttl_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(
                resp_rx
                    .await
                    .map_err(|_| return TokioActorCacheError::Receive)?,
            );
        }

        Ok(res)
    }

    pub async fn clear(&self) -> Result<(), TokioActorCacheError> {
        for node in self.nodes.values() {
            let clear_cmd = VecCmd::Clear;
            node.tx
                .send(clear_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
        }

        Ok(())
    }

    pub async fn remove(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let vals = vals.to_vec();
        let mut res = Vec::new();
        for val in vals {
            let node = self.get_node(val.clone())?;
            let (resp_tx, resp_rx) = oneshot::channel();
            let remove_cmd = VecCmd::Remove {
                vals: vec![val],
                resp_tx,
            };
            node.tx
                .send(remove_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(
                resp_rx
                    .await
                    .map_err(|_| return TokioActorCacheError::Receive)?,
            );
        }

        Ok(res)
    }

    pub async fn contains(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let vals = vals.to_vec();
        let mut res = Vec::new();
        for val in vals {
            let node = self.get_node(val.clone())?;
            let (resp_tx, resp_rx) = oneshot::channel();
            node.tx
                .send(VecCmd::Contains {
                    vals: vec![val],
                    resp_tx,
                })
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(
                resp_rx
                    .await
                    .map_err(|_| return TokioActorCacheError::Receive)?,
            );
        }

        Ok(res)
    }

    pub async fn get_all(&self) -> Result<Vec<V>, TokioActorCacheError> {
        let mut res = Vec::new();
        for node in self.nodes.values() {
            let (resp_tx, resp_rx) = oneshot::channel();
            let get_all_cmd = VecCmd::GetAll { resp_tx };
            node.tx
                .send(get_all_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(resp_rx.await.map_err(|_| TokioActorCacheError::Receive)?);
        }

        Ok(res)
    }

    pub async fn mpush(
        &self,
        vals: &[V],
        ex: &[Option<Duration>],
        nx: &[Option<bool>],
    ) -> Result<(), TokioActorCacheError> {
        if vals.len() != ex.len() || ex.len() != nx.len() {
            return Err(TokioActorCacheError::InconsistentLen);
        }

        let vals = vals.to_vec();
        let ex = ex.to_vec();
        let nx = nx.to_vec();
        for val in vals {
            let node = self.get_node(val.clone())?;
            node.tx
                .send(VecCmd::MPush {
                    vals: vec![val],
                    ex: ex.clone(),
                    nx: nx.clone(),
                })
                .map_err(|_| TokioActorCacheError::Send)?;
        }

        Ok(())
    }

    pub async fn push(
        &self,
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    ) -> Result<(), TokioActorCacheError> {
        let node = self.get_node(val.clone())?;
        let push_cmd = VecCmd::Push { val, ex, nx };
        node.tx
            .send(push_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(n_node: u64) -> Self {
        let mut nodes = HashMap::new();
        for i in 0..n_node {
            let vec_cache = VecCache::<V>::new().await;
            nodes.insert(i, vec_cache);
        }
        Self { nodes }
    }

    fn get_node(&self, val: V) -> Result<VecCache<V>, TokioActorCacheError> {
        let val_str = format!("{}", val);
        let h_id = hash_id(&val_str, self.nodes.len() as u16) as u64;
        match self.nodes.get(&h_id) {
            Some(n) => Ok(n.clone()),
            None => return Err(TokioActorCacheError::NodeNotExists),
        }
    }
}

#[derive(Debug, Clone)]
pub struct VecCache<V> {
    pub tx: UnboundedSender<VecCmd<V>>,
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
            .send(ttl_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn clear(&self) -> Result<(), TokioActorCacheError> {
        let clear_cmd = VecCmd::Clear;
        self.tx
            .send(clear_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn remove(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        let remove_cmd = VecCmd::Remove { vals, resp_tx };
        self.tx
            .send(remove_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn contains(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        self.tx
            .send(VecCmd::Contains { vals, resp_tx })
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn get_all(&self) -> Result<Vec<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(VecCmd::GetAll { resp_tx })
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
            return Err(TokioActorCacheError::InconsistentLen);
        }

        let vals = vals.to_vec();
        let ex = ex.to_vec();
        let nx = nx.to_vec();
        self.tx
            .send(VecCmd::MPush { vals, ex, nx })
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn push(
        &self,
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    ) -> Result<(), TokioActorCacheError> {
        self.tx
            .send(VecCmd::Push { val, ex, nx })
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new() -> Self
    where
        V: Clone + Eq + Hash + Debug + Send + 'static,
    {
        let mut vec = Vec::new();

        let (tx, mut rx) = mpsc::unbounded_channel();

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
