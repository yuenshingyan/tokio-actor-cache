use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::time::Duration;
use std::vec;

use crate::tokio_cache::compute::hash_id;
use crate::tokio_cache::data_struct::ValueEx;
use crate::tokio_cache::error::TokioActorCacheError;

use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, interval};

#[derive(Debug)]
pub enum HashSetCmd<V> {
    StopReplicating,
    IsReplica {
        resp_tx: oneshot::Sender<bool>,
    },
    Replicate {
        master: HashSetCache<V>,
    },
    GetAllRaw {
        resp_tx: oneshot::Sender<HashSet<ValueEx<V>>>,
    },
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
        ex: Vec<Option<Duration>>,
        nx: Vec<Option<bool>>,
    },
    Insert {
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    },
}

#[derive(Debug, Clone)]
pub struct HashSetCacheCluster<V> {
    pub nodes: HashMap<u64, HashSetCache<V>>,
}

impl<V> HashSetCacheCluster<V>
where
    V: Clone + Debug + Eq + Hash + Send + 'static + Display,
{
    pub async fn try_ttl(&self, vals: &[V]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
        let vals = vals.to_vec();
        let mut res = Vec::new();
        for val in vals {
            let node = self.get_node(val.clone())?;
            let (resp_tx, resp_rx) = oneshot::channel();
            let ttl_cmd = HashSetCmd::TTL {
                vals: vec![val],
                resp_tx,
            };
            node.tx
                .try_send(ttl_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(
                resp_rx
                    .await
                    .map_err(|_| return TokioActorCacheError::Receive)?,
            );
        }

        Ok(res)
    }

    pub async fn try_clear(&self) -> Result<(), TokioActorCacheError> {
        for node in self.nodes.values() {
            let clear_cmd = HashSetCmd::Clear;
            node.tx
                .try_send(clear_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
        }

        Ok(())
    }

    pub async fn try_remove(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let vals = vals.to_vec();
        let mut res = Vec::new();
        for val in vals {
            let node = self.get_node(val.clone())?;
            let (resp_tx, resp_rx) = oneshot::channel();
            let remove_cmd = HashSetCmd::Remove {
                vals: vec![val],
                resp_tx,
            };
            node.tx
                .try_send(remove_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(
                resp_rx
                    .await
                    .map_err(|_| return TokioActorCacheError::Receive)?,
            );
        }

        Ok(res)
    }

    pub async fn try_contains(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let vals = vals.to_vec();
        let mut res = Vec::new();
        for val in vals {
            let node = self.get_node(val.clone())?;
            let (resp_tx, resp_rx) = oneshot::channel();
            node.tx
                .try_send(HashSetCmd::Contains {
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

    pub async fn try_get_all(&self) -> Result<HashSet<V>, TokioActorCacheError> {
        let mut res = HashSet::new();
        for node in self.nodes.values() {
            let (resp_tx, resp_rx) = oneshot::channel();
            node.tx
                .try_send(HashSetCmd::GetAll { resp_tx })
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(
                resp_rx
                    .await
                    .map_err(|_| return TokioActorCacheError::Receive)?,
            );
        }

        Ok(res)
    }

    pub async fn try_minsert(
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
                .try_send(HashSetCmd::MInsert {
                    vals: vec![val],
                    ex: ex.clone(),
                    nx: nx.clone(),
                })
                .map_err(|_| TokioActorCacheError::Send)?;
        }

        Ok(())
    }

    pub async fn try_insert(
        &self,
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    ) -> Result<(), TokioActorCacheError> {
        let node = self.get_node(val.clone())?;
        node.tx
            .try_send(HashSetCmd::Insert { val, ex, nx })
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn ttl(&self, vals: &[V]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
        let vals = vals.to_vec();
        let mut res = Vec::new();
        for val in vals {
            let node = self.get_node(val.clone())?;
            let (resp_tx, resp_rx) = oneshot::channel();
            let ttl_cmd = HashSetCmd::TTL {
                vals: vec![val],
                resp_tx,
            };
            node.tx
                .send(ttl_cmd)
                .await
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
            let clear_cmd = HashSetCmd::Clear;
            node.tx
                .send(clear_cmd)
                .await
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
            let remove_cmd = HashSetCmd::Remove {
                vals: vec![val],
                resp_tx,
            };
            node.tx
                .send(remove_cmd)
                .await
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
                .send(HashSetCmd::Contains {
                    vals: vec![val],
                    resp_tx,
                })
                .await
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(
                resp_rx
                    .await
                    .map_err(|_| return TokioActorCacheError::Receive)?,
            );
        }

        Ok(res)
    }

    pub async fn get_all(&self) -> Result<HashSet<V>, TokioActorCacheError> {
        let mut res = HashSet::new();
        for node in self.nodes.values() {
            let (resp_tx, resp_rx) = oneshot::channel();
            node.tx
                .send(HashSetCmd::GetAll { resp_tx })
                .await
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(
                resp_rx
                    .await
                    .map_err(|_| return TokioActorCacheError::Receive)?,
            );
        }

        Ok(res)
    }

    pub async fn minsert(
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
                .send(HashSetCmd::MInsert {
                    vals: vec![val],
                    ex: ex.clone(),
                    nx: nx.clone(),
                })
                .await
                .map_err(|_| TokioActorCacheError::Send)?;
        }

        Ok(())
    }

    pub async fn insert(
        &self,
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    ) -> Result<(), TokioActorCacheError> {
        let node = self.get_node(val.clone())?;
        node.tx
            .send(HashSetCmd::Insert { val, ex, nx })
            .await
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(buffer: usize, n_node: u64) -> Self {
        let mut nodes = HashMap::new();
        for i in 0..n_node {
            let vec_cache = HashSetCache::<V>::new(buffer).await;
            nodes.insert(i, vec_cache);
        }
        Self { nodes }
    }

    fn get_node(&self, val: V) -> Result<HashSetCache<V>, TokioActorCacheError> {
        let val_str = format!("{}", val);
        let h_id = hash_id(&val_str, self.nodes.len() as u16) as u64;
        match self.nodes.get(&h_id) {
            Some(n) => Ok(n.clone()),
            None => return Err(TokioActorCacheError::NodeNotExists),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HashSetCache<V> {
    pub tx: Sender<HashSetCmd<V>>,
}

impl<V> HashSetCache<V>
where
    V: Clone,
{
    pub async fn try_stop_replicating(&self) -> Result<(), TokioActorCacheError> {
        let stop_replicating_cmd = HashSetCmd::StopReplicating;
        self.tx
            .try_send(stop_replicating_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn try_replicate(&self, master: &Self) -> Result<(), TokioActorCacheError> {
        let replicate_cmd = HashSetCmd::Replicate { master: master.clone() };
        self.tx
            .try_send(replicate_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn try_ttl(&self, vals: &[V]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
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

    pub async fn try_clear(&self) -> Result<(), TokioActorCacheError> {
        let clear_cmd = HashSetCmd::Clear;
        self.tx
            .try_send(clear_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn try_remove(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
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

    pub async fn try_contains(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        self.tx
            .try_send(HashSetCmd::Contains { vals, resp_tx })
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn try_get_all(&self) -> Result<HashSet<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .try_send(HashSetCmd::GetAll { resp_tx })
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn try_minsert(
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
            .try_send(HashSetCmd::MInsert { vals, ex, nx })
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn try_insert(
        &self,
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    ) -> Result<(), TokioActorCacheError> {
        self.tx
            .try_send(HashSetCmd::Insert { val, ex, nx })
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn stop_replicating(&self) -> Result<(), TokioActorCacheError> {
        let stop_replicating_cmd = HashSetCmd::StopReplicating;
        self.tx
            .send(stop_replicating_cmd)
            .await
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn replicate(&self, master: &Self) -> Result<(), TokioActorCacheError> {
        let replicate_cmd = HashSetCmd::Replicate { master: master.clone() };
        self.tx
            .send(replicate_cmd)
            .await
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn ttl(&self, vals: &[V]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        let ttl_cmd = HashSetCmd::TTL { vals, resp_tx };
        self.tx
            .send(ttl_cmd)
            .await
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn clear(&self) -> Result<(), TokioActorCacheError> {
        let clear_cmd = HashSetCmd::Clear;
        self.tx
            .send(clear_cmd)
            .await
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn remove(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        let remove_cmd = HashSetCmd::Remove { vals, resp_tx };
        self.tx
            .send(remove_cmd)
            .await
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn contains(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        self.tx
            .send(HashSetCmd::Contains { vals, resp_tx })
            .await
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn get_all(&self) -> Result<HashSet<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(HashSetCmd::GetAll { resp_tx })
            .await
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn minsert(
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
            .send(HashSetCmd::MInsert { vals, ex, nx })
            .await
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn insert(
        &self,
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    ) -> Result<(), TokioActorCacheError> {
        self.tx
            .send(HashSetCmd::Insert { val, ex, nx })
            .await
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(buffer: usize) -> Self
    where
        V: Clone + Eq + Hash + Debug + Send + 'static,
    {
        let mut hs = HashSet::new();
        let mut replica_of: Option<HashSetCache<V>> = None;

        let (tx, mut rx) = mpsc::channel(buffer);

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {

                        // Replicate master.
                        if let Some(ref master) = replica_of {
                            let (resp_tx, resp_rx) = oneshot::channel();
                            let get_all_raw_cmd = HashSetCmd::GetAllRaw { resp_tx };
                            if let Err(_) = master.tx.try_send(get_all_raw_cmd) {
                                eprintln!("the receiver dropped")
                            }
                            match resp_rx.await {
                                Ok(master_hs) => hs = master_hs,
                                Err(_) => eprintln!("the receiver dropped"),
                            }
                        }

                        // Expire key-val.
                        hs.retain(|val_ex: &ValueEx<V>| match val_ex.expiration {
                            Some(exp) => Instant::now() < exp,
                            None => true,
                        });
                    }
                    command = rx.recv() => {
                        if let Some(cmd) = command {
                            match cmd {
                                HashSetCmd::<V>::StopReplicating => {
                                    replica_of = None;
                                }
                                HashSetCmd::<V>::IsReplica { resp_tx } => {
                                    let is_replica = replica_of.is_some();
                                    if let Err(_) = resp_tx.send(is_replica) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashSetCmd::<V>::Replicate { master } => {
                                    replica_of = Some(master);
                                }
                                HashSetCmd::<V>::GetAllRaw { resp_tx } => {
                                    let val = hs.clone();
                                    if let Err(_) = resp_tx.send(val) {
                                        println!("the receiver dropped");
                                    }
                                }
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
                                    for ((val, ex), nx) in vals.into_iter().zip(ex).zip(nx) {
                                        let expiration = ex.and_then(|d| Some(Instant::now() + d));
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
