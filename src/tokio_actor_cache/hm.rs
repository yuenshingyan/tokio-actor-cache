use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::time::Duration;

use crate::tokio_actor_cache::compute::hash_id;
use crate::tokio_actor_cache::data_struct::ValueEx;
use crate::tokio_actor_cache::error::TokioActorCacheError;

use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, interval};

#[derive(Debug)]
pub enum HashMapCmd<K, V> {
    TTL {
        keys: Vec<K>,
        resp_tx: oneshot::Sender<Vec<Option<Duration>>>,
    },
    GetAll {
        resp_tx: oneshot::Sender<HashMap<K, V>>,
    },
    Clear,
    Remove {
        keys: Vec<K>,
        resp_tx: oneshot::Sender<Vec<Option<V>>>,
    },
    ContainsKey {
        keys: Vec<K>,
        resp_tx: oneshot::Sender<Vec<bool>>,
    },
    MGet {
        keys: Vec<K>,
        resp_tx: oneshot::Sender<Vec<Option<V>>>,
    },
    MInsert {
        keys: Vec<K>,
        vals: Vec<V>,
        ex: Vec<Option<Duration>>,
        nx: Vec<Option<bool>>,
    },
    Get {
        key: K,
        resp_tx: oneshot::Sender<Option<V>>,
    },
    Insert {
        key: K,
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    },
}

#[derive(Debug, Clone)]
pub struct HashMapCacheCluster<K, V> {
    pub nodes: HashMap<u64, HashMapCache<K, V>>
}

impl<K, V> HashMapCacheCluster<K, V>
where
    K: Clone + Debug + Eq + Hash + Send + 'static + Display,
    V: Clone + Debug + Eq + Hash + Send + 'static,
{
    pub async fn ttl(&self, keys: &[K]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
        let keys = keys.to_vec();
        
        let mut res = Vec::new();
        for key in keys.clone() {
            let (resp_tx, resp_rx) = oneshot::channel();
            let ttl_cmd = HashMapCmd::TTL { 
                keys: vec![key.clone()], 
                resp_tx, 
            };
            let node = self.get_node(key)?;
            node.tx
                .try_send(ttl_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
            let r = resp_rx
                .await
                .map_err(|_| return TokioActorCacheError::Receive)?;
            res.extend(r);
        }
        
        Ok(res)
    }

    pub async fn get_all(&self) -> Result<HashMap<K, V>, TokioActorCacheError> {
        let mut res = HashMap::new();
        for node in self.nodes.values() {
            let (resp_tx, resp_rx) = oneshot::channel();
            let get_all_cmd = HashMapCmd::GetAll { resp_tx };
            node.tx
                .try_send(get_all_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(
                resp_rx
                    .await
                    .map_err(|_| return TokioActorCacheError::Receive)?
            );
        }

        Ok(res)
    }

    pub async fn clear(&self) -> Result<(), TokioActorCacheError> {
        for node in self.nodes.values() {
            let clear_cmd = HashMapCmd::Clear;
            node.tx
                .try_send(clear_cmd)
                .map_err(|_| TokioActorCacheError::Send)?
        }

        Ok(())
    }

    pub async fn remove(&self, keys: &[K]) -> Result<Vec<Option<V>>, TokioActorCacheError> {
        let keys = keys.to_vec();
        let mut res = Vec::new();
        for key in keys.clone() {
            let (resp_tx, resp_rx) = oneshot::channel();
            let remove_cmd = HashMapCmd::Remove { 
                keys: vec![key.clone()], 
                resp_tx,
            };
            let node = self.get_node(key)?;
            node.tx
                .try_send(remove_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(
                resp_rx
                .await
                .map_err(|_| return TokioActorCacheError::Receive)?
            );
        }
        
        Ok(res)
    }

    pub async fn contains_key(&self, keys: &[K]) -> Result<Vec<bool>, TokioActorCacheError> {
        let keys = keys.to_vec();
        let mut res = Vec::new();
        for key in keys.clone() {
            let (resp_tx, resp_rx) = oneshot::channel();
            let contains_key_cmd = HashMapCmd::ContainsKey { 
                keys: vec![key.clone()], 
                resp_tx,
            };
            let node = self.get_node(key)?;
            node.tx
                .try_send(contains_key_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(
                resp_rx
                .await
                .map_err(|_| return TokioActorCacheError::Receive)?
            );
        }
        
        Ok(res)
    }

    pub async fn mget(&self, keys: &[K]) -> Result<Vec<Option<V>>, TokioActorCacheError> {
        let keys = keys.to_vec();
        let mut res = Vec::new();
        for key in keys.clone() {
            let (resp_tx, resp_rx) = oneshot::channel();
            let mget_cmd = HashMapCmd::MGet { 
                keys: vec![key.clone()], 
                resp_tx,
            };
            let node = self.get_node(key)?;
            node.tx
                .try_send(mget_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(
                resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)?
            );
        }
        
        Ok(res)
    }

    pub async fn minsert(
        &self,
        keys: &[K],
        vals: &[V],
        ex: &[Option<Duration>],
        nx: &[Option<bool>],
    ) -> Result<(), TokioActorCacheError> {
        if keys.len() != vals.len() || vals.len() != ex.len() || ex.len() != nx.len() {
            return Err(TokioActorCacheError::InconsistentLen);
        }

        let keys = keys.to_vec();
        let vals = vals.to_vec();
        let ex = ex.to_vec();
        let nx = nx.to_vec();

        for (key, val) in keys.into_iter().zip(vals).clone() {
            let minsert_cmd = HashMapCmd::MInsert { 
                keys: vec![key.clone()], 
                vals: vec![val.clone()], 
                ex: ex.clone(), 
                nx: nx.clone(),
            };
            let node = self.get_node(key)?;
            node.tx
                .try_send(minsert_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
        }
        
        Ok(())
    }

    pub async fn get(&self, key: K) -> Result<Option<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let get_cmd = HashMapCmd::Get { key: key.clone(), resp_tx };
        let node = self.get_node(key)?;
        node.tx
            .try_send(get_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn insert(
        &self,
        key: K,
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    ) -> Result<(), TokioActorCacheError> {
        let insert_cmd = HashMapCmd::Insert { key: key.clone(), val, ex, nx };
        let node = self.get_node(key)?;
        node.tx
            .try_send(insert_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(buffer: usize, n_node: u64) -> Self {
        let mut nodes = HashMap::new();
        for i in 0..n_node {
            let vec_cache = HashMapCache::<K, V>::new(buffer).await;
            nodes.insert(i, vec_cache);
        }
        Self {
            nodes
        }
    }

    fn get_node(&self, key: K) -> Result<HashMapCache<K, V>, TokioActorCacheError> {
        let key_str = format!("{}", key);
        let h_id = hash_id(&key_str, self.nodes.len() as u16) as u64;
        match self.nodes.get(&h_id) {
            Some(n) => Ok(n.clone()),
            None => return Err(TokioActorCacheError::NodeNotExists),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HashMapCache<K, V> {
    pub tx: Sender<HashMapCmd<K, V>>,
}

impl<K, V> HashMapCache<K, V>
where
    K: Clone,
    V: Clone,
{
    pub async fn ttl(&self, keys: &[K]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let keys = keys.to_vec();
        let ttl_cmd = HashMapCmd::TTL { keys, resp_tx };
        self.tx
            .try_send(ttl_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn get_all(&self) -> Result<HashMap<K, V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let get_all_cmd = HashMapCmd::GetAll { resp_tx };
        self.tx
            .try_send(get_all_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn clear(&self) -> Result<(), TokioActorCacheError> {
        let clear_cmd = HashMapCmd::Clear;
        self.tx
            .try_send(clear_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn remove(&self, keys: &[K]) -> Result<Vec<Option<V>>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let keys = keys.to_vec();
        let remove_cmd = HashMapCmd::Remove { keys, resp_tx };
        self.tx
            .try_send(remove_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn contains_key(&self, keys: &[K]) -> Result<Vec<bool>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let keys = keys.to_vec();
        let contains_key_cmd = HashMapCmd::ContainsKey { keys, resp_tx };
        self.tx
            .try_send(contains_key_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn mget(&self, keys: &[K]) -> Result<Vec<Option<V>>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let keys = keys.to_vec();
        let mget_cmd = HashMapCmd::MGet { keys, resp_tx };
        self.tx
            .try_send(mget_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn minsert(
        &self,
        keys: &[K],
        vals: &[V],
        ex: &[Option<Duration>],
        nx: &[Option<bool>],
    ) -> Result<(), TokioActorCacheError> {
        if keys.len() != vals.len() || vals.len() != ex.len() || ex.len() != nx.len() {
            return Err(TokioActorCacheError::InconsistentLen);
        }

        let keys = keys.to_vec();
        let vals = vals.to_vec();
        let ex = ex.to_vec();
        let nx = nx.to_vec();
        let minsert_cmd = HashMapCmd::MInsert { keys, vals, ex, nx };
        self.tx
            .try_send(minsert_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn get(&self, key: K) -> Result<Option<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let get_cmd = HashMapCmd::Get { key, resp_tx };
        self.tx
            .try_send(get_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn insert(
        &self,
        key: K,
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    ) -> Result<(), TokioActorCacheError> {
        let insert_cmd = HashMapCmd::Insert { key, val, ex, nx };
        self.tx
            .try_send(insert_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(buffer: usize) -> Self
    where
        K: Debug + Clone + Eq + Hash + Send + 'static,
        V: Debug + Clone + Eq + Hash + Send + 'static,
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
                                HashMapCmd::<K, V>::TTL { keys, resp_tx } => {
                                    let ttl = keys.iter().map(|key| {
                                        hm.get(&key).and_then(|val_ex| {
                                            val_ex.expiration.and_then(|ex| {
                                                    ex.checked_duration_since(Instant::now())
                                            })
                                        })
                                    }).collect::<Vec<Option<Duration>>>();
                                    if let Err(_) = resp_tx.send(ttl) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::GetAll { resp_tx } => {
                                    let val = hm.clone().into_iter().map(|(key, val_ex)| (key, val_ex.val)).collect::<HashMap<K, V>>();
                                    if let Err(_) = resp_tx.send(val) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::Clear => {
                                    hm.clear();
                                }
                                HashMapCmd::<K, V>::Remove { keys, resp_tx } => {
                                    let vals = keys.iter().map(|key| {
                                        hm.remove(&key).and_then(|val_ex| Some(val_ex.val))
                                    }).collect::<Vec<Option<V>>>();
                                    if let Err(_) = resp_tx.send(vals) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::ContainsKey {keys, resp_tx } => {
                                    let is_contains_keys = keys.iter().map(|key| {
                                        hm.contains_key(&key)
                                    }).collect::<Vec<bool>>();
                                    if let Err(_) = resp_tx.send(is_contains_keys) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::MGet { keys, resp_tx } => {
                                    let vals = keys.iter().map(|key| {
                                        hm.get(&key).and_then(|val_ex| Some(val_ex.val.clone()))
                                    }).collect::<Vec<Option<V>>>();
                                    if let Err(_) = resp_tx.send(vals) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::MInsert { keys, vals, ex, nx } => {
                                    for (((key, val), ex), nx) in keys.into_iter().zip(vals).zip(ex).zip(nx) {
                                        let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                        let val_ex = ValueEx { val, expiration };
                                        if nx.is_some() && nx == Some(true) {
                                            hm.entry(key).or_insert(val_ex);
                                        } else {
                                            hm.insert(key, val_ex);
                                        }
                                    }
                                }
                                HashMapCmd::<K, V>::Get { key, resp_tx } => {
                                    let val = hm.get(&key).and_then(|val_ex| Some(val_ex.val.clone()));
                                    if let Err(_) = resp_tx.send(val) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::Insert { key, val, ex, nx } => {
                                    let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                    let val_ex = ValueEx { val, expiration };
                                    if nx.is_some() && nx == Some(true) {
                                        hm.entry(key).or_insert(val_ex);
                                    } else {
                                        hm.insert(key, val_ex);
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
