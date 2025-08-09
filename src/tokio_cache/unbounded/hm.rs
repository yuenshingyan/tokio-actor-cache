use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, interval};

use crate::tokio_cache::data_struct::ValueWithState;
use crate::tokio_cache::error::TokioActorCacheError;
use crate::tokio_cache::option::ExpirationPolicy;
use crate::tokio_cache::unbounded::cmd::HashMapCmd;

#[derive(Debug, Clone)]
pub struct HashMapCache<K, V> {
    pub tx: UnboundedSender<HashMapCmd<K, V>>,
}

impl<K, V> HashMapCache<K, V>
where
    K: Clone,
    V: Clone,
{
    pub async fn stop_replicating(&self) -> Result<(), TokioActorCacheError> {
        let stop_replicating_cmd = HashMapCmd::StopReplicating;
        self.tx
            .send(stop_replicating_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn replicate(&self, master: &Self) -> Result<(), TokioActorCacheError> {
        let replicate_cmd = HashMapCmd::Replicate { master: master.clone() };
        self.tx
            .send(replicate_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn ttl(&self, keys: &[K]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let keys = keys.to_vec();
        let ttl_cmd = HashMapCmd::TTL { keys, resp_tx };
        self.tx
            .send(ttl_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn get_all(&self) -> Result<HashMap<K, V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let get_all_cmd = HashMapCmd::GetAll { resp_tx };
        self.tx
            .send(get_all_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn clear(&self) -> Result<(), TokioActorCacheError> {
        let clear_cmd = HashMapCmd::Clear;
        self.tx
            .send(clear_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn remove(&self, keys: &[K]) -> Result<Vec<Option<V>>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let keys = keys.to_vec();
        let remove_cmd = HashMapCmd::Remove { keys, resp_tx };
        self.tx
            .send(remove_cmd)
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
            .send(contains_key_cmd)
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
            .send(mget_cmd)
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
        nx: &[bool],
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
            .send(minsert_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn get(&self, key: K) -> Result<Option<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let get_cmd = HashMapCmd::Get { key, resp_tx };
        self.tx
            .send(get_cmd)
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
        nx: bool,
    ) -> Result<(), TokioActorCacheError> {
        let insert_cmd = HashMapCmd::Insert { key, val, ex, nx };
        self.tx
            .send(insert_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(expiration_policy: ExpirationPolicy) -> Self
    where
        K: Debug + Clone + Eq + Hash + Send + 'static,
        V: Debug + Clone + Eq + Hash + Send + 'static,
    {
        let mut hm = match expiration_policy {
            ExpirationPolicy::LFU(capacity) | ExpirationPolicy::LRU(capacity) => {
                HashMap::<K, ValueWithState<V>>::with_capacity(capacity)
            },
            ExpirationPolicy::None => HashMap::<K, ValueWithState<V>>::new(),
        };
        let mut replica_of: Option<HashMapCache<K, V>> = None;

        let (tx, mut rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {

                        // Replicate master.
                        if let Some(ref master) = replica_of {
                            let (resp_tx, resp_rx) = oneshot::channel();
                            let get_all_raw_cmd = HashMapCmd::GetAllRaw { resp_tx };
                            if let Err(_) = master.tx.send(get_all_raw_cmd) {
                                eprintln!("the receiver dropped")
                            }
                            match resp_rx.await {
                                Ok(master_hm) => hm = master_hm,
                                Err(_) => eprintln!("the receiver dropped"),
                            }
                        }

                        // Invalidate cache.
                        hm.retain(|_k, val_with_state| match val_with_state.expiration {
                            Some(exp) => Instant::now() < exp,
                            None => true,
                        });

                        // Invalidate cache according to expiration policy.
                        match expiration_policy {
                            ExpirationPolicy::LFU(capacity) => {
                                let n_exceed = hm.len() - capacity;
                                if hm.len() > capacity {
                                    // Find the key with the minimum call_cnt (least frequently used).
                                    for _ in 0..n_exceed {
                                        if let Some(lfu_key) = hm
                                            .iter()
                                            .min_by_key(|(_key, val_with_state)| val_with_state.call_cnt)
                                            .map(|(key, _val_with_state)| key.clone())
                                        {
                                            println!("{:?}", lfu_key);
                                            hm.remove(&lfu_key);
                                        }
                                    }
                                }
                            },
                            ExpirationPolicy::LRU(capacity) => {
                                let n_exceed = hm.len() - capacity;
                                if hm.len() > capacity {
                                    // Find the key with the minimum last_accessed (least recently used).
                                    for _ in 0..n_exceed {
                                        if let Some(lru_key) = hm
                                            .iter()
                                            .min_by_key(|(_key, val_with_state)| val_with_state.last_accessed)
                                            .map(|(key, _val_with_state)| key.clone())
                                        {
                                            hm.remove(&lru_key);
                                        }
                                    }
                                }
                            },
                            ExpirationPolicy::None => (),
                        };
                    }

                    // Handle commands.
                    command = rx.recv() => {
                        if let Some(cmd) = command {
                            match cmd {
                                HashMapCmd::<K, V>::StopReplicating => {
                                    replica_of = None;
                                }
                                HashMapCmd::<K, V>::IsReplica { resp_tx } => {
                                    let is_replica = replica_of.is_some();
                                    
                                    if let Err(_) = resp_tx.send(is_replica) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::Replicate { master } => {
                                    replica_of = Some(master);
                                }
                                HashMapCmd::<K, V>::GetAllRaw { resp_tx } => {
                                    let val = hm.clone();
                                    
                                    if let Err(_) = resp_tx.send(val) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::TTL { keys, resp_tx } => {
                                    let ttl = keys.iter().map(|key| {

                                        // Get 'val_with_state' by 'key'.
                                        hm.get_mut(&key).and_then(|val_with_state| {

                                            // incr 'call_cnt' by 1 and update 'last_accessed'.
                                            val_with_state.call_cnt += 1;
                                            val_with_state.last_accessed = Instant::now();

                                            // Get ttl from 'val_with_state'.
                                            val_with_state.expiration.and_then(|ex| {
                                                    ex.checked_duration_since(Instant::now())
                                            })
                                        })
                                    }).collect::<Vec<Option<Duration>>>();
                                    if let Err(_) = resp_tx.send(ttl) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::GetAll { resp_tx } => {
                                    let vals = hm.iter_mut().map(|(key, val_with_state)| {
                                        val_with_state.call_cnt += 1;
                                        val_with_state.last_accessed = Instant::now();

                                        (key.clone(), val_with_state.val.clone())
                                    }).collect::<HashMap<K, V>>();

                                    if let Err(_) = resp_tx.send(vals) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::Clear => {
                                    hm.clear();
                                }
                                HashMapCmd::<K, V>::Remove { keys, resp_tx } => {
                                    let vals = keys.iter().map(|key| {
                                        hm.remove(&key).and_then(|val_with_state| {
                                            Some(val_with_state.val)
                                        })
                                    }).collect::<Vec<Option<V>>>();
                                    if let Err(_) = resp_tx.send(vals) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::ContainsKey {keys, resp_tx } => {
                                    let is_contains_keys = keys.iter().map(|key| {

                                        // Incr 'call_cnt' by 1 and update 'last_accessed'.
                                        hm.get_mut(key).and_then(|val_with_state| {
                                            val_with_state.call_cnt += 1;
                                            val_with_state.last_accessed = Instant::now();
                                            Some(())
                                        });

                                        hm.contains_key(&key)
                                    }).collect::<Vec<bool>>();

                                    if let Err(_) = resp_tx.send(is_contains_keys) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::MGet { keys, resp_tx } => {
                                    let vals = keys.iter().map(|key| {
                                        hm.get_mut(&key).and_then(|val_with_state| {
                                            val_with_state.call_cnt += 1;
                                            val_with_state.last_accessed = Instant::now();
                                            Some(val_with_state.val.clone())
                                        })
                                    }).collect::<Vec<Option<V>>>();
                                    if let Err(_) = resp_tx.send(vals) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::MInsert { keys, vals, ex, nx } => {
                                    for (((key, val), ex), nx) in keys.into_iter().zip(vals).zip(ex).zip(nx) {
                                        let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                        let last_accessed = Instant::now();

                                        match (hm.get(&key), nx) {
                                            (Some(val_with_state), false) => {
                                                let call_cnt = val_with_state.call_cnt + 1;
                                                let val_with_state = ValueWithState { 
                                                    val, 
                                                    expiration, 
                                                    call_cnt, 
                                                    last_accessed,
                                                };
                                                hm.insert(key, val_with_state);
                                            },
                                            (None, true) | (None, false) => {
                                                let call_cnt = 0;
                                                let val_with_state = ValueWithState { 
                                                    val, 
                                                    expiration, 
                                                    call_cnt, 
                                                    last_accessed,
                                                };
                                                hm.insert(key, val_with_state);
                                            },
                                            _ => (),
                                        }
                                    }
                                }
                                HashMapCmd::<K, V>::Get { key, resp_tx } => {
                                    let val = hm.get_mut(&key).and_then(|val_with_state| {
                                        val_with_state.call_cnt += 1;
                                        val_with_state.last_accessed = Instant::now();
                                        Some(val_with_state.val.clone())
                                    });

                                    if let Err(_) = resp_tx.send(val) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashMapCmd::<K, V>::Insert { key, val, ex, nx } => {
                                    let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                    let last_accessed = Instant::now();

                                    match (hm.get(&key), nx) {
                                        (Some(val_with_state), false) => {
                                            let call_cnt = val_with_state.call_cnt + 1;
                                            let val_with_state = ValueWithState { 
                                                val, 
                                                expiration, 
                                                call_cnt, 
                                                last_accessed,
                                            };
                                            hm.insert(key, val_with_state);
                                        },
                                        (None, true) | (None, false) => {
                                            let call_cnt = 0;
                                            let val_with_state = ValueWithState { 
                                                val, 
                                                expiration, 
                                                call_cnt, 
                                                last_accessed,
                                            };
                                            hm.insert(key, val_with_state);
                                        },
                                        _ => (),
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
