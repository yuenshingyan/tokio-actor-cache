use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, interval};

use crate::tokio_cache::data_struct::HashSetState;
use crate::tokio_cache::error::TokioActorCacheError;
use crate::tokio_cache::expiration_policy::ExpirationPolicy;
use crate::tokio_cache::unbounded::cmd::HashSetCmd;

#[derive(Debug, Clone)]
pub struct HashSetCache<V> {
    pub tx: UnboundedSender<HashSetCmd<V>>
}

impl<V> HashSetCache<V>
where
    V: Clone
{
    pub async fn stop_replicating(&self) -> Result<(), TokioActorCacheError> {
        let stop_replicating_cmd = HashSetCmd::StopReplicating;
        self.tx
            .send(stop_replicating_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn replicate(&self, master: &Self) -> Result<(), TokioActorCacheError> {
        let replicate_cmd = HashSetCmd::Replicate { master: master.clone() };
        self.tx
            .send(replicate_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn ttl(&self, vals: &[V]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        let ttl_cmd = HashSetCmd::TTL { vals, resp_tx };
        self.tx
            .send(ttl_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn get_all(&self) -> Result<HashSet<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let get_all_cmd = HashSetCmd::GetAll { resp_tx };
        self.tx
            .send(get_all_cmd)
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn clear(&self) -> Result<(), TokioActorCacheError> {
        let clear_cmd = HashSetCmd::Clear;
        self.tx
            .send(clear_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn remove(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        let remove_cmd = HashSetCmd::Remove { vals, resp_tx };
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
        let contains_key_cmd = HashSetCmd::Contains { vals, resp_tx };
        self.tx
            .send(contains_key_cmd)
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
        let minsert_cmd = HashSetCmd::MInsert { vals, ex, nx };
        self.tx
            .send(minsert_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn insert(
        &self,
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    ) -> Result<(), TokioActorCacheError> {
        let insert_cmd = HashSetCmd::Insert { val, ex, nx };
        self.tx
            .send(insert_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(expiration_policy: ExpirationPolicy) -> Self
    where
        V: Debug + Clone + Eq + Hash + Send + 'static
    {
        let mut hm = match expiration_policy {
            ExpirationPolicy::LFU(capacity) | ExpirationPolicy::LRU(capacity) => {
                HashMap::<V, HashSetState>::with_capacity(capacity)
            },
            ExpirationPolicy::None => HashMap::<V, HashSetState>::new(),
        };
        let mut replica_of: Option<HashSetCache<V>> = None;

        let (tx, mut rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {

                        // Replicate master.
                        if let Some(ref master) = replica_of {
                            let (resp_tx, resp_rx) = oneshot::channel();
                            let get_all_raw_cmd = HashSetCmd::GetAllRaw { resp_tx };
                            if let Err(_) = master.tx.send(get_all_raw_cmd) {
                                eprintln!("the receiver dropped")
                            }
                            match resp_rx.await {
                                Ok(master_hm) => hm = master_hm,
                                Err(_) => eprintln!("the receiver dropped"),
                            }
                        }

                        // Invalidate cache.
                        hm.retain(|_k, state| match state.expiration {
                            Some(exp) => Instant::now() < exp,
                            None => true,
                        });

                        // Invalidate cache according to expiration policy.
                        match expiration_policy {
                            ExpirationPolicy::LFU(capacity) => {
                                let n_exceed = hm.len() - capacity;
                                if hm.len() > capacity {
                                    // Find the val with the minimum call_cnt (least frequently used).
                                    for _ in 0..n_exceed {
                                        if let Some(lfu_val) = hm
                                            .iter()
                                            .min_by_key(|(_, state)| state.call_cnt)
                                            .map(|(val, _)| val.clone())
                                        {
                                            hm.remove(&lfu_val);
                                        }
                                    }
                                }
                            },
                            ExpirationPolicy::LRU(capacity) => {
                                let n_exceed = hm.len() - capacity;
                                if hm.len() > capacity {
                                    // Find the val with the minimum last_accessed (least recently used).
                                    for _ in 0..n_exceed {
                                        if let Some(lru_val) = hm
                                            .iter()
                                            .min_by_key(|(_, state)| state.last_accessed)
                                            .map(|(val, _)| val.clone())
                                        {
                                            hm.remove(&lru_val);
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
                                    let val = hm.clone();

                                    if let Err(_) = resp_tx.send(val) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashSetCmd::<V>::TTL { vals, resp_tx } => {
                                    let ttl = vals.iter().map(|val| {
                                        hm.get_mut(&val).and_then(|state| {
                                            state.call_cnt += 1;
                                            state.last_accessed = Instant::now();
                                            state.expiration.and_then(|ex| {
                                                    ex.checked_duration_since(Instant::now())
                                            })
                                        })
                                    }).collect::<Vec<Option<Duration>>>();

                                    if let Err(_) = resp_tx.send(ttl) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashSetCmd::<V>::GetAll { resp_tx } => {
                                    let val = hm.clone().into_iter().map(|(val, mut state)| {
                                        state.call_cnt += 1;
                                        state.last_accessed = Instant::now();
                                        val
                                    }).collect::<HashSet<V>>();

                                    if let Err(_) = resp_tx.send(val) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashSetCmd::<V>::Clear => {
                                    hm.clear();
                                }
                                HashSetCmd::<V>::Remove { vals, resp_tx } => {
                                    let is_remove = vals.iter().map(|val| {
                                        match hm.remove(&val) {
                                            Some(_) => true,
                                            None => false,
                                        }
                                    }).collect::<Vec<bool>>();
                                    if let Err(_) = resp_tx.send(is_remove) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashSetCmd::<V>::Contains { vals, resp_tx } => {
                                    let is_contains_vals = vals.iter().map(|val| {

                                        // Get 'state' with 'val'.
                                        hm.get_mut(val).and_then(|state| {

                                            // incr 'call_cnt' by 1 and update 'last_accessed'.
                                            state.call_cnt += 1;
                                            state.last_accessed = Instant::now();

                                            Some(())
                                        });

                                        hm.contains_key(&val)
                                    }).collect::<Vec<bool>>();

                                    if let Err(_) = resp_tx.send(is_contains_vals) {
                                        println!("the receiver dropped");
                                    }
                                }
                                HashSetCmd::<V>::MInsert { vals, ex, nx } => {
                                    for ((val, ex), nx) in vals.into_iter().zip(ex).zip(nx) {
                                        let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                        let call_cnt = if nx == Some(true) {
                                            0
                                        } else {
                                            hm.get(&val).map_or(0, |v| v.call_cnt + 1)
                                        };
                                        let last_accessed = Instant::now();
                                        let state = HashSetState { expiration, call_cnt, last_accessed};
                                        if nx.is_some() && nx == Some(true) {
                                            hm.entry(val).or_insert(state);
                                        } else {
                                            hm.insert(val, state);
                                        }
                                    }
                                }
                                HashSetCmd::<V>::Insert { val, ex, nx } => {
                                    let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                    let call_cnt = if nx == Some(true) {
                                        0
                                    } else {
                                        hm.get(&val).map_or(0, |v| v.call_cnt + 1)
                                    };
                                    let last_accessed = Instant::now();
                                    let state = HashSetState { expiration, call_cnt, last_accessed };
                                    hm.insert(val, state);
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
