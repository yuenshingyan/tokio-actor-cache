use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, interval};

use crate::tokio_cache::data_struct::ValueWithState;
use crate::tokio_cache::error::TokioActorCacheError;
use crate::tokio_cache::expiration_policy::ExpirationPolicy;
use crate::tokio_cache::unbounded::cmd::VecCmd;

#[derive(Debug, Clone)]
pub struct VecCache<V> {
    pub tx: UnboundedSender<VecCmd<V>>,
}

impl<V> VecCache<V>
where
    V: Clone,
{
    pub async fn stop_replicating(&self) -> Result<(), TokioActorCacheError> {
        let stop_replicating_cmd = VecCmd::StopReplicating;
        self.tx
            .send(stop_replicating_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn replicate(&self, master: &Self) -> Result<(), TokioActorCacheError> {
        let replicate_cmd = VecCmd::Replicate { master: master.clone() };
        self.tx
            .send(replicate_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

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
        nx: &[bool],
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
        nx: bool,
    ) -> Result<(), TokioActorCacheError> {
        self.tx
            .send(VecCmd::Push { val, ex, nx })
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(expiration_policy: ExpirationPolicy) -> Self
    where
        V: Clone + Eq + Hash + Debug + Send + 'static,
    {
        let mut vec = match expiration_policy {
            ExpirationPolicy::LFU(capacity) | ExpirationPolicy::LRU(capacity) => {
                Vec::<ValueWithState<V>>::with_capacity(capacity)
            },
            ExpirationPolicy::None => Vec::<ValueWithState<V>>::new(),
        };
        let mut replica_of: Option<VecCache<V>> = None;

        let (tx, mut rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {

                        // Replicate master.
                        if let Some(ref master) = replica_of {
                            let (resp_tx, resp_rx) = oneshot::channel();
                            let get_all_raw_cmd = VecCmd::GetAllRaw { resp_tx };
                            if let Err(_) = master.tx.send(get_all_raw_cmd) {
                                eprintln!("the receiver dropped")
                            }
                            match resp_rx.await {
                                Ok(master_vec) => vec = master_vec,
                                Err(_) => eprintln!("the receiver dropped"),
                            }
                        }

                        // Expire key-val.
                        vec.retain(|val_with_state: &ValueWithState<V>| match val_with_state.expiration {
                            Some(exp) => Instant::now() < exp,
                            None => true,
                        });

                        // Invalidate cache according to expiration policy.
                        match expiration_policy {
                            ExpirationPolicy::LFU(capacity) => {
                                if vec.len() > capacity {
                                     // Find the val with the minimum call_cnt (least frequently used).
                                    if let Some(lfu_val_idx) = vec
                                        .iter()
                                        .enumerate()
                                        .min_by_key(|(_, val_with_state)| val_with_state.call_cnt)
                                        .map(|(i, _)| i)
                                    {
                                        vec.remove(lfu_val_idx);
                                    }
                                }
                            },
                            ExpirationPolicy::LRU(capacity) => {
                                if vec.len() > capacity {
                                    // Find the val with the minimum last_accessed (least recently used).
                                    if let Some(lru_val_idx) = vec
                                        .iter()
                                        .enumerate()
                                        .min_by_key(|(_, val_with_state)| val_with_state.last_accessed)
                                        .map(|(i, _)| i)
                                    {
                                        vec.remove(lru_val_idx);
                                    }
                                }
                            },
                            ExpirationPolicy::None => (),
                        };
                    }
                    command = rx.recv() => {
                        if let Some(cmd) = command {
                            match cmd {
                                VecCmd::<V>::StopReplicating => {
                                    replica_of = None;
                                }
                                VecCmd::<V>::IsReplica { resp_tx } => {
                                    let is_replica = replica_of.is_some();
                                    if let Err(_) = resp_tx.send(is_replica) {
                                        println!("the receiver dropped");
                                    }
                                }
                                VecCmd::<V>::Replicate { master } => {
                                    replica_of = Some(master);
                                }
                                VecCmd::<V>::GetAllRaw { resp_tx } => {
                                    let val = vec.clone();
                                    if let Err(_) = resp_tx.send(val) {
                                        println!("the receiver dropped");
                                    }
                                }
                                VecCmd::<V>::TTL { vals, resp_tx } => {
                                    let ttl = vals.iter().map(|val| {
                                        for val_with_state in &mut vec {
                                            if val_with_state.val == *val {
                                                val_with_state.call_cnt += 1;
                                                val_with_state.last_accessed = Instant::now();
                                                return val_with_state.expiration.and_then(|ex| {
                                                    ex.checked_duration_since(Instant::now())
                                                })
                                            }
                                        }

                                        None
                                    }).collect::<Vec<Option<Duration>>>();

                                    if let Err(_) = resp_tx.send(ttl) {
                                        println!("the receiver dropped");
                                    }
                                }
                                VecCmd::<V>::Clear => {
                                    vec.clear();
                                }
                                VecCmd::<V>::Remove { vals, resp_tx } => {
                                    let mut found_set = HashSet::with_capacity(vals.len());
                                    for val_with_state in &mut vec {
                                        if vals.contains(&val_with_state.val) {
                                            val_with_state.call_cnt += 1;
                                            val_with_state.last_accessed = Instant::now();
                                            found_set.insert(val_with_state.val.clone());
                                        }
                                    }
                                    let is_exist = vals.into_iter()
                                        .map(|val| found_set.contains(&val))
                                        .collect::<Vec<bool>>();

                                    if let Err(_) = resp_tx.send(is_exist) {
                                        println!("the receiver dropped");
                                    }
                                }
                                VecCmd::<V>::Contains { vals, resp_tx } => {
                                    let mut found_set = HashSet::new();
                                    for val_with_state in &mut vec {
                                        if vals.contains(&val_with_state.val) {
                                            val_with_state.call_cnt += 1;
                                            val_with_state.last_accessed = Instant::now();
                                            found_set.insert(val_with_state.val.clone());
                                        }
                                    }
                                    let is_exist = vals.into_iter()
                                        .map(|val| found_set.contains(&val))
                                        .collect::<Vec<bool>>();

                                    if let Err(_) = resp_tx.send(is_exist) {
                                        println!("the receiver dropped");
                                    }
                                }
                                VecCmd::<V>::GetAll { resp_tx } => {
                                    let vals = vec.iter_mut().map(|val_with_state| {
                                        val_with_state.call_cnt += 1;
                                        val_with_state.last_accessed = Instant::now();
                                        val_with_state.val.clone()
                                    }).collect::<Vec<V>>();

                                    if let Err(_) = resp_tx.send(vals) {
                                        println!("the receiver dropped");
                                    }
                                }
                                VecCmd::<V>::MPush { vals, ex, nx } => {
                                    // TODO: make other for loop => iter??
                                    for ((val, ex), nx) in vals.into_iter().zip(ex).zip(nx) {
                                        let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                        let last_accessed = Instant::now();
                                        
                                        match (vec.iter().find(|val_ex| val_ex.val == val), nx) {
                                            (Some(val_with_state), false) => {
                                                let call_cnt = val_with_state.call_cnt + 1;
                                                let val_with_state = ValueWithState { 
                                                    val, 
                                                    expiration, 
                                                    call_cnt, 
                                                    last_accessed,
                                                };
                                                vec.push(val_with_state);
                                            },
                                            (None, true) | (None, false) => {
                                                let call_cnt = 0;
                                                let val_with_state = ValueWithState { 
                                                    val, 
                                                    expiration, 
                                                    call_cnt, 
                                                    last_accessed,
                                                };
                                                vec.push(val_with_state);
                                            },
                                            _ => (),
                                        }
                                    }
                                }
                                VecCmd::<V>::Push { val, ex, nx } => {
                                    let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                    let last_accessed = Instant::now();
                                    
                                    match (vec.iter().find(|val_ex| val_ex.val == val), nx) {
                                        (Some(val_with_state), false) => {
                                            let call_cnt = val_with_state.call_cnt + 1;
                                            let val_with_state = ValueWithState { 
                                                val, 
                                                expiration, 
                                                call_cnt, 
                                                last_accessed,
                                            };
                                            vec.push(val_with_state);
                                        },
                                        (None, true) | (None, false) => {
                                            let call_cnt = 0;
                                            let val_with_state = ValueWithState { 
                                                val, 
                                                expiration, 
                                                call_cnt, 
                                                last_accessed,
                                            };
                                            vec.push(val_with_state);
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
