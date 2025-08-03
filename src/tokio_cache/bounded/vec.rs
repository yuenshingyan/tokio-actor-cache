use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;

use crate::tokio_cache::bounded::cmd::VecCmd;
use crate::tokio_cache::data_struct::ValueEx;
use crate::tokio_cache::error::TokioActorCacheError;
use crate::tokio_cache::expiration_policy::ExpirationPolicy;

use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, interval};

#[derive(Debug, Clone)]
pub struct VecCache<V> {
    pub tx: Sender<VecCmd<V>>,
}

impl<V> VecCache<V>
where
    V: Clone,
{
    pub async fn try_stop_replicating(&self) -> Result<(), TokioActorCacheError> {
        let stop_replicating_cmd = VecCmd::StopReplicating;
        self.tx
            .try_send(stop_replicating_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn try_replicate(&self, master: &Self) -> Result<(), TokioActorCacheError> {
        let replicate_cmd = VecCmd::Replicate { master: master.clone() };
        self.tx
            .try_send(replicate_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn try_ttl(&self, vals: &[V]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
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

    pub async fn try_clear(&self) -> Result<(), TokioActorCacheError> {
        let clear_cmd = VecCmd::Clear;
        self.tx
            .try_send(clear_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn try_remove(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
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

    pub async fn try_contains(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        self.tx
            .try_send(VecCmd::Contains { vals, resp_tx })
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn try_get_all(&self) -> Result<Vec<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .try_send(VecCmd::GetAll { resp_tx })
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx.await.map_err(|_| TokioActorCacheError::Receive)
    }

    pub async fn try_mpush(
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
            .try_send(VecCmd::MPush { vals, ex, nx })
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn try_push(
        &self,
        val: V,
        ex: Option<Duration>,
        nx: Option<bool>,
    ) -> Result<(), TokioActorCacheError> {
        self.tx
            .try_send(VecCmd::Push { val, ex, nx })
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn stop_replicating(&self) -> Result<(), TokioActorCacheError> {
        let stop_replicating_cmd = VecCmd::StopReplicating;
        self.tx
            .send(stop_replicating_cmd)
            .await
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn replicate(&self, master: &Self) -> Result<(), TokioActorCacheError> {
        let replicate_cmd = VecCmd::Replicate { master: master.clone() };
        self.tx
            .send(replicate_cmd)
            .await
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn ttl(&self, vals: &[V]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        let ttl_cmd = VecCmd::TTL { vals, resp_tx };
        self.tx
            .send(ttl_cmd)
            .await
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn clear(&self) -> Result<(), TokioActorCacheError> {
        let clear_cmd = VecCmd::Clear;
        self.tx
            .send(clear_cmd)
            .await
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn remove(&self, vals: &[V]) -> Result<Vec<bool>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let vals = vals.to_vec();
        let remove_cmd = VecCmd::Remove { vals, resp_tx };
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
            .send(VecCmd::Contains { vals, resp_tx })
            .await
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx
            .await
            .map_err(|_| return TokioActorCacheError::Receive)
    }

    pub async fn get_all(&self) -> Result<Vec<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(VecCmd::GetAll { resp_tx })
            .await
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
            .await
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
            .await
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(expiration_policy: ExpirationPolicy, buffer: usize) -> Self
    where
        V: Clone + Eq + Hash + Debug + Send + 'static,
    {
        let mut vec = match expiration_policy {
            ExpirationPolicy::LFU(capacity) | ExpirationPolicy::LRU(capacity) => {
                Vec::<ValueEx<V>>::with_capacity(capacity)
            },
            ExpirationPolicy::None => Vec::<ValueEx<V>>::new(),
        };
        let mut replica_of: Option<VecCache<V>> = None;

        let (tx, mut rx) = mpsc::channel(buffer);

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {

                        // Replicate master.
                        if let Some(ref master) = replica_of {
                            let (resp_tx, resp_rx) = oneshot::channel();
                            let get_all_raw_cmd = VecCmd::GetAllRaw { resp_tx };
                            if let Err(_) = master.tx.send(get_all_raw_cmd).await {
                                eprintln!("the receiver dropped")
                            }
                            match resp_rx.await {
                                Ok(master_vec) => vec = master_vec,
                                Err(_) => eprintln!("the receiver dropped"),
                            }
                        }

                        // Expire key-val.
                        vec.retain(|val_ex: &ValueEx<V>| match val_ex.expiration {
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
                                        .min_by_key(|(_, val_ex)| val_ex.call_cnt)
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
                                        .min_by_key(|(_, val_ex)| val_ex.last_accessed)
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
                                    let mut ttl = Vec::with_capacity(vals.len());
                                    for val in &vals {
                                        for val_ex in &mut vec {
                                            if val_ex.val == *val {
                                                val_ex.call_cnt += 1;
                                                val_ex.last_accessed = Instant::now();
                                                ttl.push(
                                                    val_ex.expiration.and_then(|ex| {
                                                        ex.checked_duration_since(Instant::now())
                                                    })
                                                );
                                            } else {
                                                ttl.push(None);
                                            }
                                        }
                                    }

                                    if let Err(_) = resp_tx.send(ttl) {
                                        println!("the receiver dropped");
                                    }
                                }
                                VecCmd::<V>::Clear => {
                                    vec.clear();
                                }
                                VecCmd::<V>::Remove { vals, resp_tx } => {
                                    let mut found_set = HashSet::with_capacity(vals.len());
                                    for val_ex in &mut vec {
                                        if vals.contains(&val_ex.val) {
                                            val_ex.call_cnt += 1;
                                            val_ex.last_accessed = Instant::now();
                                            found_set.insert(val_ex.val.clone());
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
                                    for val_ex in &mut vec {
                                        if vals.contains(&val_ex.val) {
                                            val_ex.call_cnt += 1;
                                            val_ex.last_accessed = Instant::now();
                                            found_set.insert(val_ex.val.clone());
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
                                    let vals = vec.iter_mut().map(|val_ex| {
                                        val_ex.call_cnt += 1;
                                        val_ex.last_accessed = Instant::now();
                                        val_ex.val.clone()
                                    }).collect::<Vec<V>>();

                                    if let Err(_) = resp_tx.send(vals) {
                                        println!("the receiver dropped");
                                    }
                                }
                                VecCmd::<V>::MPush { vals, ex, nx } => {
                                    for ((val, ex), nx) in vals.into_iter().zip(ex).zip(nx) {
                                        let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                        let call_cnt = if nx == Some(true) {
                                            0
                                        } else {
                                            let mut cnt = 0;
                                            for val_ex in &vec {
                                                if val_ex.val == val {
                                                    cnt = val_ex.call_cnt + 1;
                                                    break;
                                                }
                                            }

                                            cnt
                                        };
                                        let last_accessed = Instant::now();
                                        let val_ex = ValueEx { val, expiration, call_cnt, last_accessed };
                                        if nx.is_some() && nx == Some(true) && !vec.contains(&val_ex) {
                                            vec.push(val_ex);
                                        } else {
                                            vec.push(val_ex);
                                        }
                                    }
                                }
                                VecCmd::<V>::Push { val, ex, nx } => {
                                    let expiration = ex.and_then(|d| Some(Instant::now() + d));
                                    let call_cnt = if nx == Some(true) {
                                        0
                                    } else {
                                        let mut cnt = 0;
                                        for val_ex in &vec {
                                            if val_ex.val == val {
                                                cnt = val_ex.call_cnt + 1;
                                                break;
                                            }
                                        }

                                        cnt
                                    };
                                    let last_accessed = Instant::now();
                                    let val_ex = ValueEx { val, expiration, call_cnt, last_accessed };
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
