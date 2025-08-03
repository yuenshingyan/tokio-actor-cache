use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::time::Duration;
use tokio::sync::oneshot;

use crate::tokio_cache::bounded::cmd::VecCmd;
use crate::tokio_cache::bounded::vec::VecCache;
use crate::tokio_cache::compute::hash_id;
use crate::tokio_cache::error::TokioActorCacheError;
use crate::tokio_cache::expiration_policy::ExpirationPolicy;

#[derive(Debug, Clone)]
pub struct VecCacheCluster<V> {
    pub nodes: HashMap<u64, VecCache<V>>,
}

impl<V> VecCacheCluster<V>
where
    V: Clone + Debug + Eq + Hash + Send + 'static + Display,
{
    pub async fn try_ttl(&self, vals: &[V]) -> Result<Vec<Option<Duration>>, TokioActorCacheError> {
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
                .try_send(ttl_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
            // Cannot use extend since, results from other nodes might return empty vec.
            match resp_rx.await.map_err(|_| return TokioActorCacheError::Receive)?.first() {
                Some(ttl) => {
                    res.push(ttl.clone());
                },
                None => res.push(None),
            }
        }

        Ok(res)
    }

    pub async fn try_clear(&self) -> Result<(), TokioActorCacheError> {
        for node in self.nodes.values() {
            let clear_cmd = VecCmd::Clear;
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
            let remove_cmd = VecCmd::Remove {
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
                .try_send(VecCmd::Contains {
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

    pub async fn try_get_all(&self) -> Result<Vec<V>, TokioActorCacheError> {
        let mut res = Vec::new();
        for node in self.nodes.values() {
            let (resp_tx, resp_rx) = oneshot::channel();
            let get_all_cmd = VecCmd::GetAll { resp_tx };
            node.tx
                .try_send(get_all_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(resp_rx.await.map_err(|_| TokioActorCacheError::Receive)?);
        }

        Ok(res)
    }

    pub async fn try_mpush(
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
        for val in vals {
            let node = self.get_node(val.clone())?;
            node.tx
                .try_send(VecCmd::MPush {
                    vals: vec![val],
                    ex: ex.clone(),
                    nx: nx.clone(),
                })
                .map_err(|_| TokioActorCacheError::Send)?;
        }

        Ok(())
    }

    pub async fn try_push(
        &self,
        val: V,
        ex: Option<Duration>,
        nx: bool,
    ) -> Result<(), TokioActorCacheError> {
        let node = self.get_node(val.clone())?;
        let push_cmd = VecCmd::Push { val, ex, nx };
        node.tx
            .try_send(push_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

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
                .await
                .map_err(|_| TokioActorCacheError::Send)?;
            // Cannot use extend since, results from other nodes might return empty vec.
            match resp_rx.await.map_err(|_| return TokioActorCacheError::Receive)?.first() {
                Some(ttl) => {
                    res.push(ttl.clone());
                },
                None => res.push(None),
            }
        }

        Ok(res)
    }

    pub async fn clear(&self) -> Result<(), TokioActorCacheError> {
        for node in self.nodes.values() {
            let clear_cmd = VecCmd::Clear;
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
            let remove_cmd = VecCmd::Remove {
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
                .send(VecCmd::Contains {
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

    pub async fn get_all(&self) -> Result<Vec<V>, TokioActorCacheError> {
        let mut res = Vec::new();
        for node in self.nodes.values() {
            let (resp_tx, resp_rx) = oneshot::channel();
            let get_all_cmd = VecCmd::GetAll { resp_tx };
            node.tx
                .send(get_all_cmd)
                .await
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(resp_rx.await.map_err(|_| TokioActorCacheError::Receive)?);
        }

        Ok(res)
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
        for val in vals {
            let node = self.get_node(val.clone())?;
            node.tx
                .send(VecCmd::MPush {
                    vals: vec![val],
                    ex: ex.clone(),
                    nx: nx.clone(),
                })
                .await
                .map_err(|_| TokioActorCacheError::Send)?;
        }

        Ok(())
    }

    pub async fn push(
        &self,
        val: V,
        ex: Option<Duration>,
        nx: bool,
    ) -> Result<(), TokioActorCacheError> {
        let node = self.get_node(val.clone())?;
        let push_cmd = VecCmd::Push { val, ex, nx };
        node.tx
            .send(push_cmd)
            .await
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(expiration_policy: ExpirationPolicy, buffer: usize, n_node: u64) -> Self {
        let mut nodes = HashMap::new();
        for i in 0..n_node {
            let vec_cache = VecCache::<V>::new(expiration_policy, buffer).await;
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
