use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::time::Duration;
// use std::vec;
use tokio::sync::oneshot;

use crate::tokio_cache::compute::hash_id;
use crate::tokio_cache::error::TokioActorCacheError;
use crate::tokio_cache::expiration_policy::ExpirationPolicy;
use crate::tokio_cache::unbounded::cmd::HashSetCmd;
use crate::tokio_cache::unbounded::hs::HashSetCache;

#[derive(Debug, Clone)]
pub struct HashSetCacheCluster<V> {
    pub nodes: HashMap<u64, HashSetCache<V>>,
}

impl<V> HashSetCacheCluster<V>
where
    V: Clone + Debug + Eq + Hash + Send + 'static + Display,
{
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
                .send(HashSetCmd::MInsert {
                    vals: vec![val],
                    ex: ex.clone(),
                    nx: nx.clone(),
                })
                .map_err(|_| TokioActorCacheError::Send)?;
        }

        Ok(())
    }

    pub async fn insert(
        &self,
        val: V,
        ex: Option<Duration>,
        nx: bool,
    ) -> Result<(), TokioActorCacheError> {
        let node = self.get_node(val.clone())?;
        node.tx
            .send(HashSetCmd::Insert { val, ex, nx })
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(expiration_policy: ExpirationPolicy, n_node: u64) -> Self {
        let mut nodes = HashMap::new();
        for i in 0..n_node {
            let hs_cache = HashSetCache::<V>::new(expiration_policy).await;
            nodes.insert(i, hs_cache);
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
