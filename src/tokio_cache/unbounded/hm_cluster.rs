use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::time::Duration;
use tokio::sync::oneshot;

use crate::tokio_cache::compute::hash_id;
use crate::tokio_cache::error::TokioActorCacheError;
use crate::tokio_cache::option::ExpirationPolicy;
use crate::tokio_cache::unbounded::cmd::HashMapCmd;
use crate::tokio_cache::unbounded::hm::HashMapCache;

#[derive(Debug, Clone)]
pub struct HashMapCacheCluster<K, V> {
    pub nodes: HashMap<u64, HashMapCache<K, V>>,
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
                .send(ttl_cmd)
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
                .send(get_all_cmd)
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
            let clear_cmd = HashMapCmd::Clear;
            node.tx
                .send(clear_cmd)
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
                .send(contains_key_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
            res.extend(
                resp_rx
                    .await
                    .map_err(|_| return TokioActorCacheError::Receive)?,
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
                .send(mget_cmd)
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

        for (key, val) in keys.into_iter().zip(vals).clone() {
            let minsert_cmd = HashMapCmd::MInsert {
                keys: vec![key.clone()],
                vals: vec![val.clone()],
                ex: ex.clone(),
                nx: nx.clone(),
            };
            let node = self.get_node(key)?;
            node.tx
                .send(minsert_cmd)
                .map_err(|_| TokioActorCacheError::Send)?;
        }

        Ok(())
    }

    pub async fn get(&self, key: K) -> Result<Option<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let get_cmd = HashMapCmd::Get {
            key: key.clone(),
            resp_tx,
        };
        let node = self.get_node(key)?;
        node.tx
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
        let insert_cmd = HashMapCmd::Insert {
            key: key.clone(),
            val,
            ex,
            nx,
        };
        let node = self.get_node(key)?;
        node.tx
            .send(insert_cmd)
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(expiration_policy: ExpirationPolicy, n_node: u64) -> Self {
        let mut nodes = HashMap::new();
        for i in 0..n_node {
            let hm_cache = HashMapCache::<K, V>::new(expiration_policy).await;
            nodes.insert(i, hm_cache);
        }
        Self { nodes }
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
