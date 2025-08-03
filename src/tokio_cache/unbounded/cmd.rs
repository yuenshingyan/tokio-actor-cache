use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::oneshot;

use crate::tokio_cache::data_struct::{HashSetState, ValueEx};
use crate::tokio_cache::unbounded::hm::HashMapCache;
use crate::tokio_cache::unbounded::hs::HashSetCache;
use crate::tokio_cache::unbounded::vec::VecCache;

#[derive(Debug)]
pub enum VecCmd<V> {
    StopReplicating,
    IsReplica {
        resp_tx: oneshot::Sender<bool>,
    },
    Replicate {
        master: VecCache<V>,
    },
    GetAllRaw {
        resp_tx: oneshot::Sender<Vec<ValueEx<V>>>,
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
        resp_tx: oneshot::Sender<HashMap<V, HashSetState>>,
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

#[derive(Debug)]
pub enum HashMapCmd<K, V> {
    StopReplicating,
    IsReplica {
        resp_tx: oneshot::Sender<bool>,
    },
    Replicate {
        master: HashMapCache<K, V>,
    },
    GetAllRaw {
        resp_tx: oneshot::Sender<HashMap<K, ValueEx<V>>>,
    },
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