use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;

use crate::tokio_actor_cache::data_struct::ValueEx;
use crate::tokio_actor_cache::error::TokioActorCacheError;

use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, interval};

#[derive(Debug)]
pub enum VecCmd<V> {
    Get { resp_tx: oneshot::Sender<Vec<V>> },
    Push { val: V, duration: Option<Duration> },
}

#[derive(Debug, Clone)]
pub struct VecCache<V> {
    pub tx: Sender<VecCmd<V>>,
}

impl<V> VecCache<V> {
    pub async fn get_all(&self) -> Result<Vec<V>, TokioActorCacheError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(VecCmd::Get { resp_tx })
            .await
            .map_err(|_| TokioActorCacheError::Send)?;
        resp_rx.await.map_err(|_| TokioActorCacheError::Receive)
    }

    pub async fn push(
        &self,
        val: V,
        duration: Option<Duration>,
    ) -> Result<(), TokioActorCacheError> {
        self.tx
            .send(VecCmd::Push { val, duration })
            .await
            .map_err(|_| TokioActorCacheError::Send)
    }

    pub async fn new(buffer: usize) -> Self
    where
        V: Clone + Eq + Hash + Debug + Send + std::marker::Send + 'static,
    {
        let mut vec = Vec::new();

        let (tx, mut rx) = mpsc::channel(buffer);

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Expire key-val.
                        vec.retain(|val_ex: &ValueEx<V>| match val_ex.expiration {
                            Some(exp) => Instant::now() < exp,
                            None => true,
                        });
                    }
                    command = rx.recv() => {
                        if let Some(cmd) = command {
                            match cmd {
                                VecCmd::<V>::Push { val, duration } => {
                                    let expiration = duration.and_then(|d| Some(Instant::now() + d));
                                    let val_ex = ValueEx { val, expiration };
                                    vec.push(val_ex);
                                }
                                VecCmd::<V>::Get { resp_tx } => {
                                    let val = vec
                                        .clone()
                                        .into_iter()
                                        .map(|val_ex| val_ex.val)
                                        .collect::<Vec<V>>();
                                    if let Err(_) = resp_tx.send(val) {
                                        println!("the receiver dropped");
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
