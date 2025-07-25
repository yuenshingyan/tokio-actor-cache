use std::time::Duration;

use crate::tokio_actor_cache::{hm::HashMapCache, hs::HashSetCache, vec::VecCache};

pub mod tokio_actor_cache {
    mod data_struct;
    pub mod error;
    pub mod hm;
    pub mod hs;
    pub mod vec;
}
pub mod unittests {
    pub mod hm;
    pub mod hs;
    pub mod vec;
}

#[tokio::main]
async fn main() {
    let hs_cache = HashSetCache::new(32).await;
    hs_cache
        .insert(10, Some(Duration::from_secs(5)), None)
        .await
        .unwrap();
    let ttl = hs_cache.ttl(&[10]).await.unwrap();
    println!("{:?}", ttl);

    let vec_cache = VecCache::new(32).await;
    vec_cache
        .push(10, Some(Duration::from_secs(5)), None)
        .await
        .unwrap();
    let val = vec_cache.ttl(&[10]).await.unwrap();
    println!("{:?}", val);

    // TODO: Add MSET, MGET, TTL ...

    // High-Value Functionality
    // 1. Cache Expiration Policies
    // Support multiple expiration policies (e.g., Least Recently Used, Least Frequently Used, all-keys-random) to control which objects are evicted under memory pressure.
    // Allow setting global and per-key TTL (time to live).

    // 2. Advanced Caching Patterns
    // Cache-Aside Pattern: Facilitate application-side loading and updating of cache entries.
    // Read-Through and Write-Through: Automate cache population and updates on misses/hits and writes, reducing logic in client code and keeping cached data consistent.
    // Write-Behind: Optionally support background updates to the underlying data store, batching or delaying disk/database writes.

    // 3. Persistence Support
    // Add options to persist cached data to disk, e.g., periodic snapshots or append-only logs, to recover from crashes or restarts.

    // 4. Cache Invalidation and Pub/Sub
    // Implement mechanisms to propagate invalidation messages (Pub/Sub) to synchronize cache state across multiple replicas or instances.

    // 5. Metrics and Monitoring
    // Track cache hits/misses, evictions, memory usage, and performance statistics for effective monitoring and tuning.

    // 6. Cluster and Distribution Support
    // Facilitate sharding or consistent hashing to distribute cache data across multiple nodes for scalability.
    // Add partitioning and replication to support high availability and resilience.

    // 7. Near Cache (Local Caching)
    // Implement an optional per-process or per-node local cache to speed up repetitive reads and reduce network hops, mirroring near cache designs in high-performance systems.
}
