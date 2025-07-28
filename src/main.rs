pub mod tokio_cache {
    pub mod bounded {
        pub mod hm;
        pub mod hs;
        pub mod vec;
    }
    // pub mod unbounded {

    // }
    mod compute;
    mod data_struct;
    pub mod error;
}
pub mod unittests {
    pub mod hm;
    pub mod hm_cluster;
    pub mod hs;
    pub mod hs_cluster;
    pub mod vec;
    pub mod vec_cluster;
}

use std::time::Duration;

use crate::tokio_cache::bounded::hm::HashMapCacheCluster;

#[tokio::main]
async fn main() {
    let hm_cluster = HashMapCacheCluster::<&str, i32>::new(32, 3).await;

    // let key = "a";
    // let val = 10;
    // let ex = None;
    // let nx = None;

    // hm_cluster.insert(key, val, ex, nx).await.unwrap();
    // let val = hm_cluster.get(key).await.unwrap();
    // println!("{:?}", val);

    hm_cluster
        .minsert(
            &["a", "b", "c"],
            &[10, 20, 30],
            &[None, None, None],
            &[None, None, None],
        )
        .await
        .unwrap();
    // let vals = hm_cluster.mget(&["a", "b", "c"]).await.unwrap();
    // println!("{:?}", vals);

    // let keys = hm_cluster.contains_key(&["a", "b", "c"]).await.unwrap();
    // println!("{:?}", keys);

    // let vals = hm_cluster.remove(&["a", "b", "c"]).await.unwrap();
    // println!("{:?}", vals);

    // hm_cluster.clear().await.unwrap();
    // let vals = hm_cluster.mget(&["a", "b", "c"]).await.unwrap();
    // println!("{:?}", vals);

    // let vals = hm_cluster.get_all().await.unwrap();
    // println!("{:?}", vals);

    hm_cluster
        .minsert(
            &["a", "b", "c"],
            &[10, 20, 30],
            &[
                Some(Duration::from_secs(1)),
                Some(Duration::from_secs(1)),
                Some(Duration::from_secs(1)),
            ],
            &[None, None, None],
        )
        .await
        .unwrap();
    let ttl = hm_cluster.ttl(&["a", "b", "c"]).await.unwrap();
    println!("{:?}", ttl);

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
