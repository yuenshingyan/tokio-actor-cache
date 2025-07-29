use std::time::Duration;

use crate::{tokio_cache::unbounded::hm::HashMapCache, unittests::bounded::hm_cluster};

pub mod tokio_cache {
    pub mod bounded {
        pub mod hm;
        pub mod hs;
        pub mod vec;
    }
    pub mod unbounded {
        pub mod hm;
        pub mod hs;
        pub mod vec;
    }
    mod compute;
    mod data_struct;
    pub mod error;
}
pub mod unittests {
    pub mod bounded {
        pub mod hm;
        pub mod hm_cluster;
        pub mod hs;
        pub mod hs_cluster;
        pub mod vec;
        pub mod vec_cluster;
    }
    pub mod unbounded {
        pub mod hm;
        pub mod hm_cluster;
        pub mod hs;
        pub mod hs_cluster;
        pub mod vec;
        pub mod vec_cluster;
    }
}

#[tokio::main]
async fn main() {
    let hm_cluster1 = HashMapCache::<&str, i32>::new().await;
    let hm_cluster2 = HashMapCache::<&str, i32>::new().await;
    hm_cluster2.replicate(&hm_cluster1).await.unwrap();

    hm_cluster1.insert("a", 1, None, None).await.unwrap();

    let val_1 = hm_cluster1.get("a").await.unwrap();
    println!("{:?}", val_1);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let val_2 = hm_cluster2.get("a").await.unwrap();
    println!("{:?}", val_2);

    hm_cluster2.stop_replicating().await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    let val_2 = hm_cluster2.get("a").await.unwrap();
    println!("{:?}", val_2);

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
