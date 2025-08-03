#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use crate::tokio_cache::{expiration_policy::ExpirationPolicy, unbounded::hs::HashSetCache};

    #[tokio::test]
    async fn test_expiration_policy_lru() {
        let expiration_policy = ExpirationPolicy::LRU(1);
        let hs_cache = HashSetCache::<i32>::new(expiration_policy).await;
        hs_cache.insert(1, None, false).await.unwrap();
        hs_cache.insert(2, None, false).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let hs = hs_cache.get_all().await.unwrap();
        assert_eq!(HashSet::from([(2)]), hs);
    }

    #[tokio::test]
    async fn test_expiration_policy_lfu() {
        let expiration_policy = ExpirationPolicy::LFU(1);
        let hs_cache = HashSetCache::<i32>::new(expiration_policy).await;
        hs_cache.insert(1, None, false).await.unwrap();
        hs_cache.insert(1, None, false).await.unwrap();
        hs_cache.insert(2, None, false).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let hs = hs_cache.get_all().await.unwrap();
        assert_eq!(HashSet::from([(1)]), hs);
    }

    #[tokio::test]
    async fn test_replicated_data_persist() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cluster1 = HashSetCache::<i32>::new(expiration_policy).await;
        let hs_cluster2 = HashSetCache::<i32>::new(expiration_policy).await;
        hs_cluster2.replicate(&hs_cluster1).await.unwrap();

        hs_cluster1.insert(1, None, false).await.unwrap();

        let val_1 = hs_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        hs_cluster2.stop_replicating().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hs_cluster2.get_all().await.unwrap();

        assert_eq!(val_1, val_2);
    }

    #[tokio::test]
    async fn test_stop_replicating() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cluster1 = HashSetCache::<i32>::new(expiration_policy).await;
        let hs_cluster2 = HashSetCache::<i32>::new(expiration_policy).await;
        hs_cluster2.replicate(&hs_cluster1).await.unwrap();

        hs_cluster1.insert(1, None, false).await.unwrap();

        let val_1 = hs_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        hs_cluster2.stop_replicating().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hs_cluster2.get_all().await.unwrap();

        assert_eq!(val_1, val_2);

        hs_cluster1.insert(10, None, false).await.unwrap();

        let val_1 = hs_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(val_1 != val_2);
    }

    #[tokio::test]
    async fn test_replicate() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cluster1 = HashSetCache::<i32>::new(expiration_policy).await;
        let hs_cluster2 = HashSetCache::<i32>::new(expiration_policy).await;
        hs_cluster2.replicate(&hs_cluster1).await.unwrap();

        hs_cluster1.insert(1, None, false).await.unwrap();

        let val_1 = hs_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hs_cluster2.get_all().await.unwrap();

        assert_eq!(val_1, val_2);
    }

    #[tokio::test]
    async fn test_ttl() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy).await;
        hs_cache
            .insert(10, Some(Duration::from_secs(1)), false)
            .await
            .unwrap();
        let ttl = hs_cache.ttl(&[10, 20]).await.unwrap();
        println!("{:?}", ttl);
        assert!(Some(Duration::from_secs(1)) > ttl[0]);
        assert_eq!(ttl[1], None);
    }

    #[tokio::test]
    async fn test_clear() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy).await;
        hs_cache.insert(10, None, false).await.unwrap();
        hs_cache.insert(20, None, false).await.unwrap();
        hs_cache.insert(30, None, false).await.unwrap();
        let hs = hs_cache.get_all().await.unwrap();
        assert_eq!(hs, HashSet::from([10, 20, 30]));
        hs_cache.clear().await.unwrap();
        let hs = hs_cache.get_all().await.unwrap();
        assert_eq!(hs.is_empty(), true);
    }

    #[tokio::test]
    async fn test_remove() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy).await;
        hs_cache
            .minsert(&[10, 20, 30], &[None, None, None], &[false, false, false])
            .await
            .unwrap();
        let vals = hs_cache.remove(&[10, 20, 30, 40]).await.unwrap();
        assert_eq!(vals, vec![true, true, true, false]);
    }

    #[tokio::test]
    async fn test_contains() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy).await;
        hs_cache.insert(10, None, false).await.unwrap();
        let vals = hs_cache.contains(&[10]).await.unwrap();
        assert_eq!(vals, vec![true]);
    }

    #[tokio::test]
    async fn test_minsert_ex() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy).await;
        hs_cache
            .minsert(
                &[10, 20, 30],
                &[
                    Some(Duration::from_secs(1)),
                    Some(Duration::from_secs(1)),
                    Some(Duration::from_secs(1)),
                ],
                &[false, false, false],
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let vals = hs_cache.get_all().await.unwrap();
        assert_eq!(vals, HashSet::<i32>::new());
    }

    #[tokio::test]
    async fn test_minsert() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy).await;
        hs_cache
            .minsert(&[10, 20, 30], &[None, None, None], &[false, false, false])
            .await
            .unwrap();
        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10, 20, 30]));
    }

    #[tokio::test]
    async fn test_insert_ex() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy).await;
        hs_cache.insert(10, None, false).await.unwrap();
        hs_cache
            .insert(20, Some(Duration::from_secs(1)), false)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10]));
    }

    #[tokio::test]
    async fn test_insert() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy).await;
        hs_cache.insert(10, None, false).await.unwrap();
        hs_cache.insert(20, None, false).await.unwrap();
        hs_cache.insert(30, None, false).await.unwrap();
        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10, 20, 30]));
    }
}
