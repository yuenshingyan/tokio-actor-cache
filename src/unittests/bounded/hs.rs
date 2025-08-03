#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use crate::tokio_cache::{bounded::hs::HashSetCache, expiration_policy::ExpirationPolicy};

    #[tokio::test]
    async fn test_try_replicated_data_persist() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = HashSetCache::<i32>::new(expiration_policy, 32).await;
        let hm_cluster2 = HashSetCache::<i32>::new(expiration_policy, 32).await;
        hm_cluster2.try_replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.insert(1, None, None).await.unwrap();

        let val_1 = hm_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        hm_cluster2.try_stop_replicating().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get_all().await.unwrap();

        assert_eq!(val_1, val_2);
    }

    #[tokio::test]
    async fn test_try_stop_replicating() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = HashSetCache::<i32>::new(expiration_policy, 32).await;
        let hm_cluster2 = HashSetCache::<i32>::new(expiration_policy, 32).await;
        hm_cluster2.try_replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.insert(1, None, None).await.unwrap();

        let val_1 = hm_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        hm_cluster2.try_stop_replicating().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get_all().await.unwrap();

        assert_eq!(val_1, val_2);

        hm_cluster1.insert(10, None, None).await.unwrap();

        let val_1 = hm_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(val_1 != val_2);
    }

    #[tokio::test]
    async fn test_try_replicate() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = HashSetCache::<i32>::new(expiration_policy, 32).await;
        let hm_cluster2 = HashSetCache::<i32>::new(expiration_policy, 32).await;
        hm_cluster2.try_replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.insert(1, None, None).await.unwrap();

        let val_1 = hm_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get_all().await.unwrap();

        assert_eq!(val_1, val_2);
    }

    #[tokio::test]
    async fn test_replicated_data_persist() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = HashSetCache::<i32>::new(expiration_policy, 32).await;
        let hm_cluster2 = HashSetCache::<i32>::new(expiration_policy, 32).await;
        hm_cluster2.replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.insert(1, None, None).await.unwrap();

        let val_1 = hm_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        hm_cluster2.stop_replicating().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get_all().await.unwrap();

        assert_eq!(val_1, val_2);
    }

    #[tokio::test]
    async fn test_stop_replicating() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = HashSetCache::<i32>::new(expiration_policy, 32).await;
        let hm_cluster2 = HashSetCache::<i32>::new(expiration_policy, 32).await;
        hm_cluster2.replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.insert(1, None, None).await.unwrap();

        let val_1 = hm_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        hm_cluster2.stop_replicating().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get_all().await.unwrap();

        assert_eq!(val_1, val_2);

        hm_cluster1.insert(10, None, None).await.unwrap();

        let val_1 = hm_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(val_1 != val_2);
    }

    #[tokio::test]
    async fn test_replicate() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = HashSetCache::<i32>::new(expiration_policy, 32).await;
        let hm_cluster2 = HashSetCache::<i32>::new(expiration_policy, 32).await;
        hm_cluster2.replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.insert(1, None, None).await.unwrap();

        let val_1 = hm_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get_all().await.unwrap();

        assert_eq!(val_1, val_2);
    }

    #[tokio::test]
    async fn test_try_ttl() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache
            .insert(10, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        let ttl = hs_cache.try_ttl(&[10, 20]).await.unwrap();
        println!("{:?}", ttl);
        assert!(Some(Duration::from_secs(1)) > ttl[0]);
        assert_eq!(ttl[1], None);
    }

    #[tokio::test]
    async fn test_try_clear() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache.insert(10, None, None).await.unwrap();
        hs_cache.insert(20, None, None).await.unwrap();
        hs_cache.insert(30, None, None).await.unwrap();
        let hs = hs_cache.get_all().await.unwrap();
        assert_eq!(hs, HashSet::from([10, 20, 30]));
        hs_cache.try_clear().await.unwrap();
        let hs = hs_cache.get_all().await.unwrap();
        assert_eq!(hs.is_empty(), true);
    }

    #[tokio::test]
    async fn test_try_remove() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache
            .minsert(&[10, 20, 30], &[None, None, None], &[None, None, None])
            .await
            .unwrap();
        let vals = hs_cache.try_remove(&[10, 20, 30, 40]).await.unwrap();
        assert_eq!(vals, vec![true, true, true, false]);
    }

    #[tokio::test]
    async fn test_try_contains() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache.insert(10, None, None).await.unwrap();
        let vals = hs_cache.try_contains(&[10]).await.unwrap();
        assert_eq!(vals, vec![true]);
    }

    #[tokio::test]
    async fn test_try_minsert_ex() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache
            .try_minsert(
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
        tokio::time::sleep(Duration::from_secs(2)).await;
        let vals = hs_cache.get_all().await.unwrap();
        assert_eq!(vals, HashSet::<i32>::new());
    }

    #[tokio::test]
    async fn test_try_minsert() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache
            .try_minsert(&[10, 20, 30], &[None, None, None], &[None, None, None])
            .await
            .unwrap();
        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10, 20, 30]));
    }

    #[tokio::test]
    async fn test_try_insert_ex() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache.try_insert(10, None, None).await.unwrap();
        hs_cache
            .try_insert(20, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10]));
    }

    #[tokio::test]
    async fn test_try_insert() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache.try_insert(10, None, None).await.unwrap();
        hs_cache.try_insert(20, None, None).await.unwrap();
        hs_cache.try_insert(30, None, None).await.unwrap();
        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10, 20, 30]));
    }

    #[tokio::test]
    async fn test_ttl() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache
            .insert(10, Some(Duration::from_secs(1)), None)
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
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache.insert(10, None, None).await.unwrap();
        hs_cache.insert(20, None, None).await.unwrap();
        hs_cache.insert(30, None, None).await.unwrap();
        let hs = hs_cache.get_all().await.unwrap();
        assert_eq!(hs, HashSet::from([10, 20, 30]));
        hs_cache.clear().await.unwrap();
        let hs = hs_cache.get_all().await.unwrap();
        assert_eq!(hs.is_empty(), true);
    }

    #[tokio::test]
    async fn test_remove() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache
            .minsert(&[10, 20, 30], &[None, None, None], &[None, None, None])
            .await
            .unwrap();
        let vals = hs_cache.remove(&[10, 20, 30, 40]).await.unwrap();
        assert_eq!(vals, vec![true, true, true, false]);
    }

    #[tokio::test]
    async fn test_contains() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache.insert(10, None, None).await.unwrap();
        let vals = hs_cache.contains(&[10]).await.unwrap();
        assert_eq!(vals, vec![true]);
    }

    #[tokio::test]
    async fn test_minsert_ex() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache
            .minsert(
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
        tokio::time::sleep(Duration::from_secs(2)).await;
        let vals = hs_cache.get_all().await.unwrap();
        assert_eq!(vals, HashSet::<i32>::new());
    }

    #[tokio::test]
    async fn test_minsert() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache
            .minsert(&[10, 20, 30], &[None, None, None], &[None, None, None])
            .await
            .unwrap();
        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10, 20, 30]));
    }

    #[tokio::test]
    async fn test_insert_ex() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache.insert(10, None, None).await.unwrap();
        hs_cache
            .insert(20, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10]));
    }

    #[tokio::test]
    async fn test_insert() {
        let expiration_policy = ExpirationPolicy::None;
        let hs_cache = HashSetCache::new(expiration_policy, 32).await;
        hs_cache.insert(10, None, None).await.unwrap();
        hs_cache.insert(20, None, None).await.unwrap();
        hs_cache.insert(30, None, None).await.unwrap();
        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10, 20, 30]));
    }
}
