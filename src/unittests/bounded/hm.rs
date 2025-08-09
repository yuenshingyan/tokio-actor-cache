#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::tokio_cache::{bounded::hm::HashMapCache, option::ExpirationPolicy};

    #[tokio::test]
    async fn test_try_replicated_data_persist() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = HashMapCache::<&str, i32>::new(expiration_policy, 32).await;
        let hm_cluster2 = HashMapCache::<&str, i32>::new(expiration_policy, 32).await;
        hm_cluster2.try_replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.insert("a", 1, None, false).await.unwrap();

        let val_1 = hm_cluster1.get("a").await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        hm_cluster2.stop_replicating().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get("a").await.unwrap();

        assert_eq!(val_1, val_2);
    }

    #[tokio::test]
    async fn test_try_stop_replicating() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = HashMapCache::<&str, i32>::new(expiration_policy, 32).await;
        let hm_cluster2 = HashMapCache::<&str, i32>::new(expiration_policy, 32).await;
        hm_cluster2.try_replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.insert("a", 1, None, false).await.unwrap();

        let val_1 = hm_cluster1.get("a").await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        hm_cluster2.try_stop_replicating().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get("a").await.unwrap();

        assert_eq!(val_1, val_2);

        hm_cluster1.insert("a", 10, None, false).await.unwrap();

        let val_1 = hm_cluster1.get("a").await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(val_1 != val_2);
    }

    #[tokio::test]
    async fn test_try_replicate() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = HashMapCache::<&str, i32>::new(expiration_policy, 32).await;
        let hm_cluster2 = HashMapCache::<&str, i32>::new(expiration_policy, 32).await;
        hm_cluster2.try_replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.insert("a", 1, None, false).await.unwrap();

        let val_1 = hm_cluster1.get("a").await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get("a").await.unwrap();

        assert_eq!(val_1, val_2);
    }

    #[tokio::test]
    async fn test_replicated_data_persist() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = HashMapCache::<&str, i32>::new(expiration_policy, 32).await;
        let hm_cluster2 = HashMapCache::<&str, i32>::new(expiration_policy, 32).await;
        hm_cluster2.replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.insert("a", 1, None, false).await.unwrap();

        let val_1 = hm_cluster1.get("a").await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        hm_cluster2.stop_replicating().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get("a").await.unwrap();

        assert_eq!(val_1, val_2);
    }

    #[tokio::test]
    async fn test_stop_replicating() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = HashMapCache::<&str, i32>::new(expiration_policy, 32).await;
        let hm_cluster2 = HashMapCache::<&str, i32>::new(expiration_policy, 32).await;
        hm_cluster2.replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.insert("a", 1, None, false).await.unwrap();

        let val_1 = hm_cluster1.get("a").await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        hm_cluster2.stop_replicating().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get("a").await.unwrap();

        assert_eq!(val_1, val_2);

        hm_cluster1.insert("a", 10, None, false).await.unwrap();

        let val_1 = hm_cluster1.get("a").await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(val_1 != val_2);
    }

    #[tokio::test]
    async fn test_replicate() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = HashMapCache::<&str, i32>::new(expiration_policy, 32).await;
        let hm_cluster2 = HashMapCache::<&str, i32>::new(expiration_policy, 32).await;
        hm_cluster2.replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.insert("a", 1, None, false).await.unwrap();

        let val_1 = hm_cluster1.get("a").await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get("a").await.unwrap();

        assert_eq!(val_1, val_2);
    }

    #[tokio::test]
    async fn test_try_ttl() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache
            .insert("a", 10, Some(Duration::from_secs(1)), false)
            .await
            .unwrap();
        let ttl = hm_cache.try_ttl(&["a", "b"]).await.unwrap();
        assert!(Some(Duration::from_secs(1)) > ttl[0]);
        assert_eq!(ttl[1], None);
    }

    #[tokio::test]
    async fn test_try_clear() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.insert("a", 10, None, false).await.unwrap();
        hm_cache.insert("b", 12, None, false).await.unwrap();
        hm_cache.insert("c", 20, None, false).await.unwrap();
        let hm = hm_cache.get_all().await.unwrap();
        assert_eq!(!hm.is_empty(), true);
        hm_cache.try_clear().await.unwrap();
        let hm = hm_cache.get_all().await.unwrap();
        assert_eq!(hm.is_empty(), true);
    }

    #[tokio::test]
    async fn test_try_mget() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[true, true, true],
            )
            .await
            .unwrap();
        let vals = hm_cache.try_mget(&["a", "b", "c", "d"]).await.unwrap();
        assert_eq!(vals, vec![Some(10), Some(20), Some(30), None]);
    }

    #[tokio::test]
    async fn test_try_remove() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[false, false, false],
            )
            .await
            .unwrap();
        let vals = hm_cache.try_remove(&["a", "b", "c", "d"]).await.unwrap();
        assert_eq!(vals, vec![Some(10), Some(20), Some(30), None]);
    }

    #[tokio::test]
    async fn test_try_contains_keys() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[false, false, false],
            )
            .await
            .unwrap();
        let is_contains_keys = hm_cache
            .try_contains_key(&["a", "b", "c", "d"])
            .await
            .unwrap();
        assert_eq!(is_contains_keys, vec![true, true, true, false]);
    }

    #[tokio::test]
    async fn test_try_minsert_nx_if_not_exists() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.try_insert("a", 10, None, false).await.unwrap();
        hm_cache
            .try_minsert(
                &["a", "b", "c"],
                &[20, 20, 30],
                &[None, None, None],
                &[false, false, false],
            )
            .await
            .unwrap();
        let val = hm_cache.get("a").await.unwrap();
        assert_eq!(val, Some(20));
    }

    #[tokio::test]
    async fn test_try_minsert_nx_if_exists() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.try_insert("a", 10, None, false).await.unwrap();
        hm_cache
            .try_minsert(
                &["a", "b", "c"],
                &[20, 20, 30],
                &[None, None, None],
                &[true, true, true],
            )
            .await
            .unwrap();
        let val = hm_cache.get("a").await.unwrap();
        assert_eq!(val, Some(10));
    }

    #[tokio::test]
    async fn test_try_minsert_ex() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache
            .try_minsert(
                &["a", "b", "c"],
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
        let val_a = hm_cache.get("a").await.unwrap();
        let val_b = hm_cache.get("b").await.unwrap();
        let val_c = hm_cache.get("c").await.unwrap();
        assert_eq!(val_a, None);
        assert_eq!(val_b, None);
        assert_eq!(val_c, None);
    }

    #[tokio::test]
    async fn test_try_minsert() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache
            .try_minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[false, false, false],
            )
            .await
            .unwrap();
        let val_a = hm_cache.get("a").await.unwrap();
        let val_b = hm_cache.get("b").await.unwrap();
        let val_c = hm_cache.get("c").await.unwrap();
        assert_eq!(val_a, Some(10));
        assert_eq!(val_b, Some(20));
        assert_eq!(val_c, Some(30));
    }

    #[tokio::test]
    async fn test_try_minsert_inconsistent_len() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        let res = hm_cache
            .try_minsert(
                &["a", "b"],
                &[10, 20, 30],
                &[None, None, None],
                &[false, false, false],
            )
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_try_insert_nx_if_not_exists() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.try_insert("a", 10, None, false).await.unwrap();
        hm_cache.try_insert("a", 20, None, false).await.unwrap();
        let val = hm_cache.get("a").await.unwrap();
        assert_eq!(val, Some(20));
    }

    #[tokio::test]
    async fn test_try_insert_nx_if_exists() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.try_insert("a", 10, None, false).await.unwrap();
        hm_cache
            .try_insert("a", 20, None, true)
            .await
            .unwrap();
        let val = hm_cache.get("a").await.unwrap();
        assert_eq!(val, Some(10));
    }

    #[tokio::test]
    async fn test_try_insert_ex() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.try_insert("a", 10, None, false).await.unwrap();
        hm_cache
            .try_insert("b", 20, Some(Duration::from_secs(1)), false)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val_a = hm_cache.get("a").await.unwrap();
        let val_b = hm_cache.get("b").await.unwrap();
        assert_eq!(val_a, Some(10));
        assert_eq!(val_b, None);
    }

    #[tokio::test]
    async fn test_try_insert() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.try_insert("a", 10, None, false).await.unwrap();
        let val = hm_cache.get("a").await.unwrap();
        assert_eq!(val, Some(10));
    }

    #[tokio::test]
    async fn test_ttl() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache
            .insert("a", 10, Some(Duration::from_secs(1)), false)
            .await
            .unwrap();
        let ttl = hm_cache.ttl(&["a", "b"]).await.unwrap();
        assert!(Some(Duration::from_secs(1)) > ttl[0]);
        assert_eq!(ttl[1], None);
    }

    #[tokio::test]
    async fn test_clear() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.insert("a", 10, None, false).await.unwrap();
        hm_cache.insert("b", 12, None, false).await.unwrap();
        hm_cache.insert("c", 20, None, false).await.unwrap();
        let hm = hm_cache.get_all().await.unwrap();
        assert_eq!(!hm.is_empty(), true);
        hm_cache.clear().await.unwrap();
        let hm = hm_cache.get_all().await.unwrap();
        assert_eq!(hm.is_empty(), true);
    }

    #[tokio::test]
    async fn test_mget() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[true, true, true],
            )
            .await
            .unwrap();
        let vals = hm_cache.mget(&["a", "b", "c", "d"]).await.unwrap();
        assert_eq!(vals, vec![Some(10), Some(20), Some(30), None]);
    }

    #[tokio::test]
    async fn test_remove() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[false, false, false],
            )
            .await
            .unwrap();
        let vals = hm_cache.remove(&["a", "b", "c", "d"]).await.unwrap();
        assert_eq!(vals, vec![Some(10), Some(20), Some(30), None]);
    }

    #[tokio::test]
    async fn test_contains_keys() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[false, false, false],
            )
            .await
            .unwrap();
        let is_contains_keys = hm_cache.contains_key(&["a", "b", "c", "d"]).await.unwrap();
        assert_eq!(is_contains_keys, vec![true, true, true, false]);
    }

    #[tokio::test]
    async fn test_minsert_nx_if_not_exists() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.insert("a", 10, None, false).await.unwrap();
        hm_cache
            .minsert(
                &["a", "b", "c"],
                &[20, 20, 30],
                &[None, None, None],
                &[false, false, false],
            )
            .await
            .unwrap();
        let val = hm_cache.get("a").await.unwrap();
        assert_eq!(val, Some(20));
    }

    #[tokio::test]
    async fn test_minsert_nx_if_exists() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.insert("a", 10, None, false).await.unwrap();
        hm_cache
            .minsert(
                &["a", "b", "c"],
                &[20, 20, 30],
                &[None, None, None],
                &[true, true, true],
            )
            .await
            .unwrap();
        let val = hm_cache.get("a").await.unwrap();
        assert_eq!(val, Some(10));
    }

    #[tokio::test]
    async fn test_minsert_ex() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache
            .minsert(
                &["a", "b", "c"],
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
        let val_a = hm_cache.get("a").await.unwrap();
        let val_b = hm_cache.get("b").await.unwrap();
        let val_c = hm_cache.get("c").await.unwrap();
        assert_eq!(val_a, None);
        assert_eq!(val_b, None);
        assert_eq!(val_c, None);
    }

    #[tokio::test]
    async fn test_minsert() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[false, false, false],
            )
            .await
            .unwrap();
        let val_a = hm_cache.get("a").await.unwrap();
        let val_b = hm_cache.get("b").await.unwrap();
        let val_c = hm_cache.get("c").await.unwrap();
        assert_eq!(val_a, Some(10));
        assert_eq!(val_b, Some(20));
        assert_eq!(val_c, Some(30));
    }

    #[tokio::test]
    async fn test_minsert_inconsistent_len() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        let res = hm_cache
            .minsert(
                &["a", "b"],
                &[10, 20, 30],
                &[None, None, None],
                &[false, false, false],
            )
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_insert_nx_if_not_exists() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.insert("a", 10, None, false).await.unwrap();
        hm_cache.insert("a", 20, None, false).await.unwrap();
        let val = hm_cache.get("a").await.unwrap();
        assert_eq!(val, Some(20));
    }

    #[tokio::test]
    async fn test_insert_nx_if_exists() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.insert("a", 10, None, false).await.unwrap();
        hm_cache.insert("a", 20, None, true).await.unwrap();
        let val = hm_cache.get("a").await.unwrap();
        assert_eq!(val, Some(10));
    }

    #[tokio::test]
    async fn test_insert_ex() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.insert("a", 10, None, false).await.unwrap();
        hm_cache
            .insert("b", 20, Some(Duration::from_secs(1)), false)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val_a = hm_cache.get("a").await.unwrap();
        let val_b = hm_cache.get("b").await.unwrap();
        assert_eq!(val_a, Some(10));
        assert_eq!(val_b, None);
    }

    #[tokio::test]
    async fn test_insert() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cache = HashMapCache::new(expiration_policy, 32).await;
        hm_cache.insert("a", 10, None, false).await.unwrap();
        let val = hm_cache.get("a").await.unwrap();
        assert_eq!(val, Some(10));
    }
}
