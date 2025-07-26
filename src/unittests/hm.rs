#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::tokio_actor_cache::hm::HashMapCache;

    #[tokio::test]
    async fn test_ttl() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache
            .insert("a", 10, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        let ttl = hm_cache.ttl("a").await.unwrap();
        assert!(Some(Duration::from_secs(1)) > ttl)
    }

    #[tokio::test]
    async fn test_clear() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache
            .insert("a", 10, None, None)
            .await
            .expect("failed to insert key into hm");
        hm_cache
            .insert("b", 12, None, None)
            .await
            .expect("failed to insert key into hm");
        hm_cache
            .insert("c", 20, None, None)
            .await
            .expect("failed to insert key into hm");
        let hm = hm_cache.get_all().await.unwrap();
        assert_eq!(!hm.is_empty(), true);
        hm_cache.clear().await.unwrap();
        let hm = hm_cache.get_all().await.unwrap();
        assert_eq!(hm.is_empty(), true);
    }

    #[tokio::test]
    async fn test_mget() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache
            .minsert(vec!["a", "b", "c"], vec![10, 20, 30], None, Some(true))
            .await
            .expect("failed to insert keys into hm");
        let vals = hm_cache.mget(vec!["a", "b", "c", "d"]).await.unwrap();
        assert_eq!(vals, vec![Some(10), Some(20), Some(30), None]);
    }

    #[tokio::test]
    async fn test_remove() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache
            .insert("a", 10, None, None)
            .await
            .expect("failed to insert key into hm");
        let val = hm_cache
            .remove("a")
            .await
            .expect("failed to remove key from hm");
        assert_eq!(val, Some(10));

        let val = hm_cache.get("a").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_minsert_nx_if_not_exists() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache
            .insert("a", 10, None, None)
            .await
            .expect("failed to insert key into hm");
        hm_cache
            .minsert(vec!["a", "b", "c"], vec![20, 20, 30], None, None)
            .await
            .expect("failed to insert keys into hm");
        let val = hm_cache.get("a").await.expect("failed to get key from hm");
        assert_eq!(val, Some(20));
    }

    #[tokio::test]
    async fn test_minsert_nx_if_exists() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache
            .insert("a", 10, None, None)
            .await
            .expect("failed to insert key into hm");
        hm_cache
            .minsert(vec!["a", "b", "c"], vec![20, 20, 30], None, Some(true))
            .await
            .expect("failed to insert keys into hm");
        let val = hm_cache.get("a").await.expect("failed to get key from hm");
        assert_eq!(val, Some(10));
    }

    #[tokio::test]
    async fn test_minsert_ex() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache
            .minsert(vec!["a", "b", "c"], vec![10, 20, 30], Some(Duration::from_secs(1)), None)
            .await
            .expect("failed to insert keys into hm");
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val_a = hm_cache.get("a").await.expect("failed to get key from hm");
        let val_b = hm_cache.get("b").await.expect("failed to get key from hm");
        let val_c = hm_cache.get("c").await.expect("failed to get key from hm");
        assert_eq!(val_a, None);
        assert_eq!(val_b, None);
        assert_eq!(val_c, None);
    }

    #[tokio::test]
    async fn test_minsert() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache
            .minsert(vec!["a", "b", "c"], vec![10, 20, 30], None, None)
            .await
            .expect("failed to insert keys into hm");
        let val_a = hm_cache.get("a").await.expect("failed to get key from hm");
        let val_b = hm_cache.get("b").await.expect("failed to get key from hm");
        let val_c = hm_cache.get("c").await.expect("failed to get key from hm");
        assert_eq!(val_a, Some(10));
        assert_eq!(val_b, Some(20));
        assert_eq!(val_c, Some(30));
    }

    #[tokio::test]
    async fn test_minsert_inconsistent_len() {
        let hm_cache = HashMapCache::new(32).await;
        let res = hm_cache
            .minsert(vec!["a", "b"], vec![10, 20, 30], None, None)
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_insert_nx_if_not_exists() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache
            .insert("a", 10, None, None)
            .await
            .expect("failed to insert key into hm");
        hm_cache
            .insert("a", 20, None, None)
            .await
            .expect("failed to insert key into hm");
        let val = hm_cache.get("a").await.expect("failed to get key from hm");
        assert_eq!(val, Some(20));
    }

    #[tokio::test]
    async fn test_insert_nx_if_exists() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache
            .insert("a", 10, None, None)
            .await
            .expect("failed to insert key into hm");
        hm_cache
            .insert("a", 20, None, Some(true))
            .await
            .expect("failed to insert key into hm");
        let val = hm_cache.get("a").await.expect("failed to get key from hm");
        assert_eq!(val, Some(10));
    }

    #[tokio::test]
    async fn test_insert_ex() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache
            .insert("a", 10, None, None)
            .await
            .expect("failed to insert key into hm");
        hm_cache
            .insert("b", 20, Some(Duration::from_secs(1)), None)
            .await
            .expect("failed to insert key into hm");
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val_a = hm_cache.get("a").await.expect("failed to get key from hm");
        let val_b = hm_cache.get("b").await.expect("failed to get key from hm");
        assert_eq!(val_a, Some(10));
        assert_eq!(val_b, None);
    }

    #[tokio::test]
    async fn test_insert() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache
            .insert("a", 10, None, None)
            .await
            .expect("failed to insert key into hm");
        let val = hm_cache.get("a").await.expect("failed to get key from hm");
        assert_eq!(val, Some(10));
    }
}
