#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::tokio_actor_cache::hm::HashMapCache;

    #[tokio::test]
    async fn test_clear() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache.insert("a", 10, None, None).await.expect("failed to insert key into hm");
        hm_cache.insert("b", 12, None, None).await.expect("failed to insert key into hm");
        hm_cache.insert("c", 20, None, None).await.expect("failed to insert key into hm");
        let hm = hm_cache.get_all().await.unwrap();
        assert_eq!(!hm.is_empty(), true);
        hm_cache.clear().await.unwrap();
        let hm = hm_cache.get_all().await.unwrap();
        assert_eq!(hm.is_empty(), true);
    }

    #[tokio::test]
    async fn test_remove() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache.insert("a", 10, None, None).await.expect("failed to insert key into hm");
        let val = hm_cache.remove("a").await.expect("failed to remove key from hm");
        assert_eq!(val, Some(10));

        let val = hm_cache.get("a").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_insert_nx_if_not_exists() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache.insert("a", 10, None, None).await.expect("failed to insert key into hm");
        hm_cache.insert("a", 20, None, None).await.expect("failed to insert key into hm");
        let val = hm_cache.get("a").await.expect("failed to get key from hm");
        assert_eq!(val, Some(20));
    }

    #[tokio::test]
    async fn test_insert_nx_if_exists() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache.insert("a", 10, None, None).await.expect("failed to insert key into hm");
        hm_cache.insert("a", 20, None, Some(true)).await.expect("failed to insert key into hm");
        let val = hm_cache.get("a").await.expect("failed to get key from hm");
        assert_eq!(val, Some(10));
    }

    #[tokio::test]
    async fn test_insert_ex() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache.insert("a", 10, None, None).await.expect("failed to insert key into hm");
        hm_cache.insert("b", 20, Some(Duration::from_secs(1)), None).await.expect("failed to insert key into hm");
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val_a = hm_cache.get("a").await.expect("failed to get key from hm");
        let val_b = hm_cache.get("b").await.expect("failed to get key from hm");
        assert_eq!(val_a, Some(10));
        assert_eq!(val_b, None);
    }

    #[tokio::test]
    async fn test_insert() {
        let hm_cache = HashMapCache::new(32).await;
        hm_cache.insert("a", 10, None, None).await.expect("failed to insert key into hm");
        let val = hm_cache.get("a").await.expect("failed to get key from hm");
        assert_eq!(val, Some(10));
    }
}