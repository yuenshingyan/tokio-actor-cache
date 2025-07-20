#[cfg(test)]
mod tests {

    use std::time::Duration;

    use crate::tokio_actor_cache::vec::VecCache;

    #[tokio::test]
    async fn test_ttl() {
        let vec_cache = VecCache::new(32).await;
        vec_cache
            .push(10, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        let ttl = vec_cache.ttl(10).await.unwrap();
        assert!(Some(Duration::from_secs(1)) > ttl)
    }

    #[tokio::test]
    async fn test_clear() {
        let vec_cache = VecCache::new(32).await;
        vec_cache.push(10, None, None).await.unwrap();
        vec_cache.push(20, None, None).await.unwrap();
        vec_cache.push(30, None, None).await.unwrap();
        let hs = vec_cache.get_all().await.unwrap();
        assert_eq!(hs, Vec::from([10, 20, 30]));
        vec_cache.clear().await.unwrap();
        let hs = vec_cache.get_all().await.unwrap();
        assert_eq!(hs.is_empty(), true);
    }

    #[tokio::test]
    async fn test_remove() {
        let vec_cache = VecCache::new(32).await;
        vec_cache.push(10, None, None).await.unwrap();
        let val = vec_cache.remove(10).await.unwrap();
        assert_eq!(val, true);

        let val = vec_cache.get_all().await.unwrap();
        assert_eq!(val, Vec::new());
    }

    #[tokio::test]
    async fn test_contains() {
        let vec_cache = VecCache::new(32).await;
        vec_cache.push(10, None, None).await.unwrap();
        vec_cache.push(20, None, None).await.unwrap();
        let val = vec_cache.contains(10).await.unwrap();
        assert_eq!(val, true);
    }

    #[tokio::test]
    async fn test_push_ex() {
        let vec_cache = VecCache::new(32).await;
        vec_cache.push(10, None, None).await.unwrap();
        vec_cache
            .push(20, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val = vec_cache.get_all().await.unwrap();
        assert_eq!(val, Vec::from([10]));
    }

    #[tokio::test]
    async fn test_push() {
        let vec_cache = VecCache::new(32).await;
        vec_cache.push(10, None, None).await.unwrap();
        vec_cache.push(20, None, None).await.unwrap();
        vec_cache.push(30, None, None).await.unwrap();
        let val = vec_cache.get_all().await.unwrap();
        assert_eq!(val, Vec::from([10, 20, 30]));
    }
}
