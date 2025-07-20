#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use crate::tokio_actor_cache::hs::HashSetCache;

    #[tokio::test]
    async fn test_clear() {
        let hs_cache = HashSetCache::new(32).await;
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
        let hs_cache = HashSetCache::new(32).await;
        hs_cache.insert(10, None, None).await.unwrap();
        let val = hs_cache.remove(10).await.unwrap();
        assert_eq!(val, true);

        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::new());
    }

    #[tokio::test]
    async fn test_insert_ex() {
        let hs_cache = HashSetCache::new(32).await;
        hs_cache.insert(10, None, None).await.unwrap();
        hs_cache.insert(20, Some(Duration::from_secs(1)), None).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10]));
    }

    #[tokio::test]
    async fn test_insert() {
        let hs_cache = HashSetCache::new(32).await;
        hs_cache.insert(10, None, None).await.unwrap();
        hs_cache.insert(20, None, None).await.unwrap();
        hs_cache.insert(30, None, None).await.unwrap();
        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10, 20, 30]));
    }
}
