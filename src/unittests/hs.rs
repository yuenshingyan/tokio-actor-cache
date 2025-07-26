#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use crate::tokio_actor_cache::hs::HashSetCache;

    #[tokio::test]
    async fn test_ttl() {
        let hs_cache = HashSetCache::new(32).await;
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
        hs_cache.minsert(&[10, 20, 30], &[None, None, None], &[None, None, None]).await.unwrap();
        let vals = hs_cache.remove(&[10, 20, 30, 40]).await.unwrap();
        assert_eq!(vals, vec![true, true, true, false]);
    }

    #[tokio::test]
    async fn test_contains() {
        let hs_cache = HashSetCache::new(32).await;
        hs_cache.insert(10, None, None).await.unwrap();
        let vals = hs_cache.contains(&[10]).await.unwrap();
        assert_eq!(vals, vec![true]);
    }

    #[tokio::test]
    async fn test_minsert_ex() {
        let hs_cache = HashSetCache::new(32).await;
        hs_cache
            .minsert(
                &[10, 20, 30], 
                &[Some(Duration::from_secs(1)), Some(Duration::from_secs(1)), Some(Duration::from_secs(1))], 
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
        let hs_cache = HashSetCache::new(32).await;
        hs_cache.minsert(&[10, 20, 30], &[None, None, None], &[None, None, None]).await.unwrap();
        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10, 20, 30]));
    }

    #[tokio::test]
    async fn test_insert_ex() {
        let hs_cache = HashSetCache::new(32).await;
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
        let hs_cache = HashSetCache::new(32).await;
        hs_cache.insert(10, None, None).await.unwrap();
        hs_cache.insert(20, None, None).await.unwrap();
        hs_cache.insert(30, None, None).await.unwrap();
        let val = hs_cache.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10, 20, 30]));
    }
}
