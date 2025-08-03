#[cfg(test)]
mod tests {

    use std::time::Duration;

    use crate::tokio_cache::{expiration_policy::ExpirationPolicy, unbounded::vec::VecCache};

    #[tokio::test]
    async fn test_expiration_policy_lru() {
        let expiration_policy = ExpirationPolicy::LRU(1);
        let hs_cache = VecCache::<i32>::new(expiration_policy).await;
        hs_cache.push(1, None, false).await.unwrap();
        hs_cache.push(2, None, false).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let hs = hs_cache.get_all().await.unwrap();
        assert_eq!(Vec::from([(2)]), hs);
    }

    #[tokio::test]
    async fn test_expiration_policy_lfu() {
        let expiration_policy = ExpirationPolicy::LFU(1);
        let hs_cache = VecCache::<i32>::new(expiration_policy).await;
        hs_cache.push(1, None, false).await.unwrap();
        hs_cache.push(1, None, false).await.unwrap();
        hs_cache.push(3, None, false).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let hs = hs_cache.get_all().await.unwrap();
        assert_eq!(Vec::from([(1)]), hs);
    }

    #[tokio::test]
    async fn test_replicated_data_persist() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = VecCache::<i32>::new(expiration_policy).await;
        let hm_cluster2 = VecCache::<i32>::new(expiration_policy).await;
        hm_cluster2.replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.push(1, None, false).await.unwrap();

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
        let hm_cluster1 = VecCache::<i32>::new(expiration_policy).await;
        let hm_cluster2 = VecCache::<i32>::new(expiration_policy).await;
        hm_cluster2.replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.push(1, None, false).await.unwrap();

        let val_1 = hm_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        hm_cluster2.stop_replicating().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get_all().await.unwrap();

        assert_eq!(val_1, val_2);

        hm_cluster1.push(10, None, false).await.unwrap();

        let val_1 = hm_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(val_1 != val_2);
    }

    #[tokio::test]
    async fn test_replicate() {
        let expiration_policy = ExpirationPolicy::None;
        let hm_cluster1 = VecCache::<i32>::new(expiration_policy).await;
        let hm_cluster2 = VecCache::<i32>::new(expiration_policy).await;
        hm_cluster2.replicate(&hm_cluster1).await.unwrap();

        hm_cluster1.push(1, None, false).await.unwrap();

        let val_1 = hm_cluster1.get_all().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let val_2 = hm_cluster2.get_all().await.unwrap();

        assert_eq!(val_1, val_2);
    }

    #[tokio::test]
    async fn test_ttl() {
        let expiration_policy = ExpirationPolicy::None;
        let vec_cache = VecCache::new(expiration_policy).await;
        vec_cache
            .push(10, Some(Duration::from_secs(1)), false)
            .await
            .unwrap();
        let ttl = vec_cache.ttl(&[10, 20]).await.unwrap();
        println!("{:?}", ttl);
        assert!(Some(Duration::from_secs(1)) > ttl[0]);
        assert_eq!(ttl[1], None);
    }

    #[tokio::test]
    async fn test_clear() {
        let expiration_policy = ExpirationPolicy::None;
        let vec_cache = VecCache::new(expiration_policy).await;
        vec_cache.push(10, None, false).await.unwrap();
        vec_cache.push(20, None, false).await.unwrap();
        vec_cache.push(30, None, false).await.unwrap();
        let hs = vec_cache.get_all().await.unwrap();
        assert_eq!(hs, Vec::from([10, 20, 30]));
        vec_cache.clear().await.unwrap();
        let hs = vec_cache.get_all().await.unwrap();
        assert_eq!(hs.is_empty(), true);
    }

    #[tokio::test]
    async fn test_remove() {
        let expiration_policy = ExpirationPolicy::None;
        let vec_cache = VecCache::new(expiration_policy).await;
        vec_cache.push(10, None, false).await.unwrap();
        let val = vec_cache.remove(&[10, 20]).await.unwrap();
        assert_eq!(val, vec![true, false]);
    }

    #[tokio::test]
    async fn test_contains() {
        let expiration_policy = ExpirationPolicy::None;
        let vec_cache = VecCache::new(expiration_policy).await;
        vec_cache.push(10, None, false).await.unwrap();
        vec_cache.push(20, None, false).await.unwrap();
        let val = vec_cache.contains(&[10, 20, 30]).await.unwrap();
        assert_eq!(val, vec![true, true, false]);
    }

    #[tokio::test]
    async fn test_mpush_ex() {
        let expiration_policy = ExpirationPolicy::None;
        let vec_cache = VecCache::new(expiration_policy).await;
        vec_cache
            .mpush(
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
        let val = vec_cache.get_all().await.unwrap();
        assert_eq!(val, Vec::new());
    }

    #[tokio::test]
    async fn test_mpush() {
        let expiration_policy = ExpirationPolicy::None;
        let vec_cache = VecCache::new(expiration_policy).await;
        vec_cache
            .mpush(&[10, 20, 30], &[None, None, None], &[false, false, false])
            .await
            .unwrap();
        let val = vec_cache.get_all().await.unwrap();
        assert_eq!(val, Vec::from([10, 20, 30]));
    }

    #[tokio::test]
    async fn test_push_ex() {
        let expiration_policy = ExpirationPolicy::None;
        let vec_cache = VecCache::new(expiration_policy).await;
        vec_cache.push(10, None, false).await.unwrap();
        vec_cache
            .push(20, Some(Duration::from_secs(1)), false)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val = vec_cache.get_all().await.unwrap();
        assert_eq!(val, Vec::from([10]));
    }

    #[tokio::test]
    async fn test_push() {
        let expiration_policy = ExpirationPolicy::None;
        let vec_cache = VecCache::new(expiration_policy).await;
        vec_cache.push(10, None, false).await.unwrap();
        vec_cache.push(20, None, false).await.unwrap();
        vec_cache.push(30, None, false).await.unwrap();
        let val = vec_cache.get_all().await.unwrap();
        assert_eq!(val, Vec::from([10, 20, 30]));
    }
}
