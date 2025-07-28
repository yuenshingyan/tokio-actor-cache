#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use crate::tokio_cache::bounded::hs::HashSetCacheCluster;

    #[tokio::test]
    async fn test_try_ttl() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster
            .insert(10, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        let ttl = hs_cluster.try_ttl(&[10, 20]).await.unwrap();
        assert!(Some(Duration::from_secs(1)) > ttl[0]);
        assert_eq!(ttl[1], None);
    }

    #[tokio::test]
    async fn test_try_clear() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster.insert(10, None, None).await.unwrap();
        hs_cluster.insert(20, None, None).await.unwrap();
        hs_cluster.insert(30, None, None).await.unwrap();
        let hs = hs_cluster.get_all().await.unwrap();
        assert_eq!(hs, HashSet::from([10, 20, 30]));
        hs_cluster.try_clear().await.unwrap();
        let hs = hs_cluster.get_all().await.unwrap();
        assert_eq!(hs.is_empty(), true);
    }

    #[tokio::test]
    async fn test_try_remove() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster
            .minsert(&[10, 20, 30], &[None, None, None], &[None, None, None])
            .await
            .unwrap();
        let vals = hs_cluster.try_remove(&[10, 20, 30, 40]).await.unwrap();
        assert_eq!(vals, vec![true, true, true, false]);
    }

    #[tokio::test]
    async fn test_try_contains() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster.insert(10, None, None).await.unwrap();
        let vals = hs_cluster.try_contains(&[10]).await.unwrap();
        assert_eq!(vals, vec![true]);
    }

    #[tokio::test]
    async fn test_try_minsert_ex() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster
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
        let vals = hs_cluster.get_all().await.unwrap();
        assert_eq!(vals, HashSet::<i32>::new());
    }

    #[tokio::test]
    async fn test_try_minsert() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster
            .try_minsert(&[10, 20, 30], &[None, None, None], &[None, None, None])
            .await
            .unwrap();
        let val = hs_cluster.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10, 20, 30]));
    }

    #[tokio::test]
    async fn test_try_insert_ex() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster.try_insert(10, None, None).await.unwrap();
        hs_cluster
            .try_insert(20, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val = hs_cluster.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10]));
    }

    #[tokio::test]
    async fn test_try_insert() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster.try_insert(10, None, None).await.unwrap();
        hs_cluster.try_insert(20, None, None).await.unwrap();
        hs_cluster.try_insert(30, None, None).await.unwrap();
        let val = hs_cluster.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10, 20, 30]));
    }

    #[tokio::test]
    async fn test_hash_id() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        let keys = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
            "f".to_string(),
            "g".to_string(),
        ];
        for k in keys.clone() {
            hs_cluster.insert(k, None, None).await.unwrap();
        }

        let vals = hs_cluster.get_all().await.unwrap();
        assert_eq!(vals.len(), keys.len());
    }

    #[tokio::test]
    async fn test_ttl() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster
            .insert(10, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        let ttl = hs_cluster.ttl(&[10, 20]).await.unwrap();
        println!("{:?}", ttl);
        assert!(Some(Duration::from_secs(1)) > ttl[0]);
        assert_eq!(ttl[1], None);
    }

    #[tokio::test]
    async fn test_clear() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster.insert(10, None, None).await.unwrap();
        hs_cluster.insert(20, None, None).await.unwrap();
        hs_cluster.insert(30, None, None).await.unwrap();
        let hs = hs_cluster.get_all().await.unwrap();
        assert_eq!(hs, HashSet::from([10, 20, 30]));
        hs_cluster.clear().await.unwrap();
        let hs = hs_cluster.get_all().await.unwrap();
        assert_eq!(hs.is_empty(), true);
    }

    #[tokio::test]
    async fn test_remove() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster
            .minsert(&[10, 20, 30], &[None, None, None], &[None, None, None])
            .await
            .unwrap();
        let vals = hs_cluster.remove(&[10, 20, 30, 40]).await.unwrap();
        assert_eq!(vals, vec![true, true, true, false]);
    }

    #[tokio::test]
    async fn test_contains() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster.insert(10, None, None).await.unwrap();
        let vals = hs_cluster.contains(&[10]).await.unwrap();
        assert_eq!(vals, vec![true]);
    }

    #[tokio::test]
    async fn test_minsert_ex() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster
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
        let vals = hs_cluster.get_all().await.unwrap();
        assert_eq!(vals, HashSet::<i32>::new());
    }

    #[tokio::test]
    async fn test_minsert() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster
            .minsert(&[10, 20, 30], &[None, None, None], &[None, None, None])
            .await
            .unwrap();
        let val = hs_cluster.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10, 20, 30]));
    }

    #[tokio::test]
    async fn test_insert_ex() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster.insert(10, None, None).await.unwrap();
        hs_cluster
            .insert(20, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val = hs_cluster.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10]));
    }

    #[tokio::test]
    async fn test_insert() {
        let hs_cluster = HashSetCacheCluster::new(32, 3).await;
        hs_cluster.insert(10, None, None).await.unwrap();
        hs_cluster.insert(20, None, None).await.unwrap();
        hs_cluster.insert(30, None, None).await.unwrap();
        let val = hs_cluster.get_all().await.unwrap();
        assert_eq!(val, HashSet::from([10, 20, 30]));
    }
}
