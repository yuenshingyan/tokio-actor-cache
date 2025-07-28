#[cfg(test)]
mod tests {

    use std::time::Duration;

    use crate::tokio_cache::bounded::vec::VecCacheCluster;

    #[tokio::test]
    async fn test_try_ttl() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster
            .push(10, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        let ttl = vec_cluster.try_ttl(&[10, 20]).await.unwrap();
        assert!(Some(Duration::from_secs(1)) > ttl[0]);
        assert_eq!(ttl[1], None);
    }

    #[tokio::test]
    async fn test_try_clear() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster.push(10, None, None).await.unwrap();
        vec_cluster.push(20, None, None).await.unwrap();
        vec_cluster.push(30, None, None).await.unwrap();
        let mut vec = vec_cluster.get_all().await.unwrap();
        vec.sort();
        assert_eq!(vec, Vec::from([10, 20, 30]));
        vec_cluster.try_clear().await.unwrap();
        let hs = vec_cluster.get_all().await.unwrap();
        assert_eq!(hs.is_empty(), true);
    }

    #[tokio::test]
    async fn test_try_remove() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster.push(10, None, None).await.unwrap();
        let val = vec_cluster.try_remove(&[10, 20]).await.unwrap();
        assert_eq!(val, vec![true, false]);
    }

    #[tokio::test]
    async fn test_try_contains() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster.push(10, None, None).await.unwrap();
        vec_cluster.push(20, None, None).await.unwrap();
        let val = vec_cluster.try_contains(&[10, 20, 30]).await.unwrap();
        assert_eq!(val, vec![true, true, false]);
    }

    #[tokio::test]
    async fn test_try_mpush_ex() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster
            .try_mpush(
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
        let val = vec_cluster.get_all().await.unwrap();
        assert_eq!(val, Vec::new());
    }

    #[tokio::test]
    async fn test_try_mpush() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster
            .try_mpush(&[10, 20, 30], &[None, None, None], &[None, None, None])
            .await
            .unwrap();
        let mut val = vec_cluster.get_all().await.unwrap();
        val.sort();
        assert_eq!(val, Vec::from([10, 20, 30]));
    }

    #[tokio::test]
    async fn test_try_push_ex() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster.try_push(10, None, None).await.unwrap();
        vec_cluster
            .try_push(20, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val = vec_cluster.get_all().await.unwrap();
        assert_eq!(val, Vec::from([10]));
    }

    #[tokio::test]
    async fn test_try_push() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster.try_push(10, None, None).await.unwrap();
        vec_cluster.try_push(20, None, None).await.unwrap();
        vec_cluster.try_push(30, None, None).await.unwrap();
        let mut val = vec_cluster.get_all().await.unwrap();
        val.sort();
        assert_eq!(val, Vec::from([10, 20, 30]));
    }

    #[tokio::test]
    async fn test_hash_id() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        let vals = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
            "f".to_string(),
            "g".to_string(),
        ];
        for v in vals.clone() {
            vec_cluster.push(v.clone(), None, None).await.unwrap();
        }

        let mut vec = vec_cluster.get_all().await.unwrap();
        vec.sort();
        assert_eq!(vec, vals);
    }

    #[tokio::test]
    async fn test_ttl() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster
            .push(10, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        let ttl = vec_cluster.ttl(&[10, 20]).await.unwrap();
        assert!(Some(Duration::from_secs(1)) > ttl[0]);
        assert_eq!(ttl[1], None);
    }

    #[tokio::test]
    async fn test_clear() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster.push(10, None, None).await.unwrap();
        vec_cluster.push(20, None, None).await.unwrap();
        vec_cluster.push(30, None, None).await.unwrap();
        let mut vec = vec_cluster.get_all().await.unwrap();
        vec.sort();
        assert_eq!(vec, Vec::from([10, 20, 30]));
        vec_cluster.clear().await.unwrap();
        let hs = vec_cluster.get_all().await.unwrap();
        assert_eq!(hs.is_empty(), true);
    }

    #[tokio::test]
    async fn test_remove() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster.push(10, None, None).await.unwrap();
        let val = vec_cluster.remove(&[10, 20]).await.unwrap();
        assert_eq!(val, vec![true, false]);
    }

    #[tokio::test]
    async fn test_contains() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster.push(10, None, None).await.unwrap();
        vec_cluster.push(20, None, None).await.unwrap();
        let val = vec_cluster.contains(&[10, 20, 30]).await.unwrap();
        assert_eq!(val, vec![true, true, false]);
    }

    #[tokio::test]
    async fn test_mpush_ex() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster
            .mpush(
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
        let val = vec_cluster.get_all().await.unwrap();
        assert_eq!(val, Vec::new());
    }

    #[tokio::test]
    async fn test_mpush() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster
            .mpush(&[10, 20, 30], &[None, None, None], &[None, None, None])
            .await
            .unwrap();
        let mut val = vec_cluster.get_all().await.unwrap();
        val.sort();
        assert_eq!(val, Vec::from([10, 20, 30]));
    }

    #[tokio::test]
    async fn test_push_ex() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster.push(10, None, None).await.unwrap();
        vec_cluster
            .push(20, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val = vec_cluster.get_all().await.unwrap();
        assert_eq!(val, Vec::from([10]));
    }

    #[tokio::test]
    async fn test_push() {
        let vec_cluster = VecCacheCluster::new(32, 3).await;
        vec_cluster.push(10, None, None).await.unwrap();
        vec_cluster.push(20, None, None).await.unwrap();
        vec_cluster.push(30, None, None).await.unwrap();
        let mut val = vec_cluster.get_all().await.unwrap();
        val.sort();
        assert_eq!(val, Vec::from([10, 20, 30]));
    }
}
