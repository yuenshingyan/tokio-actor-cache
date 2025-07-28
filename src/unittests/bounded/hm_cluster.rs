#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::tokio_cache::bounded::hm::HashMapCacheCluster;

    #[tokio::test]
    async fn test_try_ttl() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster
            .insert("a", 10, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        let ttl = hm_cluster.try_ttl(&["a", "b"]).await.unwrap();
        assert!(Some(Duration::from_secs(1)) > ttl[0]);
        assert_eq!(ttl[1], None);
    }

    #[tokio::test]
    async fn test_try_clear() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.insert("a", 10, None, None).await.unwrap();
        hm_cluster.insert("b", 12, None, None).await.unwrap();
        hm_cluster.insert("c", 20, None, None).await.unwrap();
        let hm = hm_cluster.get_all().await.unwrap();
        assert_eq!(!hm.is_empty(), true);
        hm_cluster.try_clear().await.unwrap();
        let hm = hm_cluster.get_all().await.unwrap();
        assert_eq!(hm.is_empty(), true);
    }

    #[tokio::test]
    async fn test_try_mget() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[Some(true), Some(true), Some(true)],
            )
            .await
            .unwrap();
        let vals = hm_cluster.try_mget(&["a", "b", "c", "d"]).await.unwrap();
        assert_eq!(vals, vec![Some(10), Some(20), Some(30), None]);
    }

    #[tokio::test]
    async fn test_try_remove() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[None, None, None],
            )
            .await
            .unwrap();
        let vals = hm_cluster.try_remove(&["a", "b", "c", "d"]).await.unwrap();
        assert_eq!(vals, vec![Some(10), Some(20), Some(30), None]);
    }

    #[tokio::test]
    async fn test_try_contains_keys() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[None, None, None],
            )
            .await
            .unwrap();
        let is_contains_keys = hm_cluster
            .try_contains_key(&["a", "b", "c", "d"])
            .await
            .unwrap();
        assert_eq!(is_contains_keys, vec![true, true, true, false]);
    }

    #[tokio::test]
    async fn test_try_minsert_nx_if_not_exists() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.try_insert("a", 10, None, None).await.unwrap();
        hm_cluster
            .try_minsert(
                &["a", "b", "c"],
                &[20, 20, 30],
                &[None, None, None],
                &[None, None, None],
            )
            .await
            .unwrap();
        let val = hm_cluster.get("a").await.unwrap();
        assert_eq!(val, Some(20));
    }

    #[tokio::test]
    async fn test_try_minsert_nx_if_exists() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.try_insert("a", 10, None, None).await.unwrap();
        hm_cluster
            .try_minsert(
                &["a", "b", "c"],
                &[20, 20, 30],
                &[None, None, None],
                &[Some(true), Some(true), Some(true)],
            )
            .await
            .unwrap();
        let val = hm_cluster.get("a").await.unwrap();
        assert_eq!(val, Some(10));
    }

    #[tokio::test]
    async fn test_try_minsert_ex() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster
            .try_minsert(
                &["a", "b", "c"],
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
        let val_a = hm_cluster.get("a").await.unwrap();
        let val_b = hm_cluster.get("b").await.unwrap();
        let val_c = hm_cluster.get("c").await.unwrap();
        assert_eq!(val_a, None);
        assert_eq!(val_b, None);
        assert_eq!(val_c, None);
    }

    #[tokio::test]
    async fn test_try_minsert() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster
            .try_minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[None, None, None],
            )
            .await
            .unwrap();
        let val_a = hm_cluster.get("a").await.unwrap();
        let val_b = hm_cluster.get("b").await.unwrap();
        let val_c = hm_cluster.get("c").await.unwrap();
        assert_eq!(val_a, Some(10));
        assert_eq!(val_b, Some(20));
        assert_eq!(val_c, Some(30));
    }

    #[tokio::test]
    async fn test_try_minsert_inconsistent_len() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        let res = hm_cluster
            .try_minsert(
                &["a", "b"],
                &[10, 20, 30],
                &[None, None, None],
                &[None, None, None],
            )
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_try_insert_nx_if_not_exists() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.try_insert("a", 10, None, None).await.unwrap();
        hm_cluster.try_insert("a", 20, None, None).await.unwrap();
        let val = hm_cluster.get("a").await.unwrap();
        assert_eq!(val, Some(20));
    }

    #[tokio::test]
    async fn test_try_insert_nx_if_exists() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.try_insert("a", 10, None, None).await.unwrap();
        hm_cluster
            .try_insert("a", 20, None, Some(true))
            .await
            .unwrap();
        let val = hm_cluster.get("a").await.unwrap();
        assert_eq!(val, Some(10));
    }

    #[tokio::test]
    async fn test_try_insert_ex() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.try_insert("a", 10, None, None).await.unwrap();
        hm_cluster
            .try_insert("b", 20, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val_a = hm_cluster.get("a").await.unwrap();
        let val_b = hm_cluster.get("b").await.unwrap();
        assert_eq!(val_a, Some(10));
        assert_eq!(val_b, None);
    }

    #[tokio::test]
    async fn test_try_insert() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.try_insert("a", 10, None, None).await.unwrap();
        let val = hm_cluster.get("a").await.unwrap();
        assert_eq!(val, Some(10));
    }

    #[tokio::test]
    async fn test_hash_id() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        let keys = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
            "f".to_string(),
            "g".to_string(),
        ];
        for (k, v) in keys.into_iter().enumerate() {
            hm_cluster.insert(k, v.clone(), None, None).await.unwrap();
            let val = hm_cluster.get(k).await.unwrap();
            assert_eq!(val, Some(v));
        }
    }

    #[tokio::test]
    async fn test_ttl() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster
            .insert("a", 10, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        let ttl = hm_cluster.ttl(&["a", "b"]).await.unwrap();
        assert!(Some(Duration::from_secs(1)) > ttl[0]);
        assert_eq!(ttl[1], None);
    }

    #[tokio::test]
    async fn test_clear() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.insert("a", 10, None, None).await.unwrap();
        hm_cluster.insert("b", 12, None, None).await.unwrap();
        hm_cluster.insert("c", 20, None, None).await.unwrap();
        let hm = hm_cluster.get_all().await.unwrap();
        assert_eq!(!hm.is_empty(), true);
        hm_cluster.clear().await.unwrap();
        let hm = hm_cluster.get_all().await.unwrap();
        assert_eq!(hm.is_empty(), true);
    }

    #[tokio::test]
    async fn test_mget() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[Some(true), Some(true), Some(true)],
            )
            .await
            .unwrap();
        let vals = hm_cluster.mget(&["a", "b", "c", "d"]).await.unwrap();
        assert_eq!(vals, vec![Some(10), Some(20), Some(30), None]);
    }

    #[tokio::test]
    async fn test_remove() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[None, None, None],
            )
            .await
            .unwrap();
        let vals = hm_cluster.remove(&["a", "b", "c", "d"]).await.unwrap();
        assert_eq!(vals, vec![Some(10), Some(20), Some(30), None]);
    }

    #[tokio::test]
    async fn test_contains_keys() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[None, None, None],
            )
            .await
            .unwrap();
        let is_contains_keys = hm_cluster
            .contains_key(&["a", "b", "c", "d"])
            .await
            .unwrap();
        assert_eq!(is_contains_keys, vec![true, true, true, false]);
    }

    #[tokio::test]
    async fn test_minsert_nx_if_not_exists() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.insert("a", 10, None, None).await.unwrap();
        hm_cluster
            .minsert(
                &["a", "b", "c"],
                &[20, 20, 30],
                &[None, None, None],
                &[None, None, None],
            )
            .await
            .unwrap();
        let val = hm_cluster.get("a").await.unwrap();
        assert_eq!(val, Some(20));
    }

    #[tokio::test]
    async fn test_minsert_nx_if_exists() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.insert("a", 10, None, None).await.unwrap();
        hm_cluster
            .minsert(
                &["a", "b", "c"],
                &[20, 20, 30],
                &[None, None, None],
                &[Some(true), Some(true), Some(true)],
            )
            .await
            .unwrap();
        let val = hm_cluster.get("a").await.unwrap();
        assert_eq!(val, Some(10));
    }

    #[tokio::test]
    async fn test_minsert_ex() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster
            .minsert(
                &["a", "b", "c"],
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
        let val_a = hm_cluster.get("a").await.unwrap();
        let val_b = hm_cluster.get("b").await.unwrap();
        let val_c = hm_cluster.get("c").await.unwrap();
        assert_eq!(val_a, None);
        assert_eq!(val_b, None);
        assert_eq!(val_c, None);
    }

    #[tokio::test]
    async fn test_minsert() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster
            .minsert(
                &["a", "b", "c"],
                &[10, 20, 30],
                &[None, None, None],
                &[None, None, None],
            )
            .await
            .unwrap();
        let val_a = hm_cluster.get("a").await.unwrap();
        let val_b = hm_cluster.get("b").await.unwrap();
        let val_c = hm_cluster.get("c").await.unwrap();
        assert_eq!(val_a, Some(10));
        assert_eq!(val_b, Some(20));
        assert_eq!(val_c, Some(30));
    }

    #[tokio::test]
    async fn test_minsert_inconsistent_len() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        let res = hm_cluster
            .minsert(
                &["a", "b"],
                &[10, 20, 30],
                &[None, None, None],
                &[None, None, None],
            )
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_insert_nx_if_not_exists() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.insert("a", 10, None, None).await.unwrap();
        hm_cluster.insert("a", 20, None, None).await.unwrap();
        let val = hm_cluster.get("a").await.unwrap();
        assert_eq!(val, Some(20));
    }

    #[tokio::test]
    async fn test_insert_nx_if_exists() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.insert("a", 10, None, None).await.unwrap();
        hm_cluster.insert("a", 20, None, Some(true)).await.unwrap();
        let val = hm_cluster.get("a").await.unwrap();
        assert_eq!(val, Some(10));
    }

    #[tokio::test]
    async fn test_insert_ex() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.insert("a", 10, None, None).await.unwrap();
        hm_cluster
            .insert("b", 20, Some(Duration::from_secs(1)), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let val_a = hm_cluster.get("a").await.unwrap();
        let val_b = hm_cluster.get("b").await.unwrap();
        assert_eq!(val_a, Some(10));
        assert_eq!(val_b, None);
    }

    #[tokio::test]
    async fn test_insert() {
        let hm_cluster = HashMapCacheCluster::new(32, 3).await;
        hm_cluster.insert("a", 10, None, None).await.unwrap();
        let val = hm_cluster.get("a").await.unwrap();
        assert_eq!(val, Some(10));
    }
}
