pub mod tokio_actor_cache {
    mod data_struct;
    pub mod error;
    pub mod hm;
    pub mod hs;
    pub mod vec;
}
pub mod unittests {
    pub mod hs;
}

use crate::tokio_actor_cache::{hm::HashMapCache, hs::HashSetCache, vec::VecCache};

#[tokio::main]
async fn main() {
    // let hs_cache = HashSetCache::new(32).await;

    // // HSET val in hs.
    // hs_cache
    //     .insert(99, Some(Duration::from_secs(3)))
    //     .await
    //     .expect("failed to insert into hs");
    // hs_cache
    //     .insert(98, Some(Duration::from_secs(1)))
    //     .await
    //     .expect("failed to insert into hs");
    // hs_cache
    //     .insert(97, None)
    //     .await
    //     .expect("failed to insert into hs");

    // tokio::time::sleep(Duration::from_secs(2)).await;

    // // HGET
    // let hs = hs_cache.get().await;
    // println!("{:?}", hs);

    // let vec_cache = VecCache::<i32>::new(32).await;

    // //
    // vec_cache
    //     .push(99, Some(Duration::from_secs(3)))
    //     .await
    //     .expect("failed to insert into vec");
    // vec_cache
    //     .push(99, Some(Duration::from_secs(1)))
    //     .await
    //     .expect("failed to insert into vec");
    // vec_cache
    //     .push(99, None)
    //     .await
    //     .expect("failed to insert into vec");

    // tokio::time::sleep(Duration::from_secs(2)).await;

    // let res = vec_cache.get().await.expect("failed to get vec");
    // println!("{:?}", res);
}