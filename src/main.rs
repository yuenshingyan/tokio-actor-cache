pub mod tokio_actor_cache {
    mod data_struct;
    pub mod error;
    pub mod hm;
    pub mod hs;
    pub mod vec;
}
pub mod unittests {
    pub mod hm;
    pub mod hs;
}

#[tokio::main]
async fn main() {
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
