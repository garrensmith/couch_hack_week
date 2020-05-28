use couch_hack_week::constants::*;
use couch_hack_week::couch::{all_dbs, get_db};
use couch_hack_week::http;
use foundationdb::Database as FdbDatabase;
use std::*;
use tokio::runtime::Runtime;

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let fdb = FdbDatabase::default().unwrap();

    let routes = http::routes().await;
    println!("Server running on port: 3030");
    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;

    // write a value
    let trx = fdb.create_trx().unwrap();
    trx.set(b"hello", b"world"); // errors will be returned in the future result
    trx.commit().await.unwrap();

    // read a value
    let trx = fdb.create_trx().unwrap();
    let maybe_value = trx.get(b"hello", false).await.unwrap();

    let value = maybe_value.unwrap(); // unwrap the option

    let couch_directory = trx.get(COUCHDB_PREFIX, false).await.unwrap().unwrap();
    let s = String::from_utf8_lossy(&couch_directory.as_ref());
    println!("dd {:?}", s);

    let dbs = all_dbs(&trx).await.unwrap();

    dbs.iter().for_each(|db| println!("db: {:?}", db.name));
    let db = get_db(
        &trx,
        &couch_directory.as_ref(),
        String::from("all-docs-db").as_str(),
    )
    .await
    .unwrap();
    println!("woo db {:?}", db.name);

    // all_docs(&trx, &dbs[0]).await?;

    assert_eq!(b"world", &value.as_ref());
    Ok(())
}

fn main() {
    foundationdb::boot(|| {
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            async_main().await.unwrap();
        });
    });
}
