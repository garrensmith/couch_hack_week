use couch_hack_week::couch::{get_db, get_directory};
use foundationdb::Database as FdbDatabase;
use tokio::runtime::Runtime;

#[test]
fn test_missing_database() {
    foundationdb::boot(|| {
        let mut rt = Runtime::new().unwrap();
        rt.block_on(test_missing_database_async())
    });

    assert!(true);
}

async fn test_missing_database_async() {
    let fdb = FdbDatabase::default().unwrap();
    let trx = fdb.create_trx().unwrap();
    let directory = get_directory(&trx).await.unwrap();
    let db_name = "this-should-not-exist-123".to_string();

    let res = get_db(&trx, directory.as_slice(), db_name.as_str())
        .await;

    assert!(res.is_err());
}
