use foundationdb::tuple::{unpack, Bytes, Element};
use foundationdb::KeySelector;
use foundationdb::{RangeOption, Transaction};
use std::*;
use tokio::runtime::Runtime;

const COUCHDB_PREFIX: &[u8; 14] = b"\xfe\x01\xfe\x00\x14\x02couchdb\x00";

struct Database {
    name: String,
    db_prefix: Vec<u8>,
}

impl Database {
    fn new(name: &Bytes, db_prefix: &[u8]) -> Database {
        Database {
            name: String::from_utf8_lossy(name.as_ref()).into(),
            db_prefix: db_prefix.to_vec(),
        }
    }
}

async fn get_dbs(trx: Transaction) -> Result<Vec<Database>, Box<dyn std::error::Error>> {
    let couch_directory = trx.get(COUCHDB_PREFIX, false).await.unwrap().unwrap();

    let end_slice = [couch_directory.as_ref(), b"\xFF"].concat();
    let start = KeySelector::first_greater_or_equal(couch_directory.as_ref());
    let end = KeySelector::first_greater_or_equal(end_slice);

    let opts = RangeOption::from((start, end));
    let iteration: usize = 1;
    let range = trx.get_range(&opts, iteration, false).await?;

    let dbs = range
        .iter()
        .map(|kv| {
            let (_, _, db_bytes): (Element, Element, Bytes) = unpack(kv.key()).unwrap();
            Database::new(&db_bytes, kv.value())
        })
        .collect();

    Ok(dbs)
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let fdb = foundationdb::Database::default().unwrap();

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

    let dbs = get_dbs(trx).await.unwrap();

    dbs.iter().for_each(|db| println!("db: {:?}", db.name));

    assert_eq!(b"world", &value.as_ref());
    Ok(())
}

fn main() {
    foundationdb::boot(|| {
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("boom");
            async_main().await.unwrap();
        });
    });
}
