use crate::constants::{ALL_DBS, COUCHDB_PREFIX, DB_ALL_DOCS};
use crate::fdb;

use serde::{Serialize};
use foundationdb::future::FdbValue;
use foundationdb::Database as FdbDatabase;
use foundationdb::tuple::{unpack, Bytes, Element};
use foundationdb::{KeySelector, RangeOption, Transaction};
use std::sync::Arc;


// #[derive(Clone)]
pub struct DatabaseInfo {
    name: String,
    db_prefix: Vec<u8>,
    fdb: Arc<FdbDatabase>,
    transaction: Option<Transaction>
}

// impl<'a> From<'a Bytes> for String {
//     fn from(bytes: &'a Bytes) -> Self {
//         String::from_utf8_lossy(bytes.as_bytes()).into()
//     }
//
// }


impl DatabaseInfo {
    pub async fn new(fdb: Arc<FdbDatabase>, couch_directory: &[u8], name: String) -> Self{
        let trx = fdb.create_trx().unwrap();
        let db = get_db(&trx, couch_directory, name.as_str()).await.unwrap();

        DatabaseInfo {
            name,
            transaction: None,
            db_prefix: db.db_prefix,
            fdb: fdb.clone()
        }
    }

    // pub fn create_trx(&mut self) -> &Transaction {
    //     match self.transaction {
    //         Some(trx) => &trx,
    //         None => {
    //             let trx = self.fdb.create_trx().unwrap();
    //             self.transaction = Some(trx);
    //             &trx
    //         }
    //     }
    // }
}

#[derive(Clone, Serialize)]
pub struct Database {
    pub name: String,
    #[serde(skip_serializing)]
    db_prefix: Vec<u8>,
}

impl Database {
    // pub fn new<U: Into<String>>(name: &U, db_prefix: &[u8]) -> Database {
    pub fn new(name: Bytes, db_prefix: &[u8]) -> Database {
        Database {
            name: String::from_utf8_lossy(name.as_ref()).into(),
            db_prefix: db_prefix.to_vec()
        }
    }
}

#[derive(Clone, Serialize)]
pub struct Row {
    id: String,
    key: String,
    value: String,
}

impl From<FdbValue> for Row {
    fn from(kv: FdbValue) -> Self {
        println!("kb {:?}", kv);
        let (_, _, raw_key): (Element, Element, Bytes) = unpack(kv.key()).unwrap();
        let key: String = String::from_utf8_lossy(raw_key.as_ref()).into();
        let value: String = String::from_utf8_lossy(kv.value()).into();

        Row {
            id: key.clone(),
            key,
            value,
        }
    }
}

pub async fn all_dbs(trx: &Transaction) -> Result<Vec<Database>, Box<dyn std::error::Error>> {
    let couch_directory = trx.get(COUCHDB_PREFIX, false).await.unwrap().unwrap();

    let (start, end) = fdb::pack_range(&ALL_DBS, couch_directory.as_ref());

    let start_key = KeySelector::first_greater_or_equal(start);
    let end_key = KeySelector::first_greater_than(end);
    let opts = RangeOption {
        mode: foundationdb::options::StreamingMode::WantAll,
        ..RangeOption::from((start_key, end_key))
    };
    let iteration: usize = 1;
    let range = trx.get_range(&opts, iteration, false).await?;

    let dbs: Vec<Database> = range
        .iter()
        .map(|kv| {
            let (_, _, db_bytes): (Element, Element, Bytes) = unpack(kv.key()).unwrap();
            Database::new(db_bytes, kv.value())
        })
        .collect();

    Ok(dbs)
}

pub async fn get_db(
    trx: &Transaction,
    couch_directory: &[u8],
    name: &str,
) -> Result<Database, Box<dyn std::error::Error>> {
    let key = fdb::pack_with_prefix(&(ALL_DBS, Bytes::from(name)), couch_directory);
    let value = trx.get(key.as_slice(), false).await.unwrap().unwrap();
    let db = Database::new(Bytes::from(name), value.as_ref());
    Ok(db)
}

pub async fn all_docs(
    trx: &Transaction,
    db: &Database,
) -> Result<Vec<Row>, Box<dyn std::error::Error>> {
    let (start, end) = fdb::pack_range(&DB_ALL_DOCS, db.db_prefix.as_slice());
    let start_key = KeySelector::first_greater_or_equal(start);
    let end_key = KeySelector::first_greater_than(end);
    let opts = RangeOption {
        mode: foundationdb::options::StreamingMode::WantAll,
        ..RangeOption::from((start_key, end_key))
    };

    let iter: usize = 1;
    let range = trx.get_range(&opts, iter, false).await?;
    let rows: Vec<Row> = range
        .iter()
        .map(|kv| {
            let key = &kv.key()[db.db_prefix.len()..];
            let (_, id_bytes): (i64, Bytes) = unpack(key).unwrap();

            let id: String = String::from_utf8_lossy(id_bytes.as_ref()).into();

            let (rev_num, raw_rev_str): (i16, Bytes) = unpack(kv.value()).unwrap();
            let rev_str = format!("{}-{}", rev_num, hex::encode(raw_rev_str.as_ref()));

            let row = Row {
                key: id.clone(),
                id,
                value: rev_str,
            };
            println!("kv {:?} {:?}", row.id, row.value);
            row
        })
        .collect();

    Ok(rows)
}
