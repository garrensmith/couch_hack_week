use crate::constants::{ALL_DBS, COUCHDB_PREFIX, DB_ALL_DOCS, DB_STATS};
use crate::fdb;
use crate::fdb::unpack_with_prefix;
use crate::CouchError;

use foundationdb::tuple::{unpack, Bytes, Element};
use foundationdb::{KeySelector, RangeOption, Transaction};
use serde::Serialize;

pub type CouchResult<T> = Result<T, CouchError>;

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
            db_prefix: db_prefix.to_vec(),
        }
    }
}

#[derive(Clone, Serialize)]
pub struct Row {
    id: String,
    key: String,
    value: String,
}

impl Row {
    pub fn new<S>(raw: S, value: String) -> Self
    where
        S: Into<String>,
    {
        let id = raw.into();
        Row {
            key: id.clone(),
            id,
            value,
        }
    }
}

pub async fn get_directory(trx: &Transaction) -> CouchResult<Vec<u8>> {
    let res = trx.get(COUCHDB_PREFIX, false).await?;

    match res {
        Some(val) => Ok(val.to_vec()),
        None => Err(CouchError::Missing("couch_directory".to_string())),
    }
}

pub async fn all_dbs(trx: &Transaction) -> CouchResult<Vec<Database>> {
    let couch_directory = get_directory(&trx).await?;

    let (start, end) = fdb::pack_range(&ALL_DBS, couch_directory.as_slice());

    let start_key = KeySelector::first_greater_or_equal(start);
    let end_key = KeySelector::first_greater_than(end);
    let opts = RangeOption {
        mode: foundationdb::options::StreamingMode::WantAll,
        ..RangeOption::from((start_key, end_key))
    };
    let iteration: usize = 1;
    let range = trx.get_range(&opts, iteration, false).await?;

    let dbs = range
        .iter()
        .map(|kv| {
            let (_, _, db_bytes): (Element, Element, Bytes) = unpack(kv.key())?;
            Ok(Database::new(db_bytes, kv.value()))
        })
        .collect::<CouchResult<Vec<Database>>>()?;

    Ok(dbs)
}

pub async fn get_db(
    trx: &Transaction,
    couch_directory: &[u8],
    name: &str,
) -> CouchResult<Database> {
    let key = fdb::pack_with_prefix(&(ALL_DBS, Bytes::from(name)), couch_directory);
    let result = trx.get(key.as_slice(), false).await?;

    let value = result.ok_or(CouchError::Missing(name.to_string()))?;

    let db = Database::new(Bytes::from(name), value.as_ref());
    Ok(db)
}

async fn dbs_info(trx: &Transaction, db: &Database) -> CouchResult<Vec<String>> {
    let (start, end) = fdb::pack_range(&DB_STATS, db.db_prefix.as_slice());
    let start_key = KeySelector::first_greater_or_equal(start);
    let end_key = KeySelector::first_greater_than(end);
    let opts = RangeOption {
        mode: foundationdb::options::StreamingMode::WantAll,
        ..RangeOption::from((start_key, end_key))
    };

    let iteration: usize = 1;
    let range = trx.get_range(&opts, iteration, false).await?;
    let rows: Vec<String> = range
        .iter()
        .map(|kv| {
            let meta_key = &kv.key()[db.db_prefix.len()..];
            let (_, meta_keyname): (i16, Bytes) = unpack(meta_key).unwrap_or_else(|error| {
                let (prefix, _, key_name): (i16, Bytes, Bytes) = unpack(meta_key).unwrap();
                (prefix, key_name)
            });
            let id: String = String::from_utf8_lossy(meta_keyname.as_ref()).into();

            let value = &kv.value()[0];
            println!("db info {:?}: {:?}", id, value);
            Ok(id)
        })
        .collect::<CouchResult<Vec<String>>>()?;

    Ok(rows)
}

pub async fn all_docs(trx: &Transaction, db: &Database) -> CouchResult<Vec<Row>> {
    let (start, end) = fdb::pack_range(&DB_ALL_DOCS, db.db_prefix.as_slice());
    let start_key = KeySelector::first_greater_or_equal(start);
    let end_key = KeySelector::first_greater_than(end);
    let opts = RangeOption {
        mode: foundationdb::options::StreamingMode::WantAll,
        ..RangeOption::from((start_key, end_key))
    };

    let iter: usize = 1;
    let range = trx.get_range(&opts, iter, false).await?;
    let rows = range
        .iter()
        .map(|kv| {
            let (_, id_bytes): (i64, Vec<u8>) =
                unpack_with_prefix(&kv.key(), db.db_prefix.as_slice())?;

            let id = String::from_utf8_lossy(id_bytes.as_ref());

            let (rev_num, raw_rev_str): (i16, Bytes) = unpack(kv.value())?;
            let rev_str = format!("{}-{}", rev_num, hex::encode(raw_rev_str.as_ref()));

            Ok(Row::new(id, rev_str))
        })
        .collect::<CouchResult<Vec<Row>>>()?;

    Ok(rows)
}
