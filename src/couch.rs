use crate::constants::{ALL_DBS, COUCHDB_PREFIX, CHANGE_VS_STAMP, DB_ALL_DOCS, DB_STATS};
use crate::fdb;
use crate::fdb::unpack_with_prefix;
use crate::CouchError;

use foundationdb::tuple::{unpack, Bytes, Element};
use foundationdb::{KeySelector, RangeOption, Transaction};
use serde::Serialize;

use byteorder::{ReadBytesExt, LittleEndian};

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


#[derive(Clone, Serialize)]
pub struct FeedRow {
    seq: String,
    id: String,
    rev: String,
}

impl FeedRow {

    pub fn new(seq: String, id: String, rev: String) -> Self {
        FeedRow {
            seq,
            id,
            rev
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

pub struct DbInfo {
    pub doc_count: u64,
    pub size_views: u64,
    pub size_external: u64,
    pub doc_del_count: u64,
}

impl DbInfo {
    pub fn default() -> Self {
        Self {
            doc_count: 0,
            size_views: 0,
            size_external: 0,
            doc_del_count: 0,
        }
    }
}

fn bin_to_int(mut bin: &[u8]) -> u64 {
    match bin.read_u64::<LittleEndian>() {
        Ok(num) => num,
        Err(_) => 0
    }
}

pub async fn db_info(trx: &Transaction, db: &Database) -> CouchResult<DbInfo> {
    let (start, end) = fdb::pack_range(&DB_STATS, db.db_prefix.as_slice());
    let start_key = KeySelector::first_greater_or_equal(start);
    let end_key = KeySelector::first_greater_than(end);
    let opts = RangeOption {
        mode: foundationdb::options::StreamingMode::WantAll,
        ..RangeOption::from((start_key, end_key))
    };

    let iteration: usize = 1;
    let range = trx.get_range(&opts, iteration, false).await?;
    range.iter().try_fold(DbInfo::default(), |db_info, kv| {

        let tup: Vec<Element> = unpack_with_prefix(&kv.key(), db.db_prefix.as_slice())?;
        let stat = tup[1].as_bytes().unwrap().as_ref();

        let size_stat = if tup.len() == 3 {
            tup[2].as_bytes().unwrap().as_ref()
        } else {
            b"none"
        };

        let val = bin_to_int(kv.value());

        let acc = match (stat, size_stat) {
            (b"doc_count", _) =>
                DbInfo {
                    doc_count: val,
                    ..db_info
                },
            (b"doc_del_count", _) =>
                DbInfo {
                    doc_del_count: val,
                    ..db_info
                },
            (b"sizes", b"external") =>
                DbInfo {
                    size_external: val,
                    ..db_info
                },
            (b"sizes", b"views") =>
                DbInfo {
                    size_views: val,
                    ..db_info
                },
            _ => {
                println!("unknown reached");
                db_info
            },
        };
        Ok(acc)
    })
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

pub async fn changes(
    trx: &Transaction,
    db: &Database,
) -> Result<Vec<FeedRow>, Box<dyn std::error::Error>> {
    let (start, end) = fdb::pack_change_range(db.db_prefix.as_slice(), CHANGE_VS_STAMP);
    let start_key = KeySelector::first_greater_than(start);
    let end_key = KeySelector::first_greater_than(end);

    let opts = RangeOption {
        mode: foundationdb::options::StreamingMode::WantAll,
        ..RangeOption::from((start_key, end_key))
    };

    let iteration: usize = 1;
    let range = trx.get_range(&opts, iteration, false).await?;
    let rows: Vec<FeedRow> = range
        .iter()
        .map(|kv| {
            let seq_key = &kv.key()[db.db_prefix.len() + 3..];
            let seq_str = format!("{}", hex::encode(seq_key.as_ref()));

            let value = &kv.value();
            let (left, rev_byte) = value.split_at(value.len() - 18);
            let rev_str = format!("{}", hex::encode(rev_byte.as_ref()));

            let rev = hex::encode(rev_byte.as_ref());
            let (prefix_docid, _) = left.split_at(left.len() - 6);
            let (_, doc_id) = prefix_docid.split_at(1);
            let id: String = String::from_utf8_lossy(doc_id.as_ref()).into();
            FeedRow::new(seq_str, id, rev_str)

        })
        .collect();

    Ok(rows)
}

pub async fn changes_seq(
    start_seq: &String,
    trx: &Transaction,
    db: &Database,
) -> Result<Vec<FeedRow>, Box<dyn std::error::Error>> {
    let (start, end) = fdb::pack_change_seq_range(
        db.db_prefix.as_slice(),
        CHANGE_VS_STAMP,
        start_seq.as_bytes()
    );
    let start_key = KeySelector::first_greater_than(start);
    let end_key = KeySelector::first_greater_than(end);

    let opts = RangeOption {
        mode: foundationdb::options::StreamingMode::WantAll,
        ..RangeOption::from((start_key, end_key))
    };

    let iteration: usize = 1;
    let range = trx.get_range(&opts, iteration, false).await?;
    let rows: Vec<FeedRow> = range
        .iter()
        .map(|kv| {
            let seq_key = &kv.key()[db.db_prefix.len() + 3..];
            let seq_str = format!("{}", hex::encode(seq_key.as_ref()));

            let value = &kv.value();
            let (left, rev_byte) = value.split_at(value.len() - 18);
            let rev_str = format!("{}", hex::encode(rev_byte.as_ref()));

            let rev = hex::encode(rev_byte.as_ref());
            let (prefix_docid, _) = left.split_at(left.len() - 6);
            let (_, doc_id) = prefix_docid.split_at(1);
            let id: String = String::from_utf8_lossy(doc_id.as_ref()).into();
            FeedRow::new(seq_str, id, rev_str)

        })
        .collect();

    Ok(rows)
}
