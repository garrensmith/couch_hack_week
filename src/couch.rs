use crate::constants::{ALL_DBS, COUCHDB_PREFIX, DB_ALL_DOCS, DB_CHANGES, DB_STATS};
use crate::fdb;
use crate::fdb::unpack_with_prefix;
use crate::CouchError;

use crate::defs::{ChangeRow, Database, DbInfo, Id, Rev, Row};
use crate::util::bin_to_int;
use foundationdb::tuple::{unpack, Bytes, Element, Versionstamp};
use foundationdb::{KeySelector, RangeOption, Transaction};

pub type CouchResult<T> = Result<T, CouchError>;

pub async fn get_directory(trx: &Transaction) -> CouchResult<Vec<u8>> {
    let res = trx.get(COUCHDB_PREFIX, false).await?;

    match res {
        Some(val) => Ok(val.to_vec()),
        None => Err(CouchError::Missing("couch_directory".to_string())),
    }
}

pub async fn all_dbs(trx: &Transaction) -> CouchResult<Vec<Database>> {
    let couch_directory = get_directory(&trx).await?;

    let (start_key, end_key) = fdb::pack_key_range(&ALL_DBS, couch_directory.as_slice());

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

    let value = result.ok_or_else(|| CouchError::Missing(name.to_string()))?;

    let db = Database::new(Bytes::from(name), value.as_ref());
    Ok(db)
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

        // should maybe implement a from trait here instead
        let val = bin_to_int(kv.value());

        let acc = match (stat, size_stat) {
            (b"doc_count", _) => DbInfo {
                doc_count: val,
                ..db_info
            },
            (b"doc_del_count", _) => DbInfo {
                doc_del_count: val,
                ..db_info
            },
            (b"sizes", b"external") => DbInfo {
                size_external: val,
                ..db_info
            },
            (b"sizes", b"views") => DbInfo {
                size_views: val,
                ..db_info
            },
            _ => {
                println!("unknown reached");
                db_info
            }
        };
        Ok(acc)
    })
}

type EncodedRev<'a> = (i16, Bytes<'a>);
type EncodedChangeValue<'a> = (Bytes<'a>, bool, EncodedRev<'a>);

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
    range
        .iter()
        .map(|kv| {
            let (_, id_bytes): (i64, Vec<u8>) =
                unpack_with_prefix(&kv.key(), db.db_prefix.as_slice())?;

            let id: Id = id_bytes.as_slice().into();

            let rev_tuple: EncodedRev = unpack(kv.value())?;
            let rev: Rev = rev_tuple.into();

            Ok(Row::new(id, rev))
        })
        .collect::<CouchResult<Vec<Row>>>()
}

pub async fn changes(trx: &Transaction, db: &Database) -> CouchResult<Vec<ChangeRow>> {
    let (start, end) = fdb::pack_range(&DB_CHANGES, db.db_prefix.as_slice());
    let start_key = KeySelector::first_greater_than(start);
    let end_key = KeySelector::first_greater_than(end);

    let opts = RangeOption {
        mode: foundationdb::options::StreamingMode::WantAll,
        limit: Some(3),
        ..RangeOption::from((start_key, end_key))
    };

    let iteration: usize = 1;
    let range = trx.get_range(&opts, iteration, false).await?;
    range
        .iter()
        .map(|kv| {
            let (_, vs): (Element, Versionstamp) =
                unpack_with_prefix(&kv.key(), db.db_prefix.as_slice()).unwrap();
            let (id_bytes, deleted, encoded_rev): EncodedChangeValue = unpack(kv.value()).unwrap();

            let doc_id: Id = id_bytes.as_ref().into();
            let rev: Rev = encoded_rev.into();

            Ok(ChangeRow::new(doc_id, rev, vs.into(), deleted))
        })
        .collect::<CouchResult<Vec<ChangeRow>>>()
}
