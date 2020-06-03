use crate::couch::*;
use crate::{handle_rejection, CouchError};
use foundationdb::{Database as FdbDatabase, Transaction};
use serde_json::json;
use std::convert::Infallible;
use std::sync::Arc;
use warp::{Filter, Reply};

type Result<T> = std::result::Result<T, warp::Rejection>;

fn with_fdb(
    fdb: Arc<FdbDatabase>,
) -> impl Filter<Extract = (Arc<FdbDatabase>,), Error = Infallible> + Clone {
    warp::any().map(move || fdb.clone())
}

fn with_couch_directory(
    couch_directory: Vec<u8>,
) -> impl Filter<Extract = (Vec<u8>,), Error = Infallible> + Clone {
    warp::any().map(move || couch_directory.clone())
}

pub async fn routes() -> impl Filter<Extract = impl warp::Reply, Error = Infallible> + Clone {
    let fdb = Arc::new(FdbDatabase::default().unwrap());

    let trx = fdb.create_trx().unwrap();
    let couch_directory = get_directory(&trx).await.unwrap();

    let all_dbs_route = warp::get()
        .and(warp::path("_all_dbs"))
        .and(with_fdb(fdb.clone()))
        .and_then(all_dbs_req);

    let db_info_route = warp::get()
        .and(warp::path!(String))
        .and(warp::path::end())
        .and(with_fdb(fdb.clone()))
        .and(with_couch_directory(couch_directory.clone()))
        .and_then(db_info_req);

    let all_docs_route = warp::path!(String / "_all_docs")
        .and(warp::get())
        .and(with_fdb(fdb.clone()))
        .and(with_couch_directory(couch_directory.clone()))
        .and_then(all_docs_req);

    let changes_route = warp::path!(String / "_changes")
        .and(warp::get())
        .and(warp::path::end())
        .and(with_fdb(fdb.clone()))
        .and(with_couch_directory(couch_directory))
        .and_then(changes_req);

    let default_route = warp::get().and_then(home_req).and(warp::path::end());

    default_route
        .or(all_dbs_route)
        .or(all_docs_route)
        .or(db_info_route)
        .or(changes_route)
        .recover(handle_rejection)
}

pub async fn home_req() -> Result<impl Reply> {
    let welcome = json!({
      "couchdb": "Welcome",
      "version": "4.x",
      "vendor": {
        "name": "Hack-week",
        "version": "0.1"
      },
      "features": [
        "fdb"
      ],
      "features_flags": []
    });

    Ok(warp::reply::json(&welcome))
}

pub async fn all_dbs_req(fdb: Arc<FdbDatabase>) -> Result<impl Reply> {
    let trx = create_trx(fdb)?;
    let dbs: Vec<String> = all_dbs(&trx)
        .await
        .unwrap()
        .iter()
        .map(|db| db.name.clone())
        .collect();
    Ok(warp::reply::json(&dbs))
}

pub async fn db_info_req(
    name: String,
    fdb: Arc<FdbDatabase>,
    couch_directory: Vec<u8>,
) -> Result<impl Reply> {
    // let db_info = DatabaseInfo::new(fdb, couch_directory.as_slice(), name).await;
    let trx = create_trx(fdb)?;
    let db = get_db(&trx, couch_directory.as_slice(), name.as_str()).await?;

    let info = db_info(&trx, &db).await.unwrap();

    let resp = json!({
       "cluster": {
        "n": 0,
        "q": 0,
        "r": 0,
        "w": 0
      },
      "compact_running": false,
      "data_size": 0,
      "db_name": name,
      "disk_format_version": 0,
      "disk_size": 0,
      "instance_start_time": "0",
      "purge_seq": 0,
      "update_seq": "000004b4764f07a400000000",
      "doc_del_count": info.doc_del_count,
      "doc_count": info.doc_count,
      "sizes": {
        "external": info.size_external,
        "views": info.size_views
      }
    });

    Ok(warp::reply::json(&resp))
}

pub async fn all_docs_req(
    name: String,
    fdb: Arc<FdbDatabase>,
    couch_directory: Vec<u8>,
) -> Result<impl Reply> {
    let trx = create_trx(fdb)?;
    let db = get_db(&trx, couch_directory.as_slice(), name.as_str()).await?;
    let docs = all_docs(&trx, &db).await?;

    let resp = json!({
        "total_rows": docs.len(),
        "off_set": "null",
        "rows": docs
    });

    Ok(warp::reply::json(&resp))
}

pub async fn changes_req(
    name: String,
    fdb: Arc<FdbDatabase>,
    couch_directory: Vec<u8>,
) -> Result<impl Reply> {
    let trx = create_trx(fdb)?;
    let db = get_db(&trx, couch_directory.as_slice(), name.as_str()).await?;
    let results = changes(&trx, &db).await?;

    let last_seq = &results.last().unwrap().seq;

    let resp = json!({
        "results": results,
        "last_seq": last_seq,
        "pending": "null"
    });

    Ok(warp::reply::json(&resp))
}

// A wrapper around creating a transaction so that we return a CouchError instead of a FdbError
fn create_trx(fdb: Arc<FdbDatabase>) -> std::result::Result<Transaction, CouchError> {
    match fdb.create_trx() {
        Ok(trx) => Ok(trx),
        Err(error) => Err(CouchError::from(error)),
    }
}
