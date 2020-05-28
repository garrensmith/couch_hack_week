use crate::constants::COUCHDB_PREFIX;
use crate::couch::*;
use foundationdb::Database as FdbDatabase;
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

// async fn with_db_info(
//     fdb: Arc<FdbDatabase>,
//     couch_directory: Vec<u8>,
// ) -> impl Filter<Extract = (DatabaseInfo,), Error = warp::reject::Rejection> + Clone {
//     let a = warp::any().and(warp::path::param()).map(|name: String| async {
//         let b = DatabaseInfo::new(fdb.clone(), couch_directory.as_slice(), name).await;
//         Ok(b)
//     });
//
//     a
// }

// fn with_db_info(
//     name: String,
//     fdb: Arc<FdbDatabase>,
//     couch_directory: &[u8]
// ) -> impl Filter<Extract = ((String, Arc<FdbDatabase>, &[u8],), Error = Infallible> + Clone {
//     warp::any().map(move || )
// }

// async fn with_db_info(fdb: Arc<FdbDatabase>, couch_directory: &[u8], name: String)
//     -> impl Filter<Extract = (DatabaseInfo,), Error = Infallible> + Clone {
//     let db_info = DatabaseInfo::new(fdb, couch_directory, name).await;
//     warp::any().map(move || db_info.clone())
// }

pub async fn routes() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let fdb = Arc::new(FdbDatabase::default().unwrap());

    let trx = fdb.create_trx().unwrap();
    let couch_directory = trx
        .get(COUCHDB_PREFIX, false)
        .await
        .unwrap()
        .unwrap()
        .to_vec();

    let hi_route = warp::get().and(warp::path("hi").map(|| "Hello, World!"));

    let all_dbs_route = warp::get()
        .and(warp::path("_all_dbs"))
        .and(with_fdb(fdb.clone()))
        .and_then(all_dbs_req);

    let all_docs_route = warp::path!(String / "_all_docs")
        .and(warp::get())
        .and(with_fdb(fdb.clone()))
        .and(with_couch_directory(couch_directory))
        .and_then(all_docs_req);

    let default_route = warp::get().and_then(home_req).and(warp::path::end());

    hi_route
        .or(all_dbs_route)
        .or(all_docs_route)
        .or(default_route)
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
    let trx = fdb.create_trx().unwrap();
    let dbs: Vec<String> = all_dbs(&trx)
        .await
        .unwrap()
        .iter()
        .map(|db| db.name.clone())
        .collect();
    Ok(warp::reply::json(&dbs))
}

pub async fn all_docs_req(
    name: String,
    fdb: Arc<FdbDatabase>,
    couch_directory: Vec<u8>,
) -> Result<impl Reply> {
    // let db_info = DatabaseInfo::new(fdb, couch_directory.as_slice(), name).await;
    let trx = fdb.create_trx().unwrap();
    let db = get_db(&trx, couch_directory.as_slice(), name.as_str())
        .await
        .unwrap();
    let docs = all_docs(&trx, &db).await.unwrap();

    let resp = json!({
        "total_rows": docs.len(),
        "off_set": "null",
        "rows": docs
    });

    Ok(warp::reply::json(&resp))
}
