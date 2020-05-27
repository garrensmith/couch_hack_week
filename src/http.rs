use crate::couch::*;
use foundationdb::Database as FdbDatabase;
use std::convert::Infallible;
use std::sync::Arc;
use warp::{Filter, Reply};
use serde_json::json;

type Result<T> = std::result::Result<T, warp::Rejection>;

fn with_fdb(
    fdb: Arc<FdbDatabase>,
) -> impl Filter<Extract = (Arc<FdbDatabase>,), Error = Infallible> + Clone {
    warp::any().map(move || fdb.clone())
}

pub fn routes() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let fdb = Arc::new(FdbDatabase::default().unwrap());
    let hi = warp::get().and(warp::path("hi").map(|| "Hello, World!"));
    let all_dbs = warp::get()
        .and(warp::path("_dbs"))
        .and(with_fdb(fdb))
        .and_then(get_all_dbs);
    let default = warp::get().and_then(home);

    hi.or(all_dbs).or(default)
}

pub async fn home() -> Result<impl Reply> {
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

pub async fn get_all_dbs(fdb: Arc<FdbDatabase>) -> Result<impl Reply> {
    let trx = fdb.create_trx().unwrap();
    let dbs: Vec<String> = all_dbs(&trx).await.unwrap().iter().map(|db| {
        db.name.clone()
    }).collect();
    Ok(warp::reply::json(&dbs))
}

// pub async fn all_docs(fdb: &FdbDatabase) -> Result<impl Reply> {
//     let tx = fdb.create_trx().unwrap();
//
//
//
//
// }
