use foundationdb::tuple::PackError;
use foundationdb::FdbError;
use serde::Serialize;
use std::convert::Infallible;
use std::fmt::Display;
use std::{error, fmt};
use warp::{http::StatusCode, reject, Rejection, Reply};

pub mod constants;
pub mod couch;
mod defs;
pub mod fdb;
pub mod http;
mod util;

#[derive(Clone, PartialEq, Debug)]
pub struct CouchFdbError {
    code: i32,
    message: String,
}

impl Display for CouchFdbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FDB Error code: {:?} message: {:?}",
            self.code, self.message
        )
    }
}

#[derive(Debug)]
pub enum CouchError {
    Missing(String),
    FDB(CouchFdbError),
    FDBPack(PackError),
}

impl error::Error for CouchError {}

impl From<FdbError> for CouchError {
    fn from(err: FdbError) -> Self {
        let couch_fdb_err = CouchFdbError {
            code: err.code(),
            message: err.message().to_string(),
        };
        CouchError::FDB(couch_fdb_err)
    }
}

impl From<PackError> for CouchError {
    fn from(err: PackError) -> Self {
        CouchError::FDBPack(err)
    }
}

impl Display for CouchError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CouchError::Missing(s) => write!(f, "{:?} is missing or doesn't exist", s),
            CouchError::FDB(err) => write!(f, "{:?}", err),
            CouchError::FDBPack(err) => err.fmt(f),
        }
    }
}

impl warp::reject::Reject for CouchError {}

impl From<CouchError> for Rejection {
    fn from(err: CouchError) -> Self {
        reject::custom(err)
    }
}

/// An API error serializable to JSON.
#[derive(Serialize)]
struct ErrorMessage {
    error: String,
    message: String,
}

// This function receives a `Rejection` and tries to return a custom
// value, otherwise simply passes the rejection along.
pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    let code;
    let mut error = "unknown".to_string();
    let message;

    if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = "NOT_FOUND";
    } else if let Some(CouchError::Missing(missing)) = err.find() {
        code = StatusCode::NOT_FOUND;
        message = missing;
        error = "not_found".to_string();
    } else if let Some(CouchError::FDB(err)) = err.find() {
        code = StatusCode::NOT_FOUND;
        message = err.message.as_ref();
        error = format!("code: {}", err.code);
    } else if err.find::<warp::reject::MethodNotAllowed>().is_some() {
        // We can handle a specific error, here METHOD_NOT_ALLOWED,
        // and render it however we want
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = "METHOD_NOT_ALLOWED";
    } else {
        // We should have expected this... Just log and say its a 500
        eprintln!("unhandled rejection: {:?}", err);
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "UNHANDLED_REJECTION";
    }

    let json = warp::reply::json(&ErrorMessage {
        error,
        message: message.into(),
    });

    Ok(warp::reply::with_status(json, code))
}
