use foundationdb::tuple::PackError;
use foundationdb::FdbError;
use std::fmt::Display;
use std::{error, fmt, io};

pub mod constants;
pub mod couch;
pub mod fdb;
pub mod http;

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
