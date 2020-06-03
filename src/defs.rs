use foundationdb::tuple::{Bytes, Versionstamp};
use serde::Serialize;

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

#[derive(Clone, Serialize)]
pub struct Database {
    pub name: String,
    #[serde(skip_serializing)]
    pub(crate) db_prefix: Vec<u8>,
}

impl Database {
    pub fn new(name: Bytes, db_prefix: &[u8]) -> Database {
        Database {
            name: String::from_utf8_lossy(name.as_ref()).into(),
            db_prefix: db_prefix.to_vec(),
        }
    }
}

#[derive(Serialize)]
pub struct Rev(String);

impl From<(i16, Bytes<'_>)> for Rev {
    fn from((num, str): (i16, Bytes)) -> Self {
        Rev(format!("{}-{}", num, hex::encode(str.as_ref())))
    }
}

impl Into<String> for Rev {
    fn into(self) -> String {
        self.0
    }
}

#[derive(Serialize, Clone)]
pub struct Id(String);

impl From<&[u8]> for Id {
    fn from(bin: &[u8]) -> Self {
        Id(String::from_utf8_lossy(bin).into())
    }
}

impl Into<String> for Id {
    fn into(self) -> String {
        self.0
    }
}

#[derive(Serialize)]
pub struct Seq(String);

impl From<Versionstamp> for Seq {
    fn from(vs: Versionstamp) -> Self {
        Seq(hex::encode(vs.as_bytes()))
    }
}

#[derive(Clone, Serialize)]
pub struct Row {
    id: Id,
    key: String,
    value: String,
}

impl Row {
    pub fn new<S, T>(raw: S, value: T) -> Self
    where
        S: Into<Id> + Clone,
        T: Into<String>,
    {
        let id = raw.into();
        Row {
            key: id.clone().into(),
            id,
            value: value.into(),
        }
    }
}

#[derive(Serialize)]
pub struct ChangeRow {
    pub seq: Seq,
    pub id: Id,
    pub rev: Rev,
    pub deleted: bool,
}

impl ChangeRow {
    pub fn new(id: Id, rev: Rev, seq: Seq, deleted: bool) -> Self {
        ChangeRow {
            seq,
            id,
            rev,
            deleted,
        }
    }
}
