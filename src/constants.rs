use foundationdb::tuple::Element;

pub const COUCHDB_PREFIX: &[u8; 14] = b"\xfe\x01\xfe\x00\x14\x02couchdb\x00";
pub const ALL_DBS: Element = Element::Int(1);
pub const DB_STATS: Element = Element::Int(17);
pub const DB_ALL_DOCS: Element = Element::Int(18);
