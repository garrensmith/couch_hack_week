use foundationdb::tuple::Element;

pub const COUCHDB_PREFIX: &[u8; 14] = b"\xfe\x01\xfe\x00\x14\x02couchdb\x00";
// pub const DB_CHANGES: &[u8; 3] = b"\x15\x133";
pub const ALL_DBS: Element = Element::Int(1);
pub const DB_STATS: Element = Element::Int(17);
pub const DB_ALL_DOCS: Element = Element::Int(18);
pub const DB_CHANGES: Element = Element::Int(19);
pub const VS_STAMP: Element = Element::Int(51);
