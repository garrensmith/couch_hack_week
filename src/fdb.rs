use foundationdb::tuple;
use foundationdb::tuple::{TuplePack};

pub fn pack_with_prefix<T: TuplePack>(v: &T, prefix: &[u8]) -> Vec<u8> {
    let packed = tuple::pack(v);
    [prefix, packed.as_ref()].concat()
}

pub fn pack_around<T: TuplePack>(v: &T, prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    let packed = tuple::pack(v);
    [prefix, packed.as_ref(), suffix].concat()
}

pub fn pack_range<T: TuplePack>(v: &T, prefix: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let packed = tuple::pack(v);
    let start = [prefix, packed.as_ref()].concat();
    let end = [prefix, packed.as_ref(), b"\xFF"].concat();
    (start, end)
}
