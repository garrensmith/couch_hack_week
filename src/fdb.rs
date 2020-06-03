use foundationdb::tuple::{PackResult, TuplePack, TupleUnpack};
use foundationdb::{tuple, KeySelector};

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

pub fn pack_key_range<'a, T: TuplePack>(
    v: &'a T,
    prefix: &[u8],
) -> (KeySelector<'a>, KeySelector<'a>) {
    let (start, end) = pack_range(v, prefix);

    let start_key = KeySelector::first_greater_or_equal(start);
    let end_key = KeySelector::first_greater_than(end);
    (start_key, end_key)
}

pub fn unpack_with_prefix<'de, T: TupleUnpack<'de>>(
    input: &'de [u8],
    prefix: &[u8],
) -> PackResult<T> {
    let input1 = &input[prefix.len()..];
    tuple::unpack(input1)
}
