use byteorder::{LittleEndian, ReadBytesExt};

pub fn bin_to_int(mut bin: &[u8]) -> u64 {
    match bin.read_u64::<LittleEndian>() {
        Ok(num) => num,
        Err(_) => 0,
    }
}

pub fn bin_to_string(bin: &[u8]) -> String {
    String::from_utf8_lossy(bin).into()
}
