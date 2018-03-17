use byteorder::{BigEndian, ByteOrder};

pub const RAFT_HARD_STATE_KEY: &[u8] = &[0x01];
pub const RAFT_CONF_STATE_KEY: &[u8] = &[0x02];
pub const RAFT_TRUNCATED_STATE_KEY: &[u8] = &[0x03];
pub const RAFT_APPLY_INDEX_KEY: &[u8] = &[0x04];

pub const RAFT_LOG_PREFIX: u8 = 0x16;

pub const NODE_ID_KEY: &[u8] = &[0x32];
pub const DATA_PREFIX: u8 = 0x64;

#[inline]
pub fn raft_log_key(id: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = RAFT_LOG_PREFIX;
    BigEndian::write_u64(&mut key[1..9], id);
    key
}

#[inline]
pub fn decode_raft_log_key(key: &[u8]) -> Option<u64> {
    if key.len() != 9 || key[0] != RAFT_LOG_PREFIX {
        return None;
    }

    Some(BigEndian::read_u64(&key[1..9]))
}

#[inline]
pub fn data_key(key: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 1);
    v.push(DATA_PREFIX);
    v.extend_from_slice(key);
    v
}
