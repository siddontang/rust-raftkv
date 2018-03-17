use std::sync::Arc;
use std::collections::HashMap;

use protobuf::{Message, MessageStatic};
use rocksdb::{DBIterator, ReadOptions, Writable, DB};
use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};
use serde_json;

use keys::*;

#[derive(Serialize, Deserialize, Default)]
pub struct Row {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct TruncatedState {
    pub index: u64,
    pub term: u64,
}

pub fn put_u64<W: Writable>(w: &W, key: &[u8], id: u64) {
    let mut value = vec![0; 8];
    BigEndian::write_u64(&mut value, id);
    w.put(key, &value).unwrap();
}

pub fn get_u64(db: &Arc<DB>, key: &[u8]) -> Option<u64> {
    let value = db.get(key).unwrap();

    if value.is_none() {
        return None;
    }

    let value = value.unwrap();
    if value.len() != 8 {
        panic!("need 8 bytes, but only got {}", value.len());
    }

    let n = BigEndian::read_u64(&value);
    Some(n)
}

pub fn get_msg<M>(db: &Arc<DB>, key: &[u8]) -> Option<M>
where
    M: Message + MessageStatic,
{
    let value = db.get(key).unwrap();

    if value.is_none() {
        return None;
    }

    let mut m = M::new();
    m.merge_from_bytes(&value.unwrap()).unwrap();
    Some(m)
}

pub fn put_msg<W: Writable, M: Message>(w: &W, key: &[u8], m: &M) {
    let value = m.write_to_bytes().unwrap();
    w.put(key, &value).unwrap();
}

pub fn scan<F>(db: &Arc<DB>, start_key: &[u8], end_key: &[u8], f: &mut F)
where
    F: FnMut(&[u8], &[u8]) -> bool,
{
    let mut opts = ReadOptions::new();
    opts.set_iterate_lower_bound(start_key);
    opts.set_iterate_upper_bound(end_key);

    unsafe {
        let snap = db.unsafe_snap();
        opts.set_snapshot(&snap);
    }

    let mut it = DBIterator::new(Arc::clone(db), opts);
    it.seek(start_key.into());
    while it.valid() {
        let r = f(it.key(), it.value());

        if !r || !it.next() {
            break;
        }
    }
}

pub fn seek(db: &Arc<DB>, start_key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut opts = ReadOptions::new();
    opts.set_iterate_lower_bound(start_key);
    let mut it = DBIterator::new(Arc::clone(db), opts);
    it.seek(start_key.into());
    if !it.valid() {
        return None;
    }

    Some((it.key().to_vec(), it.value().to_vec()))
}

pub fn seek_for_prev(db: &Arc<DB>, end_key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut opts = ReadOptions::new();
    opts.set_iterate_upper_bound(end_key);
    let mut it = DBIterator::new(Arc::clone(db), opts);
    it.seek_for_prev(end_key.into());
    if !it.valid() {
        return None;
    }

    Some((it.key().to_vec(), it.value().to_vec()))
}

pub fn put_json<W: Writable, T: Serialize>(w: &W, key: &[u8], v: &T) {
    let value = serde_json::to_vec(v).unwrap();
    w.put(key, &value).unwrap();
}

pub fn get_truncated_state(db: &Arc<DB>) -> TruncatedState {
    let value = db.get(RAFT_TRUNCATED_STATE_KEY).unwrap();

    if value.is_none() {
        return TruncatedState::default();
    }

    let m = serde_json::from_slice(&value.unwrap()).unwrap();
    m
}

pub fn put_truncated_state<W: Writable>(w: &W, state: &TruncatedState) {
    put_json(w, RAFT_TRUNCATED_STATE_KEY, &state)
}
