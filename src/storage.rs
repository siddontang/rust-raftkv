use std::sync::Arc;

use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{self, Error as RaftError, RaftState, Ready, Result as RaftResult,
           Storage as RaftStorage, StorageError};
use rocksdb::{Writable, WriteBatch, WriteOptions, DB};
use protobuf::Message;
use serde::{Deserialize, Serialize};
use serde_json;

use util::*;
use keys::*;

pub struct Storage {
    tag: String,
    pub id: u64,
    pub db: Arc<DB>,
    truncated_state: TruncatedState,
    last_index: u64,
    last_term: u64,
    pub apply_index: u64,
    conf_state: ConfState,
}

impl Storage {
    pub fn new(id: u64, db: Arc<DB>) -> Storage {
        let truncated_state = get_truncated_state(&db);
        let last_log = get_last_log(&db).unwrap_or_default();
        let apply_index = get_u64(&db, RAFT_APPLY_INDEX_KEY).unwrap_or_default();
        // ConfState must exist here. We will create it when initilize the cluster at first.
        let conf = get_msg(&db, RAFT_CONF_STATE_KEY).unwrap();

        Storage {
            tag: format!("[{}]", id),
            id: id,
            truncated_state: truncated_state,
            last_index: last_log.get_index(),
            last_term: last_log.get_term(),
            apply_index: apply_index,
            db: db,
            conf_state: conf,
        }
    }

    pub fn append<W: Writable>(&mut self, w: &W, entries: &[Entry]) {
        let (last_index, last_term) = {
            let e = entries.last().unwrap();
            (e.get_index(), e.get_term())
        };

        for entry in entries {
            put_msg(w, &raft_log_key(entry.get_index()), entry);
        }

        let prev_last_index = self.last_index;
        // Delete any previously appended log entries which never committed.
        for i in (last_index + 1)..(prev_last_index + 1) {
            w.delete(&raft_log_key(i)).unwrap();
        }

        self.last_index = last_index;
        self.last_term = last_term;
    }

    pub fn apply_snapshot<W: Writable>(&mut self, w: &W, snap: &Snapshot) {
        self.last_index = snap.get_metadata().get_index();
        self.last_term = snap.get_metadata().get_term();
        self.apply_index = snap.get_metadata().get_term();

        // The snapshot only contains log which index > applied index, so
        // here the truncate state's (index, term) is in snapshot metadata.
        self.truncated_state.index = self.last_index;
        self.truncated_state.term = self.last_term;

        self.conf_state = snap.get_metadata().get_conf_state().clone();

        put_truncated_state(w, &self.truncated_state);
        put_msg(w, RAFT_CONF_STATE_KEY, &self.conf_state);
        put_u64(w, RAFT_APPLY_INDEX_KEY, self.apply_index);

        scan(
            &self.db,
            &[DATA_PREFIX],
            &[DATA_PREFIX + 1],
            &mut |key, _| {
                w.delete(key).unwrap();
                true
            },
        );

        let rows: Vec<Row> = serde_json::from_slice(snap.get_data()).unwrap();
        for row in rows {
            w.put(&row.key, &row.value).unwrap();
        }
    }

    fn check_range(&self, low: u64, high: u64) -> RaftResult<()> {
        if low > high {
            panic!("low {} is greater than high: {}", low, high);
        } else if low <= self.truncated_state.index {
            return Err(RaftError::Store(StorageError::Compacted));
        } else if high > self.last_index + 1 {
            panic!(
                "entires high {} is out of bound lastindex {}",
                high, self.last_index
            )
        }

        Ok(())
    }
}

impl RaftStorage for Storage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let hard = get_msg(&self.db, RAFT_HARD_STATE_KEY).unwrap_or_default();
        Ok(RaftState {
            hard_state: hard,
            conf_state: self.conf_state.clone(),
        })
    }

    fn entries(&self, low: u64, high: u64, max_size: u64) -> RaftResult<Vec<Entry>> {
        self.check_range(low, high)?;
        let mut ents = Vec::with_capacity((high - low) as usize);
        if low == high {
            return Ok(ents);
        }

        let start_key = raft_log_key(low);
        let end_key = raft_log_key(high);
        let mut total_size: u64 = 0;
        let mut next_index = low;
        let mut exceeded_max_size = false;

        scan(&self.db, &start_key, &end_key, &mut |_, value| {
            let mut entry = Entry::new();
            entry.merge_from_bytes(value).unwrap();

            // May meet gap or has been compacted.
            if entry.get_index() != next_index {
                return false;
            }
            next_index += 1;
            total_size += value.len() as u64;
            exceeded_max_size = total_size > max_size;
            if !exceeded_max_size || ents.is_empty() {
                ents.push(entry);
            }

            !exceeded_max_size
        });

        // If we get the correct number of entries, returns,
        // or the total size almost exceeds max_size, returns.
        if ents.len() == (high - low) as usize || exceeded_max_size {
            return Ok(ents);
        }

        // Here means we don't fetch enough entries.
        Err(RaftError::Store(StorageError::Unavailable))
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        if idx == self.truncated_state.index {
            return Ok(self.truncated_state.term);
        }

        self.check_range(idx, idx + 1)?;
        if self.truncated_state.term == self.last_term || idx == self.last_index {
            return Ok(self.last_term);
        }

        let entries = self.entries(idx, idx + 1, raft::NO_LIMIT)?;
        Ok(entries[0].get_term())
    }

    fn first_index(&self) -> RaftResult<u64> {
        Ok(self.truncated_state.index + 1)
    }

    fn last_index(&self) -> RaftResult<u64> {
        Ok(self.last_index)
    }

    fn snapshot(&self) -> RaftResult<Snapshot> {
        let idx = self.apply_index;
        let term = if idx == self.truncated_state.index {
            self.truncated_state.term
        } else {
            get_msg::<Entry>(&self.db, &raft_log_key(idx))
                .unwrap()
                .get_term()
        };
        let mut snapshot = Snapshot::new();
        snapshot.mut_metadata().set_index(idx);
        snapshot.mut_metadata().set_term(term);
        snapshot
            .mut_metadata()
            .set_conf_state(self.conf_state.clone());

        let mut rows: Vec<Row> = Vec::with_capacity(1000);
        scan(
            &self.db,
            &[DATA_PREFIX],
            &[DATA_PREFIX + 1],
            &mut |key, value| {
                rows.push(Row {
                    key: key.to_vec(),
                    value: value.to_vec(),
                });
                true
            },
        );

        let data = serde_json::to_vec(&rows).unwrap();
        snapshot.set_data(data);

        Ok(snapshot)
    }
}

pub fn get_last_log(db: &Arc<DB>) -> Option<Entry> {
    if let Some((key, value)) = seek_for_prev(db, &raft_log_key(u64::max_value())) {
        if let Some(id) = decode_raft_log_key(&key) {
            let mut m = Entry::new();
            m.merge_from_bytes(&value).unwrap();
            return Some(m);
        }
    }

    None
}

pub fn try_init_cluster(db: &Arc<DB>, id: u64, nodes: &[u64]) {
    if let Some(oid) = get_u64(db, NODE_ID_KEY) {
        if oid != id {
            panic!("already init for id {}, but now is {}", oid, id)
        }
        return;
    }

    let wb = WriteBatch::new();

    let mut conf_state = ConfState::new();
    conf_state.set_nodes(nodes.to_vec());
    put_u64(&wb, NODE_ID_KEY, id);
    put_msg(&wb, RAFT_CONF_STATE_KEY, &conf_state);

    let mut write_opts = WriteOptions::new();
    write_opts.set_sync(true);
    db.write_opt(wb, &write_opts).unwrap();
}
