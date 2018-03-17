use std::boxed::FnBox;
use std::time::{Duration, Instant};
use std::sync::Arc;
use std::collections::HashMap;

use crossbeam_channel::{Receiver, RecvTimeoutError};
use raft::eraftpb::{EntryType, Message as RaftMessage};
use raft::{self, Config, RawNode, SnapshotStatus};
use rocksdb::{Writable, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};
use serde_json;

use util::*;
use storage::*;
use keys::*;
use transport::*;

// op: read 1, write 2, delete 3.
#[derive(Serialize, Deserialize, Default)]
pub struct Request {
    pub id: u64,
    pub op: u32,
    pub row: Row,
}

#[derive(Serialize, Deserialize, Default)]
pub struct Response {
    pub id: u64,
    pub ok: bool,
    pub op: u32,
    pub value: Option<Vec<u8>>,
}

pub type RequestCallback = Box<FnBox(Response) + Send>;

pub enum Msg {
    Propose {
        request: Request,
        cb: RequestCallback,
    },
    Raft(RaftMessage),
    ReportUnreachable(u64),
    ReportSnapshot {
        id: u64,
        status: SnapshotStatus,
    },
}

pub struct Node {
    tag: String,
    id: u64,
    r: RawNode<Storage>,
    cbs: HashMap<u64, RequestCallback>,
    db: Arc<DB>,
    trans: Transport,
}

impl Node {
    pub fn new(id: u64, db: Arc<DB>, trans: Transport) -> Node {
        let storage = Storage::new(id, db.clone());
        let cfg = Config {
            id: id,
            peers: vec![],
            election_tick: 10,
            heartbeat_tick: 3,
            max_size_per_msg: 1024 * 1024 * 1024,
            max_inflight_msgs: 256,
            applied: storage.apply_index,
            tag: format!("[{}]", id),
            ..Default::default()
        };

        let r = RawNode::new(&cfg, storage, &[]).unwrap();

        Node {
            tag: format!("[{}]", id),
            id: id,
            r: r,
            cbs: HashMap::new(),
            db: db,
            trans: trans,
        }
    }

    pub fn on_msg(&mut self, msg: Msg) {
        match msg {
            Msg::Raft(m) => self.r.step(m).unwrap(),
            Msg::Propose { request, cb } => {
                if self.r.raft.leader_id != self.id || self.cbs.contains_key(&request.id) {
                    cb(Response {
                        id: request.id,
                        ok: false,
                        op: request.op,
                        ..Default::default()
                    });
                    return;
                }

                let data = serde_json::to_vec(&request).unwrap();
                self.r.propose(data, false).unwrap();
                self.cbs.insert(request.id, cb);
            }
            Msg::ReportUnreachable(id) => self.r.report_unreachable(id),
            Msg::ReportSnapshot { id, status } => self.r.report_snapshot(id, status),
        }
    }

    pub fn on_tick(&mut self) {
        if !self.r.has_ready() {
            return;
        }

        let wb = WriteBatch::new();
        let is_leader = self.r.raft.leader_id == self.id;
        let mut ready = self.r.ready();

        // There is a snapshot, we need to apply it.
        if !raft::is_empty_snap(&ready.snapshot) {
            self.r.mut_store().apply_snapshot(&wb, &ready.snapshot);
        }

        if !ready.entries.is_empty() {
            self.r.mut_store().append(&wb, &ready.entries);
        }

        // HardState Changed, persist it.
        if let Some(ref hs) = ready.hs {
            put_msg(&wb, RAFT_HARD_STATE_KEY, hs);
        }

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        self.db.write_opt(wb, &write_opts).unwrap();

        {
            // Send Messages if possible.
            let msgs = ready.messages.drain(..);
            for msg in msgs {
                self.trans.send(msg.get_to(), msg);
            }
        }

        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in committed_entries {
                if entry.get_data().is_empty() {
                    continue;
                }

                if entry.get_entry_type() == EntryType::EntryNormal {
                    let request: Request = serde_json::from_slice(entry.get_data()).unwrap();
                    self.on_request(request);
                }
            }
        }

        self.r.advance(ready);
    }

    fn on_request(&mut self, req: Request) {
        let cb = self.cbs.remove(&req.id);
        if cb.is_none() {
            return;
        }

        let mut resp = Response {
            id: req.id,
            op: req.op,
            ok: true,
            value: None,
        };
        match req.op {
            1 => {
                if let Some(v) = self.db.get(&req.row.key).unwrap() {
                    resp.value = Some(v.to_vec());
                }
            }
            2 => {
                self.db.put(&req.row.key, &req.row.value).unwrap();
            }
            3 => {
                self.db.delete(&req.row.key).unwrap();
            }
            _ => unreachable!(),
        }

        cb.unwrap()(resp);
    }
}

pub fn run_node(mut node: Node, ch: Receiver<Msg>) {
    let mut t = Instant::now();
    let d = Duration::from_millis(100);
    loop {
        for _ in 0..4096 {
            match ch.recv_timeout(d) {
                Ok(msg) => node.on_msg(msg),
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }

        if t.elapsed() >= d {
            t = Instant::now();
            node.r.tick();
        }

        node.on_tick();
    }
}
