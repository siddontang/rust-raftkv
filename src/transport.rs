use std::collections::HashMap;
use std::thread;

use crossbeam_channel::{unbounded, Receiver, Sender};
use raft::eraftpb::{Message as RaftMessage, MessageType};
use raft::SnapshotStatus;
use reqwest;
use protobuf::Message;

use node::*;

pub struct Transport {
    sender: Sender<Msg>,
    node_chs: HashMap<u64, Sender<RaftMessage>>,
}

impl Transport {
    pub fn new(sender: Sender<Msg>) -> Transport {
        Transport {
            sender: sender,
            node_chs: HashMap::new(),
        }
    }

    pub fn start(&mut self, node_addrs: HashMap<u64, String>) {
        for (id, addr) in node_addrs.iter() {
            let (s, r) = unbounded();
            self.node_chs.insert(*id, s);

            let id = *id;
            let addr = addr.clone();
            let sender = self.sender.clone();
            thread::spawn(move || {
                on_transport(r, id, addr, sender);
            });
        }
    }

    pub fn send(&self, id: u64, msg: RaftMessage) {
        if let Some(s) = self.node_chs.get(&id) {
            s.send(msg);
        }
    }
}

fn on_transport(ch: Receiver<RaftMessage>, id: u64, addr: String, sender: Sender<Msg>) {
    let client = reqwest::Client::new();
    let url = format!("http://{}/raft", addr);
    while let Ok(msg) = ch.recv() {
        let value = msg.write_to_bytes().unwrap();
        let is_snapshot = msg.get_msg_type() == MessageType::MsgSnapshot;
        if let Err(_) = client.post(&url).body(value).send() {
            sender.send(Msg::ReportSnapshot {
                id: id,
                status: SnapshotStatus::Failure,
            });
            sender.send(Msg::ReportUnreachable(id));
        }

        if is_snapshot {
            sender.send(Msg::ReportSnapshot {
                id: id,
                status: SnapshotStatus::Finish,
            });
        }
    }
}
