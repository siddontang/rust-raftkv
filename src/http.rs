use std::sync::Arc;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use rocket::config::{Config, Environment};
use rocket::http::Status;
use rocket::{self, State};
use rocket::response::status::{Custom, NotFound};
use protobuf::{Message, MessageStatic};
use raft::eraftpb::Message as RaftMessage;
use crossbeam_channel::{self, Sender};
use rocksdb::DB;
use rand::{self, Rng};

use node::*;
use transport::*;
use keys::*;
use util::*;

pub struct RaftServer {
    sender: Sender<Msg>,
    db: Arc<DB>,
}

impl RaftServer {
    fn do_request(&self, mut req: Request) -> Response {
        let id = rand::thread_rng().gen::<u64>();
        req.id = id;

        let (c, r) = crossbeam_channel::unbounded();

        self.sender.send(Msg::Propose {
            request: req,
            cb: Box::new(move |resp| {
                c.send(resp).unwrap();
            }),
        });

        if let Ok(r) = r.recv_timeout(Duration::from_secs(3)) {
            return r;
        }

        Response {
            ok: false,
            ..Default::default()
        }
    }
}

#[get("/local_kv/<key>")]
fn local_kv_get(state: State<RaftServer>, key: String) -> Result<String, NotFound<String>> {
    let s = state;
    if let Some(v) = s.db.get(&data_key(key.as_bytes())).unwrap() {
        return Ok(v.to_utf8().unwrap().to_string());
    }

    Err(NotFound(format!("{} is not found", key)))
}

#[get("/kv/<key>")]
fn kv_get(state: State<RaftServer>, key: String) -> Result<String, Custom<String>> {
    let s = state;
    let req = Request {
        op: 1,
        row: Row {
            key: data_key(&key.as_bytes()),
            value: vec![],
        },
        ..Default::default()
    };

    let resp = s.do_request(req);
    if resp.ok {
        match resp.value {
            None => return Err(Custom(Status::NotFound, format!("{} is not found", key))),
            Some(v) => return Ok(String::from_utf8(v).unwrap()),
        }
    }

    Err(Custom(
        Status::InternalServerError,
        format!("meet server error when get {}", key),
    ))
}

#[put("/kv/<key>", data = "<value>")]
fn kv_put(state: State<RaftServer>, key: String, value: String) -> Result<(), Custom<String>> {
    kv_post(state, key, value)
}

#[post("/kv/<key>", data = "<value>")]
fn kv_post(state: State<RaftServer>, key: String, value: String) -> Result<(), Custom<String>> {
    let s = state;
    let req = Request {
        op: 2,
        row: Row {
            key: data_key(&key.as_bytes()),
            value: value.into_bytes(),
        },
        ..Default::default()
    };

    let resp = s.do_request(req);
    if resp.ok {
        Ok(())
    } else {
        Err(Custom(
            Status::InternalServerError,
            format!("meet server error when get {}", key),
        ))
    }
}

#[delete("/kv/<key>")]
fn kv_delete(state: State<RaftServer>, key: String) -> Result<(), Custom<String>> {
    let s = state;
    let req = Request {
        op: 3,
        row: Row {
            key: data_key(&key.as_bytes()),
            value: vec![],
        },
        ..Default::default()
    };

    let resp = s.do_request(req);
    if resp.ok {
        Ok(())
    } else {
        Err(Custom(
            Status::InternalServerError,
            format!("meet server error when get {}", key),
        ))
    }
}

#[post("/raft", data = "<value>")]
fn raft_post(state: State<RaftServer>, value: Vec<u8>) -> Result<(), Status> {
    let s = state;

    let mut m = RaftMessage::new();
    m.merge_from_bytes(&value).unwrap();

    s.sender.send(Msg::Raft(m));

    Ok(())
}

pub fn run_raft_server(port: u16, id: u64, db: Arc<DB>, nodes: HashMap<u64, String>) {
    let cfg = Config::build(Environment::Staging)
        .address("0.0.0.0")
        .port(port)
        .workers(4)
        .finalize()
        .unwrap();

    let (sender, receiver) = crossbeam_channel::unbounded();
    let mut trans = Transport::new(sender.clone());
    trans.start(nodes);

    let db1 = db.clone();
    thread::spawn(move || {
        let node = Node::new(id, db1, trans);
        run_node(node, receiver);
    });

    let s = RaftServer {
        sender: sender,
        db: db,
    };
    rocket::custom(cfg, false)
        .mount(
            "/",
            routes![kv_get, kv_put, kv_post, kv_delete, raft_post, local_kv_get],
        )
        .manage(s)
        .launch();
}
