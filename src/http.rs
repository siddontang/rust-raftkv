use rocket::config::{Config, Environment};
use rocket::http::Status;
use rocket::{self, State};

pub struct RaftServer {}

impl RaftServer {}

#[get("/kv/<key>")]
fn kv_get(state: State<RaftServer>, key: String) -> Result<String, Status> {
    let _s = state;

    Ok(key)
}

#[put("/kv/<key>", data = "<value>")]
fn kv_put(state: State<RaftServer>, key: String, value: Vec<u8>) -> Result<(), Status> {
    kv_post(state, key, value)
}

#[post("/kv/<key>", data = "<value>")]
fn kv_post(state: State<RaftServer>, key: String, value: Vec<u8>) -> Result<(), Status> {
    let _s = state;
    Ok(())
}

#[delete("/kv/<key>")]
fn kv_delete(state: State<RaftServer>, key: String) -> Result<(), Status> {
    let _s = state;
    Ok(())
}

#[post("/raft", data = "<value>")]
fn raft_post(state: State<RaftServer>, value: Vec<u8>) -> Result<(), Status> {
    let _s = state;
    Ok(())
}

pub fn run_raft_server(port: u16) {
    let cfg = Config::build(Environment::Staging)
        .address("0.0.0.0")
        .port(port)
        .workers(4)
        .finalize()
        .unwrap();

    let s = RaftServer {};
    rocket::custom(cfg, false)
        .mount("/", routes![kv_get, kv_put, kv_post, kv_delete, raft_post])
        .manage(s)
        .launch();
}
