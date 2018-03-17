#![feature(rustc_private)]
#![feature(plugin, decl_macro)]
#![plugin(rocket_codegen)]
#![feature(fnbox)]

extern crate byteorder;
extern crate clap;
extern crate crossbeam_channel;
extern crate env_logger;
#[macro_use]
extern crate log;
extern crate protobuf;
extern crate raft;
extern crate rand;
extern crate reqwest;
extern crate rocket;
extern crate rocksdb;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use std::sync::Arc;
use std::collections::HashMap;

use clap::{App, Arg};
use rocksdb::DB;

mod http;
mod keys;
mod storage;
mod util;
mod node;
mod transport;

use http::*;

fn main() {
    env_logger::init();

    info!("hello raft!!!");

    let matches = App::new("raft example")
        .version("0.1")
        .author("SiddonTang, <siddontang@gmail.com>")
        .about("a simple example to use raft in Rust")
        .arg(
            Arg::with_name("data")
                .long("data")
                .value_name("./data")
                .required(true)
                .help("Directory to save data")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("cluster")
                .long("cluster")
                .value_name("1=127.0.0.1:20171,2=127.0.0.1:20172")
                .help("Cluster configuration")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("id")
                .long("id")
                .short("I")
                .value_name("1")
                .help("Unique server ID")
                .takes_value(true),
        )
        .get_matches();

    let id = matches.value_of("id").unwrap().parse::<u64>().unwrap();
    if id == 0 {
        panic!("id must > 0");
    }

    let data_path = matches.value_of("data").unwrap();
    let db = Arc::new(DB::open_default(&data_path).unwrap());

    let cluster = matches.value_of("cluster").unwrap();
    let node_addrs = parse_cluster(cluster);

    let addr = node_addrs.get(&id).unwrap().clone();
    let nodes = node_addrs.keys().cloned().collect::<Vec<u64>>();
    storage::try_init_cluster(&db, id, &nodes);

    let items: Vec<&str> = addr.split(":").collect();
    let port = items[1].parse::<u16>().unwrap();

    error!("start server {}, listen {}", id, port);
    run_raft_server(port, id, db, node_addrs);
}

fn parse_cluster(cluster: &str) -> HashMap<u64, String> {
    let mut m = HashMap::new();
    let items: Vec<&str> = cluster.split(",").collect();
    for item in items {
        let v: Vec<&str> = item.split("=").collect();
        let id = v[0].parse::<u64>().unwrap();
        m.insert(id, v[1].to_string());
    }

    m
}
