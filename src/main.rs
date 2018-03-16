#![feature(rustc_private)]
#![feature(plugin, decl_macro)]
#![plugin(rocket_codegen)]

extern crate clap;
extern crate env_logger;
#[macro_use]
extern crate log;
extern crate raft;
extern crate rocket;
extern crate sled;

use clap::{App, Arg};
use sled::{ConfigBuilder, Tree};
use raft::*;

mod http;
use http::*;

fn main() {
    env_logger::init();

    info!("hello raft!!!");

    let matches = App::new("raft example")
        .version("0.1")
        .author("SiddonTang, <siddontang@gmail.com>")
        .about("a simple example to use raft in Rust")
        .arg(
            Arg::with_name("port")
                .short("P")
                .long("port")
                .value_name("20170")
                .required(true)
                .help("HTTP listening port")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("data")
                .long("data")
                .value_name("./data")
                .required(true)
                .help("Directory to save data")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("init_cluster")
                .long("init_cluster")
                .value_name("1=127.0.0.1:20170")
                .help("Initialized Raft cluster")
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

    let port = matches.value_of("port").unwrap().parse::<u16>().unwrap();
    run_raft_server(port);
}
