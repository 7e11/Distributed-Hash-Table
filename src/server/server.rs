use std::net::{TcpListener};
use std::collections::{HashMap};
use serde_json::{to_writer};

use cse403_distributed_hash_table::protocol::{Command, barrier};
use cse403_distributed_hash_table::protocol::Command::{Get, Put};
use serde::Deserialize;
use cse403_distributed_hash_table::protocol::CommandResponse::{GetAck, PutAck};
use cse403_distributed_hash_table::settings::{parse_settings};
use std::collections::hash_map::Entry;


fn main() {
    let (client_ips, server_ips, _, _) = parse_settings()
        .expect("Unable to parse settings");
    let listener = TcpListener::bind(("0.0.0.0", 40480))
        .expect("Unable to bind listener");
    // Consume both vectors.
    let barrier_ips = client_ips.into_iter().chain(server_ips.into_iter()).collect();
    barrier(barrier_ips, &listener);
    application_listener(listener)
}

fn application_listener(listener: TcpListener) {
    println!("Listening for applications on {:?}", listener);
    let mut hash_table = HashMap::new();
    // Setup network
    // https://stackoverflow.com/questions/51809603/why-does-serde-jsonfrom-reader-take-ownership-of-the-reader
    for res_stream in listener.incoming() {
        match res_stream {
            Err(e) => eprintln!("Couldn't accept connection: {}", e),
            Ok(mut stream) => {
                // https://github.com/serde-rs/json/issues/522
                let mut de = serde_json::Deserializer::from_reader(&mut stream);
                let c = Command::deserialize(&mut de).expect("Could not deserialize command.");
//                println!("Received: {:?}", c);
                match c {
                    Put(key, value) => {
                        let res = match hash_table.entry(key) {
                            Entry::Occupied(_) => false,
                            Entry::Vacant(v) => {
                                v.insert(value);
                                true
                            },
                        };
                        to_writer(&mut stream, &PutAck(res))
                            .expect("Could not write Put result");
                    },
                    Get(key) => {
                        let opt = hash_table.get(&key);
                        // TODO: I'll need to unwrap this here in order to clone it.
                        let opt = match opt {
                            Some(vt) => Some(vt.clone()),
                            None => None,
                        };
                        to_writer(&mut stream, &GetAck(opt))
                            .expect("Could not write Get result");
                    },
                }
            },
        }
    }
}