use std::net::TcpListener;
use std::collections::{VecDeque, HashMap};
use std::sync::Mutex;
use serde_json::{from_reader, to_writer};

use cse403_distributed_hash_table::protocol::Command;
use cse403_distributed_hash_table::protocol::Command::{Get, Put};
use serde::Deserialize;


fn main() {

    // Setup Queue (And a mutex around it)
//    let work_queue = Mutex::new(VecDeque::new());

    // setup a basic hash table.    (with a mutex)
    let mut hash_table = HashMap::new();

    // Setup network
    // https://stackoverflow.com/questions/51809603/why-does-serde-jsonfrom-reader-take-ownership-of-the-reader
    let listener = TcpListener::bind(("0.0.0.0", 40480))
        .expect("Unable to bind listener");

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
                        // This should probably be an if, I'm just messing around.
                        let res = match hash_table.contains_key(&key) {
                            true => false,
                            false => {
                                hash_table.insert(key, value);
                                true
                            },
                        };
                        to_writer(&mut stream, &res)
                            .expect("Could not write Put result");
                    },
                    Get(key) => {
                        let opt = hash_table.get(&key);
                        to_writer(&mut stream, &opt)
                            .expect("Could not write Get result");
                    },
                }
            },
        }
    }
}