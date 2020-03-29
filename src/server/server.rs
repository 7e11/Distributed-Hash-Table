use std::net::{TcpListener};
use std::collections::{HashMap};
use serde_json::{to_writer};

use cse403_distributed_hash_table::protocol::{Command, barrier};
use cse403_distributed_hash_table::protocol::Command::{Get, Put, Exit};
use serde::Deserialize;
use std::path::Path;
use config::{ConfigError};
use cse403_distributed_hash_table::protocol::CommandResponse::{GetAck, PutAck, NegAck};
use cse403_distributed_hash_table::settings::parse_ips;


fn main() {
    let (client_ips, server_ips) = parse_settings().expect("Failed to parse settings");
    // Listener for the lifetime of the program
    let listener = TcpListener::bind(("0.0.0.0", 40480))
        .expect("Unable to bind listener");
    // Consume both vectors.
    let barrier_ips = client_ips.into_iter().chain(server_ips.into_iter()).collect();
    barrier(barrier_ips, &listener);
    application_listener(listener)
}


fn parse_settings() -> Result<(Vec<String>, Vec<String>), ConfigError> {
    // Expand the Vec<String> to encompass more types as the settings file increases in size.
    // See: https://github.com/mehcode/config-rs/blob/master/examples/simple/src/main.rs
    let mut config = config::Config::default();
    config.merge(config::File::from(Path::new("./settings.yaml")))?;
    parse_ips(&config)
}

fn application_listener(listener: TcpListener) {
    println!("Listening for applications on {:?}", listener);
    // TODO: Add neg ack responses if there is contention for the lock.
    // setup a basic hash table.    TODO (with a mutex and arc)
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
                        // This should probably be an if, I'm just messing around.
                        let res = match hash_table.contains_key(&key) {
                            true => false,
                            false => {
                                hash_table.insert(key, value);
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
                    Exit => {
                        // done receiving. Need some way to exit.
                        to_writer(&mut stream, &NegAck).unwrap();
                        break;
                    }
                }
            },
        }
    }
}