use std::net::{TcpListener};
use std::collections::{HashMap};
use serde_json::{to_writer};

use cse403_distributed_hash_table::protocol::{Command, barrier};
use cse403_distributed_hash_table::protocol::Command::{Get, Put};
use serde::Deserialize;
use std::path::Path;
use config::{ConfigError};


fn main() {
    let (client_ips, server_ips) = parse_settings().expect("Failed to parse settings");
    println!("Starting barrier");
    // Consume both vectors.
    let barrier_ips = client_ips.into_iter().chain(server_ips.into_iter()).collect();
    barrier(barrier_ips, 40480);
    println!("Starting application listener");
    application_listener()
}

fn parse_settings() -> Result<(Vec<String>, Vec<String>), ConfigError> {
    // Expand the Vec<String> to encompass more types as the settings file increases in size.
    // See: https://github.com/mehcode/config-rs/blob/master/examples/simple/src/main.rs
    let mut config = config::Config::default();
    // Add in server_settings.yaml
    config.merge(config::File::from(Path::new("./settings/server_settings.yaml")))?;
//    println!("{:#?}", config);
    let client_ips = config.get_array("client_ips")?;
    let server_ips = config.get_array("server_ips")?;

    // Now convert them to strings
    let client_ips = client_ips.into_iter()
        .map(|s| s.into_str().expect("Could not parse IP into str"))
        .collect();
    let server_ips = server_ips.into_iter()
        .map(|s| s.into_str().expect("Could not parse IP into str"))
        .collect();

    Ok((client_ips, server_ips))
}

fn application_listener() {
    // setup a basic hash table.    TODO (with a mutex and arc)
    let mut hash_table = HashMap::new();
    // Setup network
    // https://stackoverflow.com/questions/51809603/why-does-serde-jsonfrom-reader-take-ownership-of-the-reader
    let listener = TcpListener::bind(("0.0.0.0", 48080))    //TODO: Reuse instead of opening again
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