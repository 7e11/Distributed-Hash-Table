use std::net::{TcpListener, TcpStream, SocketAddr, ToSocketAddrs};
use std::collections::{VecDeque, HashMap};
use std::sync::Mutex;
use serde_json::{from_reader, to_writer};

use cse403_distributed_hash_table::protocol::{Command, BarrierCommand};
use cse403_distributed_hash_table::protocol::Command::{Get, Put};
use cse403_distributed_hash_table::protocol::BarrierCommand::{AllReady, NotAllReady};
use serde::Deserialize;
use std::thread;
use std::time::Duration;
use std::path::Path;
use std::convert::{TryFrom, TryInto};
use std::str::FromStr;


fn main() {

    // Get config settings
    // See: https://github.com/mehcode/config-rs/blob/master/examples/simple/src/main.rs
    let mut config = config::Config::default();
    // Add in server_settings.yaml
    config.merge(config::File::from(Path::new("./settings/server_settings.yaml")))
        .expect("Failed to merge server_settings.yaml");
    // This will fail if we add more settings TODO: make more expendable.
    let mut settings: HashMap<String, Vec<String>> = config.try_into().expect("Failed to parse settings");
    // Need to remove in order to take ownership of it.
    let node_ips = settings.remove("node_ips").expect("Failed to parse ips");

    println!("Starting barrier");
    barrier(node_ips);
    println!("Starting application listener");
//    start()
}

fn barrier(node_ips: Vec<String>) {
    // Distributed Barrier (My idea)
    // When coming online, go into "listen" mode and send a message to all IP's.
    // Wait for an acknowledgement from everyone (including yourself)
    //  - If everyone acknowledges, send out a messgae and start
    //  - If someone does not acknowledge, just keep listening.

    // Bind the broadcast listener
    let listener = TcpListener::bind(("0.0.0.0", 40480))
        .expect("Unable to bind listener");

    // Spawn the broadcast thread.
    let handle = thread::spawn(move || {
        barrier_broadcast(node_ips);
    });

    for res_stream in listener.incoming() {
        match res_stream {
            Err(e) => eprintln!("Couldn't accept barrier connection: {}", e),
            Ok(mut stream) => {
                let mut de = serde_json::Deserializer::from_reader(&mut stream);
                let bc = BarrierCommand::deserialize(&mut de)
                    .expect("Could not deserialize barrier command.");
                match bc {
                    AllReady => break,
                    NotAllReady => (),
                }
            },
        }
    }
    handle.join().expect("broadcast thread not stopped");
}

fn barrier_broadcast(node_ips: Vec<String>) {
    // Attempt to open a TCP connection with everyone.
//    let node_ips = [[127, 0, 0, 1], [192, 0, 2, 1]];
    // east, west, central
    let mut open_streams = Vec::new();

    for ip in &node_ips {
        let stream_res = TcpStream::connect_timeout(
            &SocketAddr::from_str((ip.to_owned() + ":40480").as_ref()).unwrap(), //TODO Very gross
            Duration::from_secs(5));
        match stream_res {
            Ok(stream) => open_streams.push(stream),
            Err(e) => {
                println!("Failed broadcast to {:?}: {}", ip, e);
                // write NotAllReady to the ones that succeeded and return
                for mut stream in open_streams {
                    to_writer(&mut stream, &NotAllReady).expect("Failed to send NotAllReady");
                }
                return;
            },
        }
    }
    println!("Broadcast successful");

    // If everything worked, then write AllReady to all of them.
    for mut stream in open_streams {
        to_writer(&mut stream, &AllReady).expect("Failed to send AllReady");
    }
}

fn start() {
    // setup a basic hash table.    TODO (with a mutex and arc)
    let mut hash_table = HashMap::new();
    // Setup network
    // https://stackoverflow.com/questions/51809603/why-does-serde-jsonfrom-reader-take-ownership-of-the-reader
    let listener = TcpListener::bind(("0.0.0.0", 40481))
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