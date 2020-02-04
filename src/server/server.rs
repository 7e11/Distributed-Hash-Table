use std::net::{TcpListener, TcpStream, SocketAddr};
use std::collections::{HashMap};
use serde_json::{to_writer};

use cse403_distributed_hash_table::protocol::{Command, BarrierCommand};
use cse403_distributed_hash_table::protocol::Command::{Get, Put};
use cse403_distributed_hash_table::protocol::BarrierCommand::{AllReady, NotAllReady};
use serde::Deserialize;
use std::thread;
use std::time::Duration;
use std::path::Path;
use std::str::FromStr;
use config::{ConfigError};


fn main() {
    let node_ips = parse_settings().expect("Failed to parse settings");
    println!("Starting barrier");
    barrier(node_ips);
    println!("Starting application listener");
    application_listener()
}

fn parse_settings() -> Result<Vec<String>, ConfigError> {
    // Expand the Vec<String> to encompass more types as the settings file increases in size.
    // See: https://github.com/mehcode/config-rs/blob/master/examples/simple/src/main.rs
    let mut config = config::Config::default();
    // Add in server_settings.yaml
    config.merge(config::File::from(Path::new("./settings/server_settings.yaml")))?;
//    println!("{:#?}", config);
    let node_ips = config.get_array("node_ips")?;
    let node_ips = node_ips.into_iter().map(|s| s.into_str()
        .expect("Could not parse IP into str")).collect();
    Ok(node_ips)
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
            Ok(stream) => {
                let mut de = serde_json::Deserializer::from_reader(stream);
                let bc = BarrierCommand::deserialize(&mut de)
                    .expect("Could not deserialize barrier command.");
                match bc {
                    AllReady => break,
                    NotAllReady => (),
                }
            },
        }
    }
    handle.join().expect("Failed to join on broadcast thread");
}

fn barrier_broadcast(node_ips: Vec<String>) {
    // Attempt to open a TCP connection with everyone.
    // east, west, central
    let mut open_streams = Vec::new();

    // This expends the node_ips iterator and takes ownership of the ips.
    // See: http://xion.io/post/code/rust-for-loop.html
    for ip in node_ips {
        let stream_res = TcpStream::connect_timeout(
            &SocketAddr::from_str(ip.as_str()).expect("Could not parse IP"),
            Duration::from_secs(5));
        match stream_res {
            Ok(stream) => open_streams.push(stream),
            Err(e) => {
                println!("Failed broadcast to {} {}", ip, e);
                // write NotAllReady to the ones that succeeded and return
                for stream in open_streams {
                    // This will consume open_streams.
                    to_writer(stream, &NotAllReady).expect("Failed to send NotAllReady");
                }
                return
            },
        }
    }
    println!("Broadcast successful");

    // If everything worked, then write AllReady to all of them. This also consumes open_streams
    for stream in open_streams {
        to_writer(stream, &AllReady).expect("Failed to send AllReady");
    }
}

fn application_listener() {
    // setup a basic hash table.    TODO (with a mutex and arc)
    let mut hash_table = HashMap::new();
    // Setup network
    // https://stackoverflow.com/questions/51809603/why-does-serde-jsonfrom-reader-take-ownership-of-the-reader
    let listener = TcpListener::bind("0.0.0.0:40481")
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