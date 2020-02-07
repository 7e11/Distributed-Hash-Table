use std::net::{TcpListener, TcpStream};
use std::collections::{HashMap, VecDeque};
use serde_json::{to_writer};

use cse403_distributed_hash_table::protocol::{Command, barrier};
use cse403_distributed_hash_table::protocol::Command::{Get, Put};
use serde::Deserialize;
use std::path::Path;
use config::{ConfigError};
use cse403_distributed_hash_table::protocol::CommandResponse::{GetAck, PutAck, NegAck};
use cse403_distributed_hash_table::settings::parse_ips;
use std::sync::{Arc, Mutex, Condvar};
use std::thread;

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

    let hash_table_arc = Arc::new(Mutex::new(HashMap::new()));
    let work_queue_arc= Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
    let num_threads = 4;
    let mut threads = Vec::new();
    // See: https://stackoverflow.com/questions/29870837/how-do-i-use-a-condvar-to-limit-multithreading
    for i in 0..num_threads {
        let ht_arc = hash_table_arc.clone();
        let wq_arc = work_queue_arc.clone();

        threads.push(thread::Builder::new().name(format!("[Worker {}]", i)).spawn(move || {
            // FIXME: Why is this &* necessary ?
            let (queue_lock, cvar) = &*wq_arc;
            loop {
                let mut queue = queue_lock.lock().unwrap();
                while queue.is_empty() {
                    // Weird: https://stackoverflow.com/questions/56939439/how-do-i-use-a-condvar-without-moving-the-mutex-variable
                    queue = cvar.wait(queue).unwrap();
                }
                let mut s = queue.pop_front().unwrap();
                drop(queue);    // Drop the lock on the queue.

                // HANDLE STREAM
                // https://github.com/serde-rs/json/issues/522
                let mut de = serde_json::Deserializer::from_reader(&mut s);
                let c = Command::deserialize(&mut de).expect("Could not deserialize command.");
//                println!("{} Received: {:?}", thread::current().name().unwrap(), c);
                match c {
                    Put(key, value) => {
                        let hash_table_res = ht_arc.try_lock();
                        let resp = match hash_table_res {
                            Err(e) => {
//                                println!("{} Failed lock", thread::current().name().unwrap());
                                NegAck
                            },
                            Ok(mut hash_table) => {
//                                println!("{} Acquired lock", thread::current().name().unwrap());
                                if hash_table.contains_key(&key) {
                                    PutAck(false)
                                } else {
                                    hash_table.insert(key, value);
                                    PutAck(true)
                                }
                            },
                        };
                        to_writer(&mut s, &resp)
                            .expect("Could not write Put result");
                    },
                    Get(key) => {
                        let hash_table_res = ht_arc.try_lock();
                        let resp = match hash_table_res {
                            Err(e) => {
//                                println!("{} Failed lock", thread::current().name().unwrap());
                                NegAck
                            },
                            Ok(hash_table) => {
//                                println!("{} Acquired lock", thread::current().name().unwrap());
                                let opt = hash_table.get(&key);
                                // TODO: I'll need to unwrap this here in order to clone it. Is there a better way?
                                let opt = match opt {
                                    Some(vt) => Some(vt.clone()),
                                    None => None,
                                };
                                GetAck(opt)
                            },
                        };
                        to_writer(&mut s, &resp)
                            .expect("Could not write Get result");
                    },
                }

                // FIXME: Do I need to drop more stuff ???

            }
        }));
    }

    // Setup network
    // https://stackoverflow.com/questions/51809603/why-does-serde-jsonfrom-reader-take-ownership-of-the-reader
    for res_stream in listener.incoming() {
        match res_stream {
            Err(e) => eprintln!("Couldn't accept connection: {}", e),
            Ok(mut stream) => {
                // Lock the queue
                // Add the stream to it
                // Notify one
                let (queue_lock, cvar) = &*work_queue_arc;
                let mut queue = queue_lock.lock().unwrap(); //BLOCKING
                queue.push_back(stream);
                drop(queue); //Drop the lock on the queue FIXME Necessary?
                cvar.notify_one();
            },
        }
    }
}