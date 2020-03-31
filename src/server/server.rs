use std::net::{TcpListener, TcpStream};
use cse403_distributed_hash_table::barrier::{barrier};
use cse403_distributed_hash_table::settings::{parse_settings};
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use cse403_distributed_hash_table::parallel::{ConcurrentHashTable};
use cse403_distributed_hash_table::transport::Command;
use cse403_distributed_hash_table::parallel::LockCheck;
use cse403_distributed_hash_table::transport::CommandResponse;
use serde_json::to_writer;
use serde::de::Deserialize;
use std::time::Duration;

fn main() {
    let (client_ips, server_ips, _, key_range) = parse_settings()
        .expect("Unable to parse settings");
    let listener = TcpListener::bind(("0.0.0.0", 40480))
        .expect("Unable to bind listener");
    let num_clients = client_ips.len();
    let num_servers = server_ips.len();
    // Consume both vectors.
    let barrier_ips = client_ips.into_iter().chain(server_ips.into_iter()).collect();
    barrier(barrier_ips, &listener);
    println!("Server passed barrier");

    let threads_complete = Arc::new(AtomicU32::new(0));
    let cht =
        Arc::new(ConcurrentHashTable::new(4096, key_range, num_servers));
    let mut listen_threads = Vec::new();

    // Accept exactly (num_clients * threads_per_client) connections
    for i in 0..(num_clients * 1) {
        let (stream, addr) = listener.accept().expect("Couldn't accept connection");
        let cht_clone = cht.clone();
        let counter_clone = threads_complete.clone();

        listen_threads.push(thread::Builder::new()
            .name(format!("[listener {} | {}]", i, addr))
            .spawn(move || listen_stream(cht_clone, stream, counter_clone))
            .expect("Could't spawn thread"));
    }

    // // I want some way to join only if ALL threads are done.
    // // Take statistics, when the atomic int is done, print it.
    // while threads_complete.load(Ordering::Relaxed) < listen_threads.len() as u32 {
    //     // Take statistics, maybe just a counter of operations for now?
    //     // Should i keep track of time as well?
    //
    //     thread::sleep(Duration::from_millis(50));
    // }
    // Join on those threads, should work instantly b/c they all should be done.
    for jh in listen_threads {
        jh.join().unwrap();
    }
    // Print statistics.
    println!("Server complete")
}

fn listen_stream(cht: Arc<ConcurrentHashTable>, stream: TcpStream, counter: Arc<AtomicU32>) -> () {
    // let mut de = serde_json::Deserializer::from_reader(&stream);
    loop {
        // Deserialize from stream https://github.com/serde-rs/json/issues/522
        // let mut de = serde_json::Deserializer::from_reader(&stream);
        // let c = Command::deserialize(&mut de).expect("Could not deserialize command.");
        let c = bincode::deserialize_from(&stream).unwrap();
        println!("{} Received: {:?}", thread::current().name().unwrap(), c);
        match c {
            Command::Put(key, value) => {
                let hash_table_res = cht.insert_if_absent(key, value);
                let resp = match hash_table_res {
                    LockCheck::LockFail => CommandResponse::NegAck,
                    LockCheck::Type(b) => CommandResponse::PutAck(b),
                };
                // println!("{} Response: {:?}", thread::current().name().unwrap(), resp);
                // TODO: Why doesn't this need to be mutable?
                // to_writer(&stream, &resp).expect("Could not write Put result");
                bincode::serialize_into(&stream, &resp).expect("Could not write Put result");
            },
            Command::Get(key) => {
                let hash_table_res = cht.get(&key);
                let resp = match hash_table_res {
                    LockCheck::LockFail => CommandResponse::NegAck,
                    LockCheck::Type(o) => CommandResponse::GetAck(o),
                };
                // println!("{} Response: {:?}", thread::current().name().unwrap(), resp);
                // to_writer(&stream, &resp).expect("Could not write Get result");
                bincode::serialize_into(&stream, &resp).expect("Could not write Get result");
            },
            Command::Exit => {
                // Signals that we're done, and that we should collect the stream.
                // Maybe increment an AtomicInt in the statistics thread which keeps track of the number of
                // Threads which have terminated.
                counter.fetch_add(1, Ordering::Relaxed); // Is this ordering OK?
                break;
            }
        }
    }
}
