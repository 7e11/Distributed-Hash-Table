use std::net::{TcpListener, TcpStream};
use cse403_distributed_hash_table::barrier::{barrier};
use cse403_distributed_hash_table::settings::{parse_settings};
use std::sync::{Arc};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use cse403_distributed_hash_table::parallel::{ConcurrentHashTable};
use cse403_distributed_hash_table::transport::{Command, buffered_serialize_into};
use cse403_distributed_hash_table::parallel::LockCheck;
use cse403_distributed_hash_table::transport::CommandResponse;
use serde::de::Deserialize;
use std::time::{Duration, Instant};

fn main() {
    let (client_ips, server_ips, _num_ops, key_range, client_threads, replication_degree) = parse_settings()
        .expect("Unable to parse settings");
    let listener = TcpListener::bind(("0.0.0.0", 40480))
        .expect("Unable to bind listener");
    let num_clients = client_ips.len();
    let num_servers = server_ips.len();
    // Consume both vectors.
    let barrier_ips = client_ips.into_iter().chain(server_ips.into_iter()).collect();
    barrier(barrier_ips, &listener);
    // println!("Server passed barrier");

    let threads_complete = Arc::new(AtomicU32::new(0));
    let cht =
        Arc::new(ConcurrentHashTable::new(4096, key_range, num_servers));
    let mut listen_threads = Vec::new();

    // Accept exactly (num_clients * threads_per_client) connections
    for i in 0..(num_clients * client_threads as usize) {
        let (stream, addr) = listener.accept().expect("Couldn't accept connection");
        let cht_clone = cht.clone();
        let counter_clone = threads_complete.clone();

        // println!("Spawning server thread: [listener {} | {}]", i, addr);

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
    // println!("Server done accepting connections, joining on listening threads");
    for jh in listen_threads {
        jh.join().unwrap();
    }
    // Print statistics.
    // println!("Server complete")
}

fn listen_stream(cht: Arc<ConcurrentHashTable>, stream: TcpStream, counter: Arc<AtomicU32>) -> () {
    let mut buckets_locked = Vec::new();
    loop {
        let c = bincode::deserialize_from(&stream).unwrap();
        // println!("{} Received: {:?}", thread::current().name().unwrap(), c);
        match c {
            Command::Put(key, value) => {
                let hash_table_res = cht.insert(key, value);
                let resp = match hash_table_res {
                    LockCheck::LockFail => CommandResponse::NegAck,
                    LockCheck::Type(b) => CommandResponse::PutAck(b),
                };
                // println!("{} Response: {:?}", thread::current().name().unwrap(), resp);
                buffered_serialize_into(&stream, &resp).expect("Could not write Put result");
            },
            Command::Get(key) => {
                let hash_table_res = cht.get(&key);
                let resp = match hash_table_res {
                    LockCheck::LockFail => CommandResponse::NegAck,
                    LockCheck::Type(o) => CommandResponse::GetAck(o),
                };
                // println!("{} Response: {:?}", thread::current().name().unwrap(), resp);
                buffered_serialize_into(&stream, &resp).expect("Could not write Get result");
            },
            Command::Exit => {
                // Signals that we're done, and that we should collect the stream.
                // Maybe increment an AtomicInt in the statistics thread which keeps track of the number of
                // Threads which have terminated.
                counter.fetch_add(1, Ordering::Relaxed); // Is this ordering OK?
                break;
            },
            Command::PutRequest(key) => {
                // get a real lock in the hash table.
                let bucket_lock = cht.buckets.get(cht.compute_bucket(&key)).unwrap();
                let bucket_lock = bucket_lock.try_lock();
                match bucket_lock {
                    Err(_) => buffered_serialize_into(&stream, &CommandResponse::VoteNo).unwrap(),
                    Ok(mutex_guard) => {
                        buckets_locked.push((key, mutex_guard));
                        buffered_serialize_into(&stream, &CommandResponse::VoteYes).unwrap()
                    },
                };
            },
            Command::PutCommit(key, value) => {
                // do the put operation with the locks we have acquired
                let buckets_locked_index = buckets_locked.iter().position(|(k, _)| *k == key).unwrap();
                let (_k, mut bucket) = buckets_locked.remove(buckets_locked_index);
                let index = bucket.iter().position(|(k, _)| *k == key);
                if let Some(i) = index {
                    // The key exists, update it.
                    let pair = bucket.get_mut(i).unwrap();
                    pair.1 = value;
                } else {
                    // The key doesn't exist yet
                    bucket.push((key, value));
                }
            },
            Command::PutAbort => {
                // Dump our entire buckets_locked. After all, we're only saving locks for one thread.
                // It can only do one put / multiput operation at a time.
                // All of our saved locks are pushed out of scope.
                buckets_locked.clear();
            }
        }
    }
}
