use std::net::{TcpListener, TcpStream};
use std::collections::{VecDeque};

use cse403_distributed_hash_table::barrier::{barrier};
use cse403_distributed_hash_table::settings::{parse_settings};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use cse403_distributed_hash_table::parallel::{ConcurrentHashTable, worker_func};
use cse403_distributed_hash_table::transport::Command;
use cse403_distributed_hash_table::transport::Command::{Put, Get};
use cse403_distributed_hash_table::parallel::LockCheck::{LockFail, Type};
use cse403_distributed_hash_table::transport::CommandResponse::{NegAck, PutAck, GetAck};
use serde_json::to_writer;


fn main() {
    let (client_ips, server_ips, _, key_range) = parse_settings()
        .expect("Unable to parse settings");
    let listener = TcpListener::bind(("0.0.0.0", 40480))
        .expect("Unable to bind listener");
    let num_servers = server_ips.len();
    // Consume both vectors.
    let barrier_ips = client_ips.into_iter().chain(server_ips.into_iter()).collect();
    barrier(barrier_ips, &listener);
    println!("Server passed barrier");
    let work_queue_arc= Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
    setup_workers(key_range, num_servers, &work_queue_arc);
    listen(listener, work_queue_arc)
}

fn setup_workers(key_range: u32, num_servers: usize, work_queue_arc: &Arc<(Mutex<VecDeque<TcpStream>>, Condvar)>) {
    let num_threads = 4;
    let mut threads = Vec::with_capacity(num_threads);
    let hash_table_arc =
        Arc::new(ConcurrentHashTable::new(4096, key_range, num_servers));
    // See: https://stackoverflow.com/questions/29870837/how-do-i-use-a-condvar-to-limit-multithreading
    for i in 0..num_threads {
        let ht_arc = hash_table_arc.clone();
        let wq_arc = work_queue_arc.clone();
        threads.push(thread::Builder::new()
            .name(format!("[Worker {}]", i))
            .spawn(move || worker_func(ht_arc, wq_arc)));
    }
}

fn listen(listener: TcpListener, work_queue_arc: Arc<(Mutex<VecDeque<TcpStream>>, Condvar)>) -> () {
    // Setup Listener thread
    // https://stackoverflow.com/questions/51809603/why-does-serde-jsonfrom-reader-take-ownership-of-the-reader
    for res_stream in listener.incoming() {
        match res_stream {
            Err(e) => eprintln!("Couldn't accept connection: {}", e),
            Ok(stream) => {
                let (queue_lock, cvar) = &*work_queue_arc;
                let mut queue = queue_lock.lock().unwrap(); //BLOCKING
                queue.push_back(stream);
                drop(queue); //Drop the lock on the queue FIXME Necessary before notify?
                cvar.notify_one();
            },
        }
    }
}

pub fn worker_func(ht_arc: Arc<ConcurrentHashTable>, wq_arc: Arc<(Mutex<VecDeque<TcpStream>>, Condvar)>) -> ! {
    let (queue_lock, cvar) = &*wq_arc;
    loop {
        let mut queue = queue_lock.lock().unwrap();
        while queue.is_empty() {
            // Weird: https://stackoverflow.com/questions/56939439/how-do-i-use-a-condvar-without-moving-the-mutex-variable
            queue = cvar.wait(queue).unwrap();
//                    println!("{} Woke Up", thread::current().name().unwrap())
        }
        let mut s = queue.pop_front().unwrap();
        drop(queue);    // Drop the lock on the queue.

        handle_stream(&ht_arc, &mut s)
    }
}

fn handle_stream(ht_arc: &Arc<ConcurrentHashTable>, mut s: &mut TcpStream) -> () {
    // Deserialize from stream
    // https://github.com/serde-rs/json/issues/522
    let mut de = serde_json::Deserializer::from_reader(&mut s);
    let c = Command::deserialize(&mut de).expect("Could not deserialize command.");
//        println!("{} Received: {:?} From {:?}", thread::current().name().unwrap(), c, s);
    // Handle the command
    match c {
        Put(key, value) => {
            let hash_table_res = ht_arc.insert_if_absent(key, value);
            let resp = match hash_table_res {
                LockFail => NegAck,
                Type(b) => PutAck(b),
            };
//               println!("{} Response: {:?}", thread::current().name().unwrap(), resp);
            to_writer(&mut s, &resp)
                .expect("Could not write Put result");
        },
        Get(key) => {
            let hash_table_res = ht_arc.get(&key);
            let resp = match hash_table_res {
                LockFail => NegAck,
                Type(o) => GetAck(o),
            };
//                println!("{} Response: {:?}", thread::current().name().unwrap(), resp);
            to_writer(&mut s, &resp)
                .expect("Could not write Get result");
        },
    }
}

