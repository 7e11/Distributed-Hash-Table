use std::net::{TcpListener, TcpStream};
use cse403_distributed_hash_table::barrier::{barrier};
use cse403_distributed_hash_table::settings::{parse_settings};
use std::sync::{Arc};
use std::sync::atomic::{Ordering, AtomicU64};
use std::thread;
use cse403_distributed_hash_table::parallel::{ConcurrentHashTable};
use cse403_distributed_hash_table::transport::{Command, buffered_serialize_into, Bench};
use cse403_distributed_hash_table::parallel::LockCheck;
use cse403_distributed_hash_table::transport::CommandResponse;
use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{Write, Read};
use cse403_distributed_hash_table::statistics::{ListenerMetrics, JsonMetrics};
use bufstream::BufStream;


fn main() {
    let (client_ips, server_ips, num_ops, key_range, client_threads, replication_degree) = parse_settings()
        .expect("Unable to parse settings");
    let listener = TcpListener::bind(("0.0.0.0", 40480))
        .expect("Unable to bind listener");
    let num_clients = client_ips.len();
    let num_servers = server_ips.len();
    // Consume both vectors.
    let barrier_ips = client_ips.into_iter().chain(server_ips.into_iter()).collect();
    barrier(barrier_ips, &listener);
    // println!("Server passed barrier");

    let metrics = Arc::new(ListenerMetrics::new());

    // TODO: Make num_buckets a configurable value
    let cht =
        Arc::new(ConcurrentHashTable::new(4096, key_range, num_servers));
    let mut listen_threads = Vec::new();
    let mut server_ip = String::from("ERROR");

    // Accept exactly (num_clients * threads_per_client) connections
    for i in 0..(num_clients * client_threads as usize) {
        let (stream, addr) = listener.accept().expect("Couldn't accept connection");
        let cht_clone = cht.clone();
        let metrics_clone = metrics.clone();
        server_ip = stream.local_addr().unwrap().ip().to_string(); // For statistics

        // println!("Spawning server thread: [listener {} | {}]", i, addr);

        listen_threads.push(thread::Builder::new()
            .name(format!("[{} | {}]", stream.local_addr().unwrap(), addr))
            .spawn(move || listen_stream(cht_clone, stream, metrics_clone))
            .expect("Could't spawn thread"));
    }

    // I want some way to join only if ALL threads are done.
    // Take Statistics, when the atomic int is done, print it.
    let timer = Instant::now();
    let mut timer_delta = Instant::now();
    let mut time_series_data = Vec::new();
    let (mut put_commit_cum, mut put_abort_cum, mut get_cum, mut get_negack_cum) = (0,0,0,0);
    while metrics.threads_complete.load(Ordering::Relaxed) < listen_threads.len() as u64 {
        let time_elapsed_ms_cum = timer.elapsed().as_millis() as u64;
        let time_elapsed_ms = timer_delta.elapsed().as_millis() as u64;
        timer_delta = Instant::now(); // reset the timer
        let time_elapsed_s = time_elapsed_ms as f64 / 1000 as f64;
        let time_elapsed_s_cum = time_elapsed_ms_cum as f64 / 1000 as f64;
        let put_commit = metrics.put_commit.swap(0, Ordering::Relaxed);
        let put_abort = metrics.put_abort.swap(0, Ordering::Relaxed);
        let get = metrics.get.swap(0, Ordering::Relaxed);
        let get_negack = metrics.get_negack.swap(0, Ordering::Relaxed);
        put_commit_cum += put_commit;
        put_abort_cum += put_abort;
        get_cum += get;
        get_negack_cum += get_negack;
        let jm = JsonMetrics { time_elapsed_ms, put_commit, put_abort, get, get_negack,
            time_elapsed_ms_cum, put_commit_cum, put_abort_cum, get_cum, get_negack_cum, // Cumulative Fields
            ops: put_commit + get, ops_cum: put_commit_cum + get_cum,
            throughput: (put_commit + get) as f64 / (time_elapsed_ms as f64 / 1000 as f64),
            throughput_cum: (put_commit_cum + get_cum) as f64 / (time_elapsed_ms_cum as f64 / 1000 as f64),
            latency: (time_elapsed_ms as f64 / 1000 as f64) / (put_commit + get) as f64,
            latency_cum: (time_elapsed_ms_cum as f64 / 1000 as f64) / (put_commit_cum + get_cum) as f64,
            // Static stuff
            server_ip: server_ip.clone(), num_ops, key_range, client_threads, replication_degree };
        time_series_data.push(jm);
        thread::sleep(Duration::from_millis(50));
    }
    // Join on those threads, should work instantly b/c they all should be done.
    for jh in listen_threads {
        jh.join().unwrap();
    }
    let duation_ms = timer.elapsed().as_millis() as u64;
    // Print some cumulative Statistics for debugging
    println!();
    println!("CUMULATIVE STATISTICS");
    println!("{:<20}{:<20}{:<16}", "num_ops", "key_range", "time_ms");
    println!("{:<20}{:<20}{:<16}", (put_commit_cum + get_cum), key_range, duation_ms);
    println!("{:<20}{:<20}{:<16}{:<16}", "put_commit", "put_abort", "get", "get_negack");
    println!("{:<20}{:<20}{:<16}{:<16}", put_commit_cum, put_abort_cum, get_cum, get_negack_cum);
    println!("{:<20}{:<20}", "throughput (ops/s)", "latency (s/op)");
    println!("{:<20.3}{:<20.3}", (put_commit_cum + get_cum) as f64 / (duation_ms as f64 / 1000 as f64), (duation_ms as f64 / 1000 as f64) / (put_commit_cum + get_cum) as f64);
    println!();
    // Write the time series statistics to a JSON file.
    // let time_series_data_json = serde_json::to_vec(&time_series_data).unwrap();
    // let mut file = File::create("time_series_data.json").unwrap();
    // file.write(time_series_data_json.as_slice()).unwrap();
}

fn listen_stream(cht: Arc<ConcurrentHashTable>, mut stream: TcpStream, statistics: Arc<ListenerMetrics>) -> () {
    let mut buckets_locked = Vec::new();
    // let mut stream = BufStream::new(stream)
    loop {
        // let timer = Instant::now();
        // let mut buffer = [0u8; 16];
        // stream.read(&mut buffer).unwrap();
        // // println!("{:?}", buffer);
        // let c = bincode::deserialize(&buffer).unwrap();
        // FIXME: Custom("invalid value: integer `1819033890`, expected variant index 0 <= i < 6")'
        //  Not sure why this happens. It's 6C6C 4122 in Hex, looks structured
        let c = bincode::deserialize_from(&stream)
            .expect("Could not deserialize command");

        // println!("cmd: {:?}", c);

        match c {
            Command::Put(key, value) => {
                let hash_table_res = cht.insert(key, value);
                let cr = match hash_table_res {
                    LockCheck::LockFail => CommandResponse::NegAck,
                    LockCheck::Type(b) => CommandResponse::PutAck(b),
                };
                // println!("SERVER {:20} -> {:20} in {} ms", format!("{:?}", c), format!("{:?}", cr), timer.elapsed().as_millis());
                buffered_serialize_into(&stream, &cr).expect("Could not write Put result");
                // bincode::serialize_into(&mut stream, &resp).unwrap();
                // stream.flush();
            },
            Command::Get(key) => {
                let hash_table_res = cht.get(&key);
                let cr = match hash_table_res {
                    LockCheck::LockFail => {
                        statistics.get_negack.fetch_add(1, Ordering::Relaxed);
                        CommandResponse::NegAck
                    },
                    LockCheck::Type(o) => {
                        statistics.get.fetch_add(1, Ordering::Relaxed);
                        CommandResponse::GetAck(o)
                    },
                };
                // println!("SERVER {:20} -> {:20} in {} ms", format!("{:?}", c), format!("{:?}", cr), timer.elapsed().as_millis());
                buffered_serialize_into(&stream, &cr).expect("Could not write Get result");
                // bincode::serialize_into(&mut stream, &resp).unwrap();
                // stream.flush();
            },
            Command::Exit => {
                // Signals that we're done, and that we should collect the stream.
                // Maybe increment an AtomicInt in the Statistics thread which keeps track of the number of
                // Threads which have terminated.
                statistics.threads_complete.fetch_add(1, Ordering::Relaxed); // Is this ordering OK?
                break;
            },
            Command::PutRequest(key) => {
                // get a real lock in the hash table.
                let bucket_lock = cht.buckets.get(cht.compute_bucket(&key)).unwrap();
                let bucket_lock = bucket_lock.try_lock();
                let cr = match bucket_lock {
                    Err(_) => CommandResponse::VoteNo,
                    Ok(mutex_guard) => {
                        buckets_locked.push((key, mutex_guard));
                        CommandResponse::VoteYes
                    },
                };
                // println!("SERVER {:20} -> {:20} in {} ms", format!("{:?}", c), format!("{:?}", cr), timer.elapsed().as_millis());
                buffered_serialize_into(&stream, &cr).unwrap();
                // bincode::serialize_into(&mut stream, &resp).unwrap();
                // stream.flush();
            },
            Command::PutCommit(key, value) => {
                statistics.put_commit.fetch_add(1, Ordering::Relaxed);

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
                //Send back some random response
                buffered_serialize_into(&stream, &CommandResponse::PutCommitAck).unwrap();
            },
            Command::PutAbort => {
                statistics.put_abort.fetch_add(1, Ordering::Relaxed);


                // Dump our entire buckets_locked. After all, we're only saving locks for one thread.
                // It can only do one put / multiput operation at a time.
                // If we're aborting, everything in our table is for the multiput.
                buckets_locked.clear();

                //Send back some random response
                buffered_serialize_into(&stream, &CommandResponse::PutAbortAck).unwrap();
            }
        }
    }
}
