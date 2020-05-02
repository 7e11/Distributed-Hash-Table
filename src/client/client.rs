use std::net::{TcpStream, TcpListener};
use rand::distributions::{Bernoulli, Distribution};
use rand::{thread_rng, Rng};
use std::time::{Instant, Duration};
use std::thread;
use cse403_distributed_hash_table::settings::{parse_settings};
use cse403_distributed_hash_table::barrier::{barrier, BarrierCommand};
use cse403_distributed_hash_table::transport::{KeyType, ValueType, Command, CommandResponse, buffered_serialize_into, Bench};
use serde::de::Deserialize;
use std::collections::HashMap;

fn main() {
    let (client_ips, server_ips, num_ops, key_range, client_threads, replication_degree) = parse_settings()
        .expect("Unable to parse settings");
    let listener = TcpListener::bind(("0.0.0.0", 40481))
        .expect("Unable to bind listener");
    // Consume only the client_ips vector, clone server_ips.
    let barrier_ips = client_ips.into_iter().chain(server_ips.clone().into_iter()).collect();
    barrier(barrier_ips, &listener);
    // println!("Client passed barrier");

    // Start up the threads, then join on all of them.
    // Each thread has its own collection of streams it manages.
    let mut threads = Vec::new();
    for i in 0..client_threads {
        // println!("CLIENT Started thread {}", i);
        let server_ips_clone = server_ips.clone(); // This could be Arc instead.
        threads.push(thread::Builder::new()
            .name(format!("[client thread {}]", i))
            .spawn(move || client_thread(server_ips_clone, num_ops, key_range, replication_degree))
            .expect("Could't spawn client thread"));
    }

    // join on all the threads.
    for jh in threads {
        jh.join().unwrap();
    }
}

fn client_thread(server_ips: Vec<String>, num_ops: u32, key_range: u32, replication_degree: u32) {
    // Open a stream for each server.
    let mut streams: Vec<TcpStream> = server_ips.iter()
        .map(|ip| {
            let stream = TcpStream::connect(ip).expect("Unable to connect");
            stream
        }).collect();
    let (mut put_insert, mut put_upsert, mut get_success, mut get_fail, mut neg_ack) = (0, 0, 0, 0, 0);
    let do_put_dist = Bernoulli::from_ratio(4, 10).unwrap();
    let put_type_dist = Bernoulli::from_ratio(1, 2).unwrap();
    let start = Instant::now();
    for _ in 0..num_ops {
        if do_put_dist.sample(&mut thread_rng()) {
            // 40% chance to do a put (single or multi)
            if put_type_dist.sample(&mut thread_rng()) {
                // 20% chance for a single put
                let timer= Instant::now();
                // let (b, neg_ack_incr) = put(thread_rng().gen_range(0, key_range), 2, &mut streams, key_range);
                let neg_ack_incr = put_2pc(vec![(thread_rng().gen_range(0, key_range), 2); 1],
                                           &mut streams, key_range, replication_degree, start);
                // println!("CLIENT 2PC_SINGLE in {} ms", timer.elapsed().as_millis());
                neg_ack += neg_ack_incr;
            } else {
                // 20% chance for a multiput (3 keys)
                let timer= Instant::now();
                // FIXME: These 3 keys need to be unique, otherwise it doesn't make sense.
                let mut records: Vec<(KeyType, ValueType)> = Vec::new();
                while records.len() < 3 {
                    let key = thread_rng().gen_range(0, key_range);
                    if !records.iter().any(|(k, _v)| *k == key) {
                        records.push((key, 3));
                    }
                }
                let neg_ack_incr = put_2pc(records,
                                           &mut streams, key_range, replication_degree, start);
                // println!("CLIENT 2PC_MULTI in {} ms", timer.elapsed().as_millis());
                neg_ack += neg_ack_incr;
            }
        } else {
            // 60% chance to do a get
            let timer= Instant::now();
            let (o, neg_ack_incr) = get(thread_rng().gen_range(0, key_range),
                                        &mut streams, key_range, start);
            neg_ack += neg_ack_incr;
            // println!("CLIENT GET in {} ms", timer.elapsed().as_millis());
            match o {
                Some(_) => get_success += 1,
                None => get_fail += 1,
            }
        }
    }
    // We've finished, send an Exit command to ALL streams to close the server side socket. EOF will crash the server.
    for stream in streams {
        // to_writer(stream, &Command::Exit).unwrap();
        // bincode::serialize_into(stream, &Command::Exit).expect("Unable to write command");
        buffered_serialize_into(stream, &Command::Exit).unwrap();
    }
    // let duration = start.elapsed().as_millis();
    // println!();
    // println!("{:<20}{:<20}{:<20}", "num_ops", "key_range", "time_ms");
    // println!("{:<20}{:<20}{:<20}", num_ops, key_range, duration);
    // println!("{:<20}{:<20}{:<20}{:<20}", "put_insert", "put_upsert", "get_success", "get_fail");
    // println!("{:<20}{:<20}{:<20}{:<20}", put_insert, put_upsert, get_success, get_fail);
    // println!("{:<20}{:<20}", "throughput (ops/ms)", "latency (ms/op)");
    // println!("{:<20.10}{:<20.10}", num_ops as f64 / duration as f64, duration as f64 / num_ops as f64);
    // println!("{:<20}", "neg_ack");
    // println!("{:<20}", neg_ack);
    // println!();
}

#[allow(dead_code)]
fn put(key: KeyType, value: ValueType, streams: &mut Vec<TcpStream>, key_range: u32) -> (bool, u32) {
    // Returns the result of the put, and the number of retries as a tuple.

    let c = Command::Put(key, value);
    let index: usize = ((key as f64 / key_range as f64) * streams.len() as f64) as usize;
    let stream_ref = streams.get_mut(index).unwrap();
    let mut retries = 0;

    loop {
        let cr = send_recv_command(&mut *stream_ref, &c);
        match cr {
            CommandResponse::PutAck(o) => break (o.is_none(), retries),    // This returns
            CommandResponse::NegAck => retries += 1,
            _ => panic!("Received unexpected CR in put"),
        }
        // Exponential backoff
        thread::sleep(Duration::from_micros(thread_rng().gen_range(0, 2u32.pow(retries)) as u64));
    }
}

fn get(key: KeyType, streams: &mut Vec<TcpStream>, key_range: u32, timer: Instant) -> (Option<ValueType>, u32) {
    let c = Command::Get(key);
    let index: usize = ((key as f64 / key_range as f64) * streams.len() as f64) as usize;
    let stream_ref = streams.get_mut(index).unwrap();
    let mut retries = 0;

    loop {
        // println!("CLIENT {:10} GET send_recv START", timer.elapsed().as_millis());
        let cr = send_recv_command(&mut *stream_ref, &c);
        // println!("CLIENT {:10} GET send_recv END", timer.elapsed().as_millis());
        // println!("{:?} -> {:?}", c, cr);
        match cr {
            CommandResponse::GetAck(o) => break (o, retries),    // This returns
            CommandResponse::NegAck => retries += 1,
            _ => panic!("Received unexpected CR in get"),
        }
        // Exponential backoff
        thread::sleep(Duration::from_micros(thread_rng().gen_range(0, 2u32.pow(retries)) as u64));
    }
}

/// Only returns the # retries, while a singular put will return whether it was an insert or upsert.
fn put_2pc(records: Vec<(KeyType, ValueType)>, streams: &mut Vec<TcpStream>, key_range: u32, replication_degree: u32, timer: Instant) -> u32 {
    // Creating bundles so we don't need to send more messages than necessary
    let mut bundles = HashMap::new();
    let mut key_bundles = HashMap::new();
    for (k, v) in records {
        for replication_offset in 0..(replication_degree + 1) {
            let node_index = ((k as f64 / key_range as f64) * streams.len() as f64) as usize;
            let node_index = (node_index + replication_offset as usize) % streams.len();
            // Use Entry API for default vec insertion: https://stackoverflow.com/a/28512504/3990826
            let bundle = bundles.entry(node_index).or_insert(Vec::new());
            bundle.push((k, v));
            let key_bundle = key_bundles.entry(node_index).or_insert(Vec::new());
            key_bundle.push(k);
        }
    }

    // Constructing the commands
    let request_commands: Vec<(usize, Command)> = key_bundles.into_iter().map(|(node_index, key_bundle)| {
        (node_index, Command::PutRequest(key_bundle))
    }).collect();
    let commit_commands: Vec<(usize, Command)> = bundles.into_iter().map(|(node_index, bundle)| {
        (node_index, Command::PutCommit(bundle))
    }).collect();

    // phase1 requests
    let mut retries = 0;
    loop {
        let phase1_success = put_2pc_phase1(&mut *streams, &request_commands);
        if phase1_success {
            break;
        }
        // Send the aborts (one per relevant node), we got a no vote
        for (node_index, _) in &request_commands {
            let stream_ref = streams.get_mut(*node_index).unwrap();
            send_recv_command(&mut *stream_ref, &Command::PutAbort);
        }
        // println!("CLIENT 2PC REQUEST PHASE RESTARTING {}, RECORDS: {:?}", retries, records);
        thread::sleep(Duration::from_micros(thread_rng().gen_range(0, 2u32.pow(retries)) as u64));
        retries += 1;
    }

    // At this point, we've gotten all votes yes, and can do phase 2
    for (node_index, commit_command) in &commit_commands {
        let stream_ref = streams.get_mut(*node_index).unwrap();
        send_recv_command(&mut *stream_ref, &commit_command);
    }
    retries
}

fn put_2pc_phase1(streams: &mut Vec<TcpStream>, request_commands: &Vec<(usize, Command)>) -> bool {
    for (node_index, c) in request_commands {
        let stream_ref = streams.get_mut(*node_index).unwrap();
        let cr = send_recv_command(&mut *stream_ref, c);
        match cr {
            CommandResponse::VoteYes => (),
            CommandResponse::VoteNo => return false,
            _ => panic!("Received unexpected CR in put_2pc_phase1"),
        }
    }
    true
}


fn send_recv_command(stream_ref: &mut TcpStream, c: &Command) -> CommandResponse {
    let timer = Instant::now();
    // bincode::serialize_into(&mut *stream_ref, &c).expect("Unable to write command");
    buffered_serialize_into(&mut *stream_ref, &c).expect("Unable to write Command");
    let cr = bincode::deserialize_from(&mut *stream_ref).unwrap();
    // println!("CLIENT {:20} -> {:20} in {} ms", format!("{:?}", c), format!("{:?}", cr), timer.elapsed().as_millis());
    cr
}