use std::net::{TcpStream, TcpListener};
use rand::distributions::{Bernoulli, Distribution};
use rand::{thread_rng, Rng};
use std::time::{Instant, Duration};
use std::thread;
use cse403_distributed_hash_table::settings::{parse_settings};
use cse403_distributed_hash_table::barrier::{barrier, BarrierCommand};
use cse403_distributed_hash_table::transport::{KeyType, ValueType, Command, CommandResponse, buffered_serialize_into, Bench};
use serde::de::Deserialize;

fn main() {
    // debug();
    // return;
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
                // FIXME: These 3 keys need to be unique, else we get deadlock.
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
    let timer_request = Instant::now();
    let mut retries = 0;
    loop {
        let mut received_voteno = false;
        'request_loop: for (key, _value) in &records {
            let c = Command::PutRequest(*key);  // FIXME: This will fail if using a non-copy key type
            for replication_offset in 0..(replication_degree + 1) {
                // Normalizes key between 0..1 (exclusive),
                // then multiplies by streams.len() to get indicies from 0..streams.len (exclusive)
                let index: usize = ((*key as f64 / key_range as f64) * streams.len() as f64) as usize;
                let index = (index + replication_offset as usize) % streams.len();
                let stream_ref = streams.get_mut(index).unwrap();
                // println!("CLIENT {:10} PUT send_recv START", timer.elapsed().as_millis());
                let cr = send_recv_command(&mut *stream_ref, &c);
                // println!("CLIENT {:10} PUT send_recv END", timer.elapsed().as_millis());
                match cr {
                    CommandResponse::VoteYes => (),
                    CommandResponse::VoteNo => {
                        received_voteno = true;
                        retries += 1;
                        break 'request_loop;
                    },
                    _ => panic!("Received unexpected CR in put_2pc"),
                }
            }
        }
        if !received_voteno {
            break;
        }
        // Send all the aborts, we got a no vote
        // FIXME: If 2 keys are on the same server, this will send more aborts to that server than necessary
        // Not really a critical problem though, so not worrying about it for now.
        for (key, _value) in &records {
            let c = Command::PutAbort;
            for replication_offset in 0..(replication_degree + 1) {
                let index: usize = ((*key as f64 / key_range as f64) * streams.len() as f64) as usize;
                let index = (index + replication_offset as usize) % streams.len();
                let stream_ref = streams.get_mut(index).unwrap();
                // bincode::serialize_into(&mut *stream_ref, &c).expect("Unable to write command");
                send_recv_command(&mut *stream_ref, &c);
            }
        }
        // println!("CLIENT 2PC REQUEST PHASE RESTARTING");
        thread::sleep(Duration::from_micros(thread_rng().gen_range(0, 2u32.pow(retries)) as u64));
    }

    // println!("CLIENT 2PC REQUEST PHASE in {} ms", timer_request.elapsed().as_millis());
    let timer_commit = Instant::now();

    // At this point, we've gotten all votes yes, and can do phase 2
    // If 2 keys are on the same server, it will get putcommit for both keys.
    // Not an error, but can make metrics look weird.
    for (key, value) in records {
        let c = Command::PutCommit(key, value);
        for replication_offset in 0..(replication_degree + 1) {
            let index: usize = ((key as f64 / key_range as f64) * streams.len() as f64) as usize;
            let index = (index + replication_offset as usize) % streams.len();
            let stream_ref = streams.get_mut(index).unwrap();
            // bincode::serialize_into(&mut *stream_ref, &c).expect("Unable to write command");
            // println!("CLIENT {:10} PUT ser_into START", timer.elapsed().as_millis());
            //FIXME: Removing the following line resolves the strange 40ms delay.
            // Why does it happen? Let's try replacing it with a read+write
            // buffered_serialize_into(&mut *stream_ref, &c).expect("Unable to write Command");
            send_recv_command(&mut *stream_ref, &c);
            // println!("CLIENT {:10} PUT ser_into END", timer.elapsed().as_millis());
            // We don't have any acks for this
        }
    }
    // println!("CLIENT 2PC COMMIT PHASE in {} ms", timer_commit.elapsed().as_millis());
    // println!("CLIENT 2PC TOTAL in {} ms", timer_request.elapsed().as_millis());
    retries
}

fn send_recv_command(stream_ref: &mut TcpStream, c: &Command, ) -> CommandResponse {
    let timer = Instant::now();
    // bincode::serialize_into(&mut *stream_ref, &c).expect("Unable to write command");
    buffered_serialize_into(&mut *stream_ref, &c).expect("Unable to write Command");
    let cr = bincode::deserialize_from(&mut *stream_ref).unwrap();
    // println!("CLIENT {:20} -> {:20} in {} ms", format!("{:?}", c), format!("{:?}", cr), timer.elapsed().as_millis());
    cr
}

fn debug() {
    println!("AllReady: {:?}", bincode::serialize(&BarrierCommand::AllReady).unwrap());
    println!("NotAllReady: {:?}", bincode::serialize(&BarrierCommand::NotAllReady).unwrap());
    println!("PutRequest: {:?}", bincode::serialize(&Command::PutRequest(99)).unwrap());
    println!("Get: {:?}", bincode::serialize(&Command::Get(99)).unwrap());
}