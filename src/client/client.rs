use std::net::{TcpStream, TcpListener};
use serde_json::{to_writer};
use rand::distributions::{Bernoulli, Distribution};
use rand::{thread_rng, Rng};
use std::time::{Instant, Duration};
use std::thread;
use cse403_distributed_hash_table::settings::{parse_settings};
use cse403_distributed_hash_table::barrier::barrier;
use cse403_distributed_hash_table::transport::{KeyType, ValueType, Command, CommandResponse, buffered_serialize_into};
use serde::de::Deserialize;
use std::io::{Read, Write};

fn main() {
    let (client_ips, server_ips, num_ops, key_range, client_threads, replication_degree) = parse_settings()
        .expect("Unable to parse settings");
    let listener = TcpListener::bind(("0.0.0.0", 40481))
        .expect("Unable to bind listener");
    // Consume only the client_ips vector, clone server_ips.
    let barrier_ips = client_ips.into_iter().chain(server_ips.clone().into_iter()).collect();
    barrier(barrier_ips, &listener);
    // println!("Client passed barrier");

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
                // let (b, neg_ack_incr) = put(thread_rng().gen_range(0, key_range), 2, &mut streams, key_range);
                let neg_ack_incr = put_2pc(vec![(thread_rng().gen_range(0, key_range), 2); 1],
                                           &mut streams, key_range, replication_degree);
                neg_ack += neg_ack_incr;
            } else {
                // 20% chance for a multiput (3 keys)
                // FIXME: These 3 keys need to be unique, else we get deadlock.
                let mut records: Vec<(KeyType, ValueType)> = Vec::new();
                while records.len() < 3 {
                    let key = thread_rng().gen_range(0, key_range);
                    if !records.iter().any(|(k, v)| *k == key) {
                        records.push((key, 3));
                    }
                }
                let neg_ack_incr = put_2pc(records,
                                           &mut streams, key_range, replication_degree);
                neg_ack += neg_ack_incr;
            }
        } else {
            // 60% chance to do a get
            let (o, neg_ack_incr) = get(thread_rng().gen_range(0, key_range),
                                            &mut streams, key_range);
            neg_ack += neg_ack_incr;
            match o {
                Some(_) => get_success += 1,
                None => get_fail += 1,
            }
        }
    }

    // We've finished, send an Exit command to ALL streams to close the server side socket. EOF will crash the server.
    for stream in streams {
        // to_writer(stream, &Command::Exit).unwrap();
        buffered_serialize_into(stream, &Command::Exit).unwrap();
    }

    let duration = start.elapsed().as_millis();

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

fn get(key: KeyType, streams: &mut Vec<TcpStream>, key_range: u32) -> (Option<ValueType>, u32) {
    let c = Command::Get(key);
    let index: usize = ((key as f64 / key_range as f64) * streams.len() as f64) as usize;
    let stream_ref = streams.get_mut(index).unwrap();
    let mut retries = 0;

    loop {
        let cr = send_recv_command(&mut *stream_ref, &c);
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
fn put_2pc(records: Vec<(KeyType, ValueType)>, streams: &mut Vec<TcpStream>, key_range: u32, replication_degree: u32) -> u32 {
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

                let cr = send_recv_command(&mut *stream_ref, &c);
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
                buffered_serialize_into(&mut *stream_ref, &c).expect("Unable to write Command");
                // We don't have any acks for this
            }
        }

        thread::sleep(Duration::from_micros(thread_rng().gen_range(0, 2u32.pow(retries)) as u64));
    }

    // At this point, we've gotten all votes yes, and can do phase 2
    for (key, value) in records {
        let c = Command::PutCommit(key, value);
        for replication_offset in 0..(replication_degree + 1) {
            let index: usize = ((key as f64 / key_range as f64) * streams.len() as f64) as usize;
            let index = (index + replication_offset as usize) % streams.len();
            let stream_ref = streams.get_mut(index).unwrap();
            buffered_serialize_into(&mut *stream_ref, &c).expect("Unable to write Command");
            // We don't have any acks for this
        }
    }
    retries
}

fn send_recv_command(stream_ref: &mut TcpStream, c: &Command, ) -> CommandResponse {
    // let timer = Instant::now();
    buffered_serialize_into(&mut *stream_ref, &c).expect("Unable to write Command");
    let cr = bincode::deserialize_from(&mut *stream_ref).unwrap();
    // println!("Client got response after {} ms", timer.elapsed().as_millis());
    cr
}
