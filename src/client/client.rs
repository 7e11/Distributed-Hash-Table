use std::net::{TcpStream, TcpListener};
use serde_json::{to_writer};
use rand::distributions::{Bernoulli, Distribution};
use rand::{thread_rng, Rng};
use std::time::{Instant, Duration};
use std::thread;
use cse403_distributed_hash_table::settings::{parse_settings};
use cse403_distributed_hash_table::barrier::barrier;
use cse403_distributed_hash_table::transport::{KeyType, ValueType, Command, CommandResponse};
use serde::de::Deserialize;

fn main() {
    let (client_ips, server_ips, num_ops, key_range) = parse_settings()
        .expect("Unable to parse settings");
    let listener = TcpListener::bind(("0.0.0.0", 40481))
        .expect("Unable to bind listener");
    // Consume only the client_ips vector, clone server_ips.
    let barrier_ips = client_ips.into_iter().chain(server_ips.clone().into_iter()).collect();
    barrier(barrier_ips, &listener);
    println!("Client passed barrier");

    // Open a stream for each server.
    let streams = server_ips.iter()
        .map(|ip| {
            let stream = TcpStream::connect(ip).expect("Unable to connect");
            stream
        })
        .collect();


    let (mut put_success, mut put_fail, mut get_success, mut get_fail, mut neg_ack) = (0, 0, 0, 0, 0);
    let do_put_dist = Bernoulli::from_ratio(4, 10).unwrap();
    let start = Instant::now();

    for _ in 0..num_ops {
        if do_put_dist.sample(&mut thread_rng()) {
            // 40% chance to do a put
            let (b, neg_ack_incr) = put(thread_rng().gen_range(0, key_range),
                                        String::from("A"), &streams, key_range);
            neg_ack += neg_ack_incr;
            match b {
                true => put_success += 1,
                false => put_fail +=1,
            }
        } else {
            // 60% chance to do a get
            let (o, neg_ack_incr) = get(thread_rng().gen_range(0, key_range),
                                            &streams, key_range);
            neg_ack += neg_ack_incr;
            match o {
                Some(_) => get_success += 1,
                None => get_fail += 1,
            }
        }
        // Think Time (Not necessary for closed loop)
       // thread::sleep(Duration::from_micros(thread_rng().gen_range(0, 100) as u64))
    }

    // We've finished, send an Exit command to ALL streams to close the server side socket. EOF will crash the server.
    for stream in streams {
        // to_writer(stream, &Command::Exit).unwrap();
        bincode::serialize_into(stream, &Command::Exit).unwrap();
    }

    let duration = start.elapsed().as_millis();

    println!();
    println!("{:<20}{:<20}{:<20}", "num_ops", "key_range", "time_ms");
    println!("{:<20}{:<20}{:<20}", num_ops, key_range, duration);
    println!("{:<20}{:<20}{:<20}{:<20}", "put_success", "put_fail", "get_success", "get_fail");
    println!("{:<20}{:<20}{:<20}{:<20}", put_success, put_fail, get_success, get_fail);
    println!("{:<20}{:<20}", "throughput (ops/ms)", "latency (ms/op)");
    println!("{:<20.10}{:<20.10}", num_ops as f64 / duration as f64, duration as f64 / num_ops as f64);
    println!("{:<20}", "neg_ack");
    println!("{:<20}", neg_ack);
    println!();
}

fn put(key: KeyType, value: ValueType, streams: &Vec<TcpStream>, key_range: u32) -> (bool, u32) {
    // Returns the result of the put, and the number of retries as a tuple.

    let c = Command::Put(key, value);
    let index: usize = ((key as f64 / key_range as f64) * streams.len() as f64) as usize;
    let stream_ref = streams.get(index).unwrap();
    let mut retries = 0;

    loop {
        let timer = Instant::now();
        println!("Client serializing {:?}", c);

        bincode::serialize_into(stream_ref, &c).expect("Unable to write Command");
        println!("Client serialized, about to deserialize {}", timer.elapsed().as_millis());
        let cr = bincode::deserialize_from(stream_ref).unwrap();

        println!("Client got response after {} ms", timer.elapsed().as_millis());

        match cr {
            CommandResponse::PutAck(b) => break (b, retries),    // This returns
            CommandResponse::NegAck => retries += 1,
            _ => println!("Received unexpected response"),
        }
        // Exponential backoff
        thread::sleep(Duration::from_micros(thread_rng().gen_range(0, 2u32.pow(retries)) as u64));
    }
}

fn get(key: KeyType, streams: &Vec<TcpStream>, key_range: u32) -> (Option<ValueType>, u32) {
    // Returns the result of the put, and the number of retries as a tuple.

    let c = Command::Get(key);
    let index: usize = ((key as f64 / key_range as f64) * streams.len() as f64) as usize;
    let stream_ref = streams.get(index).unwrap();
    let mut retries = 0;

    loop {
        let timer = Instant::now();
        println!("Client serializing {:?}", c);

        bincode::serialize_into(stream_ref, &c).expect("Unable to write Command");
        println!("Client serialized, about to deserialize {}", timer.elapsed().as_millis());
        let cr = bincode::deserialize_from(stream_ref).unwrap();

        println!("Client got response after {} ms", timer.elapsed().as_millis());

        match cr {
            CommandResponse::GetAck(o) => break (o, retries),    // This returns
            CommandResponse::NegAck => retries += 1,
            _ => println!("Received unexpected response"),
        }
        // Exponential backoff
        thread::sleep(Duration::from_micros(thread_rng().gen_range(0, 2u32.pow(retries)) as u64));
    }
}