use std::net::TcpStream;
use std::io::Write;
use serde_json::{to_vec, to_writer, to_writer_pretty, from_reader};
use rand::distributions::{Bernoulli, Distribution};
use rand::{thread_rng, Rng};
use std::time::{Instant, Duration};

use cse403_distributed_hash_table::protocol::{Command, KeyType, ValueType};


fn main() {
    let num_ops: u64 = 100;  //TODO: Get this and keyrange from the settings file
    let (mut put_success, mut put_fail, mut get_success, mut get_fail) = (0, 0, 0, 0);
    let key_range = 1_000;
    let do_put_dist = Bernoulli::from_ratio(4, 10).unwrap();
    let start = Instant::now();

    for _ in 0..num_ops {
        if do_put_dist.sample(&mut thread_rng()) {
            // 40% chance to do a put
            let b = put(thread_rng().gen_range(0, key_range), String::from("AAA"));
            match b {
                true => put_success += 1,
                false => put_fail +=1,
            }
        } else {
            // 60% chance to do a get
            let o = get(thread_rng().gen_range(0, key_range));
            match o {
                Some(_) => get_success += 1,
                None => get_fail += 1,
            }
        }
    }

    let duration = start.elapsed().as_millis();

    println!("{:<20}{:<20}{:<20}", "num_ops", "key_range", "time_ms");
    println!("{:<20}{:<20}{:<20}", num_ops, key_range, duration);
    println!("{:<20}{:<20}{:<20}{:<20}", "put_success", "put_fail", "get_success", "get_fail");
    println!("{:<20}{:<20}{:<20}{:<20}", put_success, put_fail, get_success, get_fail);
    println!("{:<20}{:<20}", "throughput (ops/ms)", "latency (ms/ops)");
    println!("{:<20}{:<20}", (num_ops as u128) / duration, duration / (num_ops as u128));
}

fn put(key: KeyType, value: ValueType) -> bool {
    // It's fine to remake the stream repeatedly, as we don't know where the key will be next.
    let mut stream = TcpStream::connect(("127.0.0.1", 40481))
        .expect("Unable to connect");
    let c = Command::Put(key, value);
    to_writer(&mut stream, &c).expect("Unable write Put");

    from_reader(&mut stream).expect("Unable to read Put")
}

fn get(key: KeyType) -> Option<ValueType> {
    let mut stream = TcpStream::connect(("127.0.0.1", 40481))
        .expect("Unable to connect");
    let c = Command::Get(key);
    to_writer(&mut stream,&c).expect("Unable to write Get");

    from_reader(&mut stream).expect("Unable to read Get")
}
