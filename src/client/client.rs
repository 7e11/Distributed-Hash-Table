use std::net::TcpStream;
use serde_json::{to_writer, from_reader};
use rand::distributions::{Bernoulli, Distribution};
use rand::{thread_rng, Rng};
use std::time::{Instant, Duration};

use cse403_distributed_hash_table::protocol::{Command, KeyType, ValueType, barrier, CommandResponse};
use config::ConfigError;
use std::path::Path;
use cse403_distributed_hash_table::protocol::CommandResponse::{PutAck, GetAck, NegAck};
use std::thread;


fn main() {
    // Parse settings
    let (client_ips, server_ips, num_ops, key_range) = parse_settings()
        .expect("Unable to parse settings");
    println!("Starting barrier");
    // Consume only the client_ips vector, clone server_ips.
    let barrier_ips = client_ips.into_iter().chain(server_ips.clone().into_iter()).collect();
    barrier(barrier_ips, 40481);
    println!("Starting application");
//    application_listener()

    let (mut put_success, mut put_fail, mut get_success, mut get_fail, mut neg_ack) = (0, 0, 0, 0, 0);
    let do_put_dist = Bernoulli::from_ratio(4, 10).unwrap();
    let start = Instant::now();

    for _ in 0..num_ops {
        if do_put_dist.sample(&mut thread_rng()) {
            // 40% chance to do a put
            let (b, neg_ack_incr) = put(thread_rng().gen_range(0, key_range),
                                        String::from("A"), &server_ips, key_range);
            neg_ack += neg_ack_incr;
            match b {
                true => put_success += 1,
                false => put_fail +=1,
            }
        } else {
            // 60% chance to do a get
            let (o, neg_ack_incr) = get(thread_rng().gen_range(0, key_range),
                                &server_ips, key_range);
            neg_ack += neg_ack_incr;
            match o {
                Some(_) => get_success += 1,
                None => get_fail += 1,
            }
        }
        // Think Time
        thread::sleep(Duration::from_micros(thread_rng().gen_range(0, 1_000) as u64))
    }

    let duration = start.elapsed().as_millis();

    println!("{:<20}{:<20}{:<20}", "num_ops", "key_range", "time_ms");
    println!("{:<20}{:<20}{:<20}", num_ops, key_range, duration);
    println!("{:<20}{:<20}{:<20}{:<20}", "put_success", "put_fail", "get_success", "get_fail");
    println!("{:<20}{:<20}{:<20}{:<20}", put_success, put_fail, get_success, get_fail);
    println!("{:<20}{:<20}", "throughput (ops/ms)", "latency (ms/ops)");
    println!("{:<20}{:<20}", (num_ops as u128) / duration, duration / (num_ops as u128));
    println!("{:<20}", "neg_ack");
    println!("{:<20}", neg_ack);
}

fn parse_settings() -> Result<(Vec<String>, Vec<String>, i32, i32), ConfigError> {
    // Returns client_ips, server_ips, num_ops, key_range all as a tuple.

    let mut config = config::Config::default();
    // Add in server_settings.yaml
    config.merge(config::File::from(Path::new("./settings/client_settings.yaml")))?;
//    println!("{:#?}", config);
    let client_ips = config.get_array("client_ips")?;
    let server_ips = config.get_array("server_ips")?;

    // Now convert them to strings
    let client_ips = client_ips.into_iter()
        .map(|s| s.into_str().expect("Could not parse IP into str"))
        .collect();
    let server_ips = server_ips.into_iter()
        .map(|s| s.into_str().expect("Could not parse IP into str"))
        .collect();

    // Gather the other settings
    let num_ops = config.get_int("num_ops")?;
    let key_range = config.get_int("key_range")?;

    Ok((client_ips, server_ips, num_ops as i32, key_range as i32))
}

fn put(key: KeyType, value: ValueType, node_ips: &Vec<String>, key_range: i32) -> (bool, u32) {
    // Returns the result of the put, and the number of retries as a tuple.

    let c = Command::Put(key, value);
    let server_ip = map_server_ip(key, node_ips, key_range);
    let mut retries = 0;

    // TODO: Keep the socket open between retries (?)
    loop {
        let cr: CommandResponse = write_command(&c, server_ip);
        match cr {
            PutAck(b) => break (b, retries),    // This returns
            NegAck => retries += 1,
            _ => println!("Received unexpected response"),
        }
        // Exponential backoff
        thread::sleep(Duration::from_micros(thread_rng().gen_range(0, 2u32.pow(retries)) as u64))
    }
}

fn get(key: KeyType, node_ips: &Vec<String>, key_range: i32) -> (Option<ValueType>, u32) {
    // Returns the result of the put, and the number of retries as a tuple.

    let c = Command::Get(key);
    let server_ip = map_server_ip(key, node_ips, key_range);
    let mut retries = 0;

    // TODO: Keep the socket open between retries (?)
    loop {
        let cr: CommandResponse = write_command(&c, server_ip);
        match cr {
            GetAck(o) => break (o, retries),    // This returns
            NegAck => retries += 1,
            _ => println!("Received unexpected response"),
        }
        // Exponential backoff
        thread::sleep(Duration::from_micros(thread_rng().gen_range(0, 2u32.pow(retries)) as u64))
    }
}

fn write_command(c: &Command, server_ip: &String) -> CommandResponse {
    let mut stream = TcpStream::connect(server_ip)
        .expect("Unable to connect");
    to_writer(&mut stream, &c).expect("Unable to write Command");
    from_reader(&mut stream).expect("Unable to read Response")
}

fn map_server_ip(key: KeyType, node_ips: &Vec<String>, key_range: i32) -> &String {
    // FIXME: This is gross, and relies on the fact that generating key is exclusive of key_range
    node_ips.get((key as f64 / key_range as f64) as usize * node_ips.len() as usize)
        .expect("Could not map key to server")
}
