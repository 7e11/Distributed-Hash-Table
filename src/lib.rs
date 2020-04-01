pub mod barrier {
    use serde::{Serialize, Deserialize};
    use std::net::{TcpListener, TcpStream, SocketAddr};
    use std::thread;
    use std::time::Duration;
    use serde_json::to_writer;
    use crate::barrier::BarrierCommand::{AllReady, NotAllReady};
    use std::str::FromStr;

    #[derive(Serialize, Deserialize, Debug)]
    pub enum BarrierCommand {
        AllReady,
        NotAllReady,
    }

    pub fn barrier(barrier_ips: Vec<String>, listener: &TcpListener) {
        // Distributed Barrier (My idea)
        // When coming online, go into "listen" mode and send a message to all IP's.
        // Wait for an acknowledgement from everyone (including yourself)
        //  - If everyone acknowledges, send out a messgae and start
        //  - If someone does not acknowledge, just keep listening.

        // Bind the broadcast listener
        println!("Listening for barrier on {:?}", listener);

        // Spawn the broadcast thread.
        let handle = thread::spawn(move || {
            barrier_broadcast(barrier_ips);
        });

        for res_stream in listener.incoming() {
            match res_stream {
                Err(e) => eprintln!("Couldn't accept barrier connection: {}", e),
                Ok(stream) => {
                    let mut de = serde_json::Deserializer::from_reader(stream);
                    let bc = BarrierCommand::deserialize(&mut de)
                        .expect("Could not deserialize barrier command.");
                    match bc {
                        AllReady => break,
                        NotAllReady => (),
                    }
                },
            }
        }
        handle.join().expect("Failed to join on broadcast thread");
    }

    fn barrier_broadcast(node_ips: Vec<String>) {
        // Attempt to open a TCP connection with everyone.
        // east, west, central
        let mut open_streams = Vec::new();

        // This expends the node_ips iterator and takes ownership of the ips.
        // See: http://xion.io/post/code/rust-for-loop.html
        for ip in node_ips {
            let stream_res = TcpStream::connect_timeout(
                &SocketAddr::from_str(ip.as_str()).expect("Could not parse IP"),
                Duration::from_secs(5));
            match stream_res {
                Ok(stream) => open_streams.push(stream),
                Err(e) => {
                    println!("Failed broadcast to {} {}", ip, e);
                    // write NotAllReady to the ones that succeeded and return
                    for stream in open_streams {
                        // This will consume open_streams.
                        to_writer(stream, &NotAllReady).expect("Failed to send NotAllReady");
                    }
                    return
                },
            }
        }
        println!("Broadcast successful");

        // If everything worked, then write AllReady to all of them. This also consumes open_streams
        for stream in open_streams {
            to_writer(stream, &AllReady).expect("Failed to send AllReady");
        }
    }
}

pub mod parallel {
    use std::sync::{RwLock, Mutex};
    use crate::parallel::LockCheck::{LockFail, Type};
    use crate::transport::{KeyType, ValueType};

    // TODO: Make generic for real...
    pub struct ConcurrentHashTable {
        buckets: RwLock<Vec<Mutex<Vec<(KeyType, ValueType)>>>>,
        num_buckets: usize,
        key_range: u32,
        num_servers: usize,
    }

    pub enum LockCheck<T> {
        LockFail,
        Type(T),
    }

    impl ConcurrentHashTable {
        pub fn new(num_buckets: usize, key_range: u32, num_servers: usize) -> ConcurrentHashTable {
            assert!(num_buckets > 0 && key_range > 0 && num_servers > 0);

            // Abusing RW locks (kind of) in order to avoid unsafe code.
            let buckets: RwLock<Vec<Mutex<Vec<(KeyType, ValueType)>>>>
                = RwLock::new(Vec::with_capacity(num_buckets));
            let mut buckets_lock = buckets.write().unwrap();
            for _ in 0..num_buckets {
                buckets_lock.push(Mutex::new(Vec::new()));
            }
            drop(buckets_lock);
            ConcurrentHashTable {
                buckets,
                num_buckets,
                key_range,
                num_servers
            }
        }

        // DIFFERENCES:
        // This clones the value type. Convenient for later.
        pub fn get(&self, key: &KeyType) -> LockCheck<Option<ValueType>> {
            let buckets = self.buckets.read().unwrap();
            let bucket_lock = buckets.get(self.compute_bucket(key)).unwrap();
            // TRY LOCK !
            let bucket_lock = bucket_lock.try_lock();
            match bucket_lock {
                Err(_) => LockFail,
                Ok(bucket) => {
                    let res = bucket.iter().find_map(|(k, v)| {
                        if k == key {
                            Some(v.clone())
                        } else {
                            None
                        }
                    });
                    Type(res)
                },
            }
        }


        // DOES update keys to new values
        pub fn insert(&self, key: KeyType, value: ValueType) -> LockCheck<Option<ValueType>> {
            let buckets = self.buckets.read().unwrap();
            let bucket_lock = buckets.get(self.compute_bucket(&key)).unwrap();
            // TRY LOCK !
            let bucket_lock = bucket_lock.try_lock();
            match bucket_lock {
                Err(_) => LockFail,
                // FIXME: Does this work???
                Ok(mut bucket) => {
                    let index = bucket.iter().position(|(k, _)| *k == key);
                    if let Some(index) = index {
                        // The key already existed, update it.
                        let pair = bucket.get_mut(index).unwrap();
                        // FIXME: This performance is gonna suck. Try doing something else.
                        // Look into entry and or_insert.
                        let prev_value = pair.1.clone();
                        pair.1 = value;
                        Type(Some(prev_value))
                    } else {
                        // The key has not existed, append it.
                        bucket.push((key, value));
                        Type(None)
                    }
                },
            }
        }

        pub fn contains_key(&self, key: &KeyType) -> LockCheck<bool> {
            let buckets = self.buckets.read().unwrap();
            let bucket_lock = buckets.get(self.compute_bucket(key)).unwrap();
            // TRY LOCK !
            let bucket_lock = bucket_lock.try_lock();
            match bucket_lock {
                Err(_) => LockFail,
                Ok(bucket) => {
                    let res = bucket.iter().any(|(k, _)| k == key);
                    Type(res)
                },
            }
        }

        pub fn insert_if_absent(&self, key: KeyType, value: ValueType) -> LockCheck<bool> {
            // true means inserted, false means not inserted (there was something already there)
            let buckets = self.buckets.read().unwrap();
            let bucket_lock = buckets.get(self.compute_bucket(&key)).unwrap();
            // TRY LOCK !
            let bucket_lock = bucket_lock.try_lock();
            match bucket_lock {
                Err(_) => LockFail,
                Ok(mut bucket) => {
                    let res = bucket.iter().any(|(k, _)| *k == key);
                    if res {
                        // The key exists already
                        Type(false)
                    } else {
                        // The key does not exist
                        bucket.push((key, value));
                        Type(true)
                    }
                },
            }
        }

        fn compute_bucket(&self, key: &KeyType) -> usize {
            // We consistent hashing gives us contiguous ranges like [0, 1, 2, 3]
            *key as usize % self.num_buckets
        }
    }
}

pub mod settings {
    use config::{ConfigError, Config};
    use std::path::Path;

    // Shared get_ips method, so both client and server can use it.
    fn parse_ips(config: &Config) -> Result<(Vec<String>, Vec<String>), ConfigError> {
        let client_ips = config.get_array("client_ips")?;
        let server_ips = config.get_array("server_ips")?;

        // Now convert them to strings
        let client_ips = client_ips.into_iter()
            .map(|s| s.into_str().expect("Could not parse IP into str"))
            .collect();
        let server_ips = server_ips.into_iter()
            .map(|s| s.into_str().expect("Could not parse IP into str"))
            .collect();

        Ok((client_ips, server_ips))
    }

    pub fn parse_settings() -> Result<(Vec<String>, Vec<String>, u32, u32), ConfigError> {
        // Returns client_ips, server_ips, num_ops, key_range all as a tuple.

        let mut config = config::Config::default();
        // Add in server_settings.yaml
        config.merge(config::File::from(Path::new("./settings.yaml")))?;
//    println!("{:#?}", config);
        let (client_ips, server_ips) = parse_ips(&config)?;

        // Gather the other settings
        let num_ops = config.get_int("num_ops")?;
        let key_range = config.get_int("key_range")?;

        Ok((client_ips, server_ips, num_ops as u32, key_range as u32))
    }
}

pub mod transport {
    use serde::{Serialize, Deserialize};
    use std::net::TcpStream;

    // This entire class is only really client side.
    // Is there anything I can do server side ?
    // I'll need to handle a connection pool there also.

    pub type KeyType = u32;
    pub type ValueType = String;    // TODO: Should be Any (Or equivalent)

    #[derive(Serialize, Deserialize, Debug)]
    pub enum Command {
        Put(KeyType, ValueType),
        Get(KeyType),
        Exit,
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub enum CommandResponse {
        PutAck(bool),
        GetAck(Option<ValueType>),
        NegAck,
    }

    ///Taken from here: https://docs.rs/bincode/1.2.1/src/bincode/lib.rs.html#85
    /// Will need to make this return the writer back to the caller.
    pub fn buffered_serialize_into< W, T: ?Sized>(mut writer: W, value: &T) -> std::io::Result<usize>
    where
        W: std::io::Write,
        T: serde::Serialize,
    {
        let buffer = bincode::serialize(value).unwrap();
        writer.write(buffer.as_slice())
    }

    // ///signature taken from:
    // /// deserialize_from: https://docs.rs/bincode/1.2.1/src/bincode/lib.rs.html#104
    // /// deserialize: https://docs.rs/bincode/1.2.1/src/bincode/lib.rs.html#138
    // pub fn buffered_deserialize_from<'a, R, T>(mut reader: R) -> std::result::Result<T, bincode::Error>
    //     where
    //         R: std::io::Read,
    //         T: serde::de::Deserialize<'a>,
    // {
    //     let mut buffer: [u8; 256] = [0; 256];
    //     reader.read(&mut buffer[..]);   // mutable slice
    //     bincode::deserialize(&buffer[..])
    // }

    // For client:
    // let mut buffer: [u8; 256] = [0; 256];
    // stream_ref.read(&mut buffer[..]).unwrap();
    // let cr = bincode::deserialize(&buffer[..]).unwrap();

    // For server:
    // let mut buffer: [u8; 256] = [0; 256];
    // stream.read(&mut buffer[..]).unwrap();
    // let c = bincode::deserialize(&buffer[..]).unwrap();

}