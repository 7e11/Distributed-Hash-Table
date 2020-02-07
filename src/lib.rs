


pub mod protocol {
    use serde::{Serialize, Deserialize};
    use std::net::{TcpListener, TcpStream, SocketAddr};
    use std::thread;
    use std::time::Duration;
    use serde_json::to_writer;
    use crate::protocol::BarrierCommand::{AllReady, NotAllReady};
    use std::str::FromStr;

    pub type KeyType = i32;         // TODO: Make generic (from config file?)
    pub type ValueType = String;    // TODO: Should be Any (Or equivalent)

    #[derive(Serialize, Deserialize, Debug)]
    pub enum Command {
        Put(KeyType, ValueType),
        Get(KeyType),
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub enum CommandResponse {
        PutAck(bool),
        GetAck(Option<ValueType>),
        NegAck,
    }

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


pub mod pool {
    use std::collections::VecDeque;
    use std::net::TcpStream;
    use std::sync::{Mutex, Condvar, Arc};
    use std::thread::{Thread, JoinHandle};
    use std::thread;

//    pub struct ThreadQueue {
//        // TODO: Make generic
//        work_queue: Arc<(Mutex<VecDeque<TcpStream>>, Condvar)>,
//        // Only accessed at startup. Is it ok to be in a mutex?
//        threads: Vec<JoinHandle<_>>,
//
//    }
    // https://doc.rust-lang.org/1.25.0/book/second-edition/ch20-03-designing-the-interface.html
    // https://doc.rust-lang.org/1.25.0/book/second-edition/ch20-04-storing-threads.html
//    impl ThreadQueue {
//        // Doesn't need &self as their first parameter b/c it's not called on an instance.
//        pub fn new<F>(size: usize, f: F) -> ThreadQueue
//            where F: FnOnce() + Send + 'static {
//            assert!(size > 0);
//            let mut threads = Vec::new();
//            for _ in 0..size {
//                threads.push(thread::spawn(move || f));
//            }
//
//            ThreadQueue {
//                work_queue: Arc::new((Mutex::new(VecDeque::new()), Condvar::new())),
//                threads
//            }
//        }
//
//        fn add_work(&self, ) {
//
//        }
//
//
//        pub fn execute<F>(&self, f: F)
//            where F: FnOnce() + Send + 'static {
//
//        }
//    }
}

pub mod settings {
    use config::{ConfigError, Config};
    use std::path::Path;

    // Shared get_ips method, so both client and server can use it.
    pub fn parse_ips(config: &Config) -> Result<(Vec<String>, Vec<String>), ConfigError> {
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
}