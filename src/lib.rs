

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

    pub fn barrier(barrier_ips: Vec<String>, listen_port: u16) {
        // Distributed Barrier (My idea)
        // When coming online, go into "listen" mode and send a message to all IP's.
        // Wait for an acknowledgement from everyone (including yourself)
        //  - If everyone acknowledges, send out a messgae and start
        //  - If someone does not acknowledge, just keep listening.

        // Bind the broadcast listener
        let listener = TcpListener::bind(("0.0.0.0", listen_port))
            .expect("Unable to bind listener");
        println!("Listening for barrier on port: {}", listen_port);

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