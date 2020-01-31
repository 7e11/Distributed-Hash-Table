use std::net::TcpListener;
use std::collections::{VecDeque, HashMap};
use std::sync::Mutex;
use serde_json::{from_reader};

use cse403_distributed_hash_table::protocol::Command;


fn main() {

    // Setup Queue (And a mutex around it)
//    let work_queue = Mutex::new(VecDeque::new());

    // setup a basic hash table.    (with a mutex)
//    let hash_table: HashMap<u32, String> = HashMap::new();

    // Setup network
    let listener = TcpListener::bind(("0.0.0.0", 40480))
        .expect("Unable to bind listener");

    for res_stream in listener.incoming() {
        match res_stream {
            Err(e) => eprintln!("Couldn't accept connection: {}", e),
            Ok(stream) => {
                // handle it for real. We're gonna be single threaded for now.
                let c: Command = from_reader(stream)
                    .expect("Could not read / deserialize stream");
                println!("Received: {:?}", c);
            }
        }
    }
}