use std::net::TcpStream;
use std::io::Write;
use serde_json::{to_vec, to_writer, to_writer_pretty};

use cse403_distributed_hash_table::protocol::Command;


fn main() {
    let mut stream = TcpStream::connect(("127.0.0.1", 40480))
        .expect("Unable to connect");

    let c = Command::Set(1, String::from("This is some data!"));

    to_writer(stream,&c)
        .expect("Unable to serialize / write to stream");
}