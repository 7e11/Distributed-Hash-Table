pkill -x server
pkill -x client
cargo run --bin server --release &
cargo run --bin client --release