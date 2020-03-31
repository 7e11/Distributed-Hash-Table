pkill -x server
pkill -x client
cargo run --bin server &
cargo run --bin client