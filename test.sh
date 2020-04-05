pkill -x server
pkill -x client
cargo run --bin server &
cargo run --bin client


#Flamegraph commands:
#pkill -x client
#pkill -x server
## Very important you don't build for release so you get function names.
## However, this won't have compiler optimizations.
#cargo build --bin client
#cargo build --bin server
#
#./target/debug/client &
#sudo perf record --call-graph dwarf ./target/debug/server
## OR
#./target/debug/server &
#sudo perf record --call-graph dwarf ./target/debug/client
#
#sudo perf script | ~/FlameGraph/stackcollapse-perf.pl | ~/FlameGraph/stackcollapse-recursive.pl | c++filt | ~/FlameGraph/flamegraph.pl > client_flame_new.svg