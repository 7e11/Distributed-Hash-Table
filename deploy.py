# Should build for release
# Copy over the executable to the servers
# Run it and take all of the results, save to a csv where columns are parameters and rows are nodes.
#   I'll need a different approach if I end up having a monitoring thread which takes statistics every 50ms.
#   For those results, I'll need to save a CSV for every statistic which is monitored. Columns are monitoring threads,
#   Rows are time. (Time won't be exactly the same for all of the nodes, as there are no guarentees when the threads
#   wake up.
#   Stuff to monitor in real time: #messages which have been processed since the last wakeup (throughput).


# Perhaps there could also be a version which runs it with different combinations of key ranges and num ops.
from fabric import Connection, ThreadingGroup as Group
from pathlib import Path, PurePath, PurePosixPath
from threading import Thread
import os

# FIXME: Temporary thing while I'm just working on one node.
nodes = [
    'evan@40.117.213.2:22',
    # 'evan@40.117.212.129:22',
    # 'evan@40.117.214.137:22',
]

transfer_files = [
    Path('src\\client\\client.rs'),
    Path('src\\server\\server.rs'),
    Path('src\\lib.rs'),
    # Path('Cargo.lock'),
    Path('Cargo.toml'),
    Path('settings.yaml'),
    # Path('test.sh'),
]

def deploy(group):
    # Put isn't supported on groups yet, do it on each individual connection
    # https://github.com/fabric/fabric/issues/1810
    for connection in group:
        connection.cd
        # Can't transfer entire directories, transfer one file at a time.
        for path in transfer_files:
            connection.put(path, '/home/evan/dht/' + path.as_posix())

def update(group):
    group.run("/home/evan/.cargo/bin/rustup update")

def kill(group):
    group.run("pkill -x server")
    group.run("pkill -x client")

def run_client(group):
    # Need to chain cd
    # https://github.com/fabric/fabric/issues/1862
    # This will need to be asynchronous in the future, when we start taking statistics on the server.
    group.run("cd dht && /home/evan/.cargo/bin/cargo run --bin client --release")

def run_server(group, sockname='dtach'):
    cmd = '/home/evan/.cargo/bin/cargo run --bin server --release'
    # cmd = '/home/evan/.cargo/bin/cargo run --bin server --release --manifest-path /home/evan/dht/Cargo.toml'
    for conn in group:
        conn.run('cd /home/evan/dht && dtach -n `mktemp -u /tmp/%s.XXXX` %s' % (sockname, cmd))
        # background_run(conn, cmd)

def background_run(connection, command):
    command = 'nohup %s &> /dev/null &' % command
    connection.run(command, pty=False)

if __name__ == '__main__':
    # One thread for managing the client ThreadedGroup, and another for the server ThreadedGroup
    # Have the server group be thread managed.
    # This doesn't parallelize, only allows for concurrency.
    def server_operations():
        server_group = Group(*nodes)
        # kill(server_group)
        deploy(server_group)
        server_group.run('cd /home/evan/dht && /home/evan/.cargo/bin/cargo run --bin server --release')
    server_group_thread = Thread(target=server_operations)
    # server_group_thread.start()

    client_group = Group(*nodes)
    # kill(client_group)
    deploy(client_group)
    # client_group.run("cd /home/evan/dht && /home/evan/.cargo/bin/cargo run --bin client --release")

    # server_group_thread.join()
