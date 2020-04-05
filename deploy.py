# Should build for release
# Copy over the executable to the servers
# Run it and take all of the results, save to a csv where columns are parameters and rows are nodes.
#   I'll need a different approach if I end up having a monitoring thread which takes statistics every 50ms.
#   For those results, I'll need to save a CSV for every statistic which is monitored. Columns are monitoring threads,
#   Rows are time. (Time won't be exactly the same for all of the nodes, as there are no guarentees when the threads
#   wake up.
#   Stuff to monitor in real time: #messages which have been processed since the last wakeup (throughput).


# Perhaps there could also be a version which runs it with different combinations of key ranges and num ops.
from typing import List, Dict

from fabric import Connection, ThreadingGroup as Group
from pathlib import Path, PurePath, PurePosixPath
from threading import Thread
from fabric.config import Config
import os
from plot import plot_time_series_cum
from tempfile import TemporaryFile
import json

# user = 'evan'
user = 'ec2-user'

# nodes = [
#     '40.117.213.2:22',
#     # '40.117.212.129:22',
#     # '40.117.214.137:22',
# ]

# Changes on every restart
nodes = [
    '52.14.247.51',
    '3.15.40.83',
    '18.220.208.203',
    '52.15.189.99',
    '18.191.158.65',
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

def configure_group(hosts):
    connections = []
    for host in hosts:
        connections.append(
            Connection(host=host,
                       user=user,
                       connect_kwargs={'key_filename': 'C:\\Users\\Evan.Evan-Desktop\\.ssh\\dht-aws-key.pem'}))
    group = Group.from_connections(connections)
    return group

def deploy(group):
    # Put isn't supported on groups yet, do it on each individual connection
    # https://github.com/fabric/fabric/issues/1810
    # Make sure the required directories exist in each server.
    group.run('mkdir -p /home/' + user + '/dht/src/client')
    group.run('mkdir -p /home/' + user + '/dht/src/server')
    for connection in group:
        # Can't transfer entire directories, transfer one file at a time.
        for path in transfer_files:
            connection.put(path, '/home/' + user + '/dht/' + path.as_posix())

def install(group):
    group.run('sudo yum -y groupinstall \"Development Tools\"')     # ec2 doesn't have apt
    group.run('curl --proto \'=https\' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y')

def update(group):
    group.run('/home/' + user + '/.cargo/bin/rustup update')

def kill(group):
    # pkill exit(1) if it tries to kill something not running.
    # That will crash fabric, so the "|| true" will make sure the command always succeeds (?)
    group.run("pkill -x server || true")
    group.run("pkill -x client || true")

def run_client(group):
    # Need to chain cd
    # https://github.com/fabric/fabric/issues/1862
    # This will need to be asynchronous in the future, when we start taking statistics on the server.
    group.run('cd dht && /home/' + user + '/.cargo/bin/cargo run --bin client --release')

def run_server(group, sockname='dtach'):
    cmd = '/home/' + user + '/.cargo/bin/cargo run --bin server --release'
    # cmd = '/home/evan/.cargo/bin/cargo run --bin server --release --manifest-path /home/evan/dht/Cargo.toml'
    for conn in group:
        conn.run('cd /home/' + user + '/dht && dtach -n `mktemp -u /tmp/%s.XXXX` %s' % (sockname, cmd))
        # background_run(conn, cmd)

def collect_results(group: Group) -> List[dict]:
    results: List[dict] = []
    for conn in group:
        with TemporaryFile() as file:
            conn.get(remote='/home/' + user + '/dht/time_series_data.json', local=file)
            file.seek(0)
            str_response = file.read().decode('utf-8')
            results.extend(json.loads(str_response))
    return results

if __name__ == '__main__':
    # One thread for managing the client ThreadedGroup, and another for the server ThreadedGroup
    # Have the server group be thread managed.
    # This doesn't parallelize, only allows for concurrency.

    client_group = configure_group(nodes)
    # install(client_group)
    kill(client_group)
    deploy(client_group)

    def server_operations():
        server_group = configure_group(nodes)
        # kill(server_group)
        # deploy(server_group)
        server_group.run('cd /home/' + user + '/dht && /home/' + user + '/.cargo/bin/cargo run --bin server')
    server_group_thread = Thread(target=server_operations)
    server_group_thread.start()

    client_group.run('cd /home/' + user + '/dht && /home/' + user + '/.cargo/bin/cargo run --bin client')
    server_group_thread.join()

    print('Collecting Results')

    # Retrieve the JSON files from each of the servers. Can be done from the client group
    results: List[dict] = collect_results(client_group)
    # Append these results to our data store

    with open('results_all.json', 'r') as json_file:
        data_all = json.load(json_file)

    data_all.extend(results)

    with open('results_all.json', 'w') as json_file:
        json.dump(data_all, json_file)
        # json.dump(results, json_file)

    # print(results)
    # Then run a plotting program.
    # plot_time_series_cum(results)
