#!/usr/bin/env python3

import os
import socket
import time
import subprocess
import json
import sys
from pathlib import Path
import multiprocessing.reduction

def find_aws():
    # find an aws that is not this file, in case you are using this as your aws command
    path = os.environ.get('PATH', os.defpath).split(os.pathsep)
    # unique but preserve order
    path = {p: 0 for p in path}
    for p in path:
        file = os.path.join(p, 'aws')
        if os.path.isfile(file) and os.access(file, os.X_OK) and not os.path.samefile(file, __file__):
            return file

def wait_for_server(proc, socket_path):
    # wait up to 1s for server to start
    for i in range(10):
        if os.path.exists(socket_path):
            return True
        if proc.poll() is not None:
            return False
        time.sleep(0.1)

def main(socket_path=os.environ.get('AWS_CLI_SOCKET', os.path.expanduser('~/.aws/cli/command_server.sock'))):
    if not os.path.exists(socket_path):
        # spawn the server
        proc = subprocess.Popen(
            [sys.executable, Path(__file__).readlink().parent/'plugin.py'],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True
        )
        if not wait_for_server(proc, socket_path):
            # could not spawn/connect to server, run aws directly
            proc.terminate()
            # proc.kill()
            args = [find_aws(), *sys.argv[1:]]
            os.execvp(args[0], args)

    data = [dict(os.environ)] + sys.argv[1:]
    serialized_data = json.dumps(data).encode() + b'\n'

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(socket_path)

        # Send file descriptors for stdin, stdout, and stderr
        fds = [sys.stdin.fileno(), sys.stdout.fileno(), sys.stderr.fileno()]
        multiprocessing.reduction.sendfds(client, fds)

        # Send the serialized data
        client.sendall(serialized_data)

        # Read the exit code from the server
        return int(client.recv(1024).decode() or '1')

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
