import os
import socket
import time
import subprocess
import json
import sys
from pathlib import Path
import multiprocessing.reduction

def main(socket_path=os.environ.get('AWS_SOCKET', os.path.expanduser('~/.aws/cli/start_server.sock'))):
    if not os.path.exists(socket_path):
        # spawn the server
        proc = subprocess.Popen([sys.executable, Path(__file__).parent/'command_server.py'])
        # wait up to 1s for server to start
        for i in range(10):
            if os.path.exists(socket_path):
                break
            time.sleep(0.1)
        else:
            # could not spawn/connect to server, run aws directly
            proc.terminate()
            proc.kill()
            args = ['aws', *sys.argv[1:]]
            os.execvp(args[0], args)

    data = [dict(os.environ)] + sys.argv[1:]
    serialized_data = json.dumps(data).encode()

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(socket_path)

        # Send file descriptors for stdin, stdout, and stderr
        fds = [sys.stdin.fileno(), sys.stdout.fileno(), sys.stderr.fileno()]
        multiprocessing.reduction.sendfds(client, fds)

        # Send the serialized data
        client.sendall(serialized_data)
        # done writing but not reading
        client.shutdown(socket.SHUT_WR)

        # Read the exit code from the server
        return int(client.recv(1024).decode() or '1')

if __name__ == "__main__":
    sys.exit(main())
