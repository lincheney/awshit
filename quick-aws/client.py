import os
import socket
import json
import sys
import multiprocessing.reduction

def send_to_server(socket_path, env_vars, args):
    """
    Send data to the Unix socket server.

    :param socket_path: Path to the Unix socket.
    :param env_vars: Dictionary of environment variables to send.
    :param args: List of arguments to send.
    """
    data = [env_vars] + args
    serialized_data = json.dumps(data).encode()

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:
        try:
            client_socket.connect(socket_path)

            # Send file descriptors for stdin, stdout, and stderr
            fds = [sys.stdin.fileno(), sys.stdout.fileno(), sys.stderr.fileno()]
            multiprocessing.reduction.sendfds(client_socket, fds)

            # Send the serialized data
            client_socket.sendall(serialized_data)
            client_socket.shutdown(socket.SHUT_WR)

            # Read the exit code from the server
            exit_code = int(client_socket.recv(1024).decode())
            sys.exit(exit_code)
        except FileNotFoundError:
            print(f"Error: Socket not found at {socket_path}.")
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

if __name__ == "__main__":
    # Path to the Unix socket
    socket_path = os.environ.get('AWS_SOCKET', '~/.aws/cli/start_server.sock')
    socket_path = os.path.expanduser(socket_path)

    # Use environment variables from os.environ
    env_vars = dict(os.environ)

    # Take arguments from sys.argv, excluding the script name
    args = sys.argv[1:]

    # Send data to the server
    send_to_server(socket_path, env_vars, args)
