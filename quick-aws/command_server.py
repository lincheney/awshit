import sys
import copy
import signal
import os
import threading
from functools import partial, cache
import json
import socket
import contextlib
import multiprocessing.reduction
import botocore.session

def awscli_initialize(event_hooks):
    event_hooks.register('building-command-table.main', inject_commands)

def inject_commands(command_table, session, command_object, **kwargs):
    command_table['.start-server'] = partial(start_server, command_object)

@contextlib.contextmanager
def temp_set_attr(var, name, file):
    original = getattr(var, name)
    try:
        setattr(var, name, file)
        yield
    finally:
        setattr(var, name, original)

def work(driver, loader, sock, environ):
    with contextlib.ExitStack() as exit_stack:
        enter_context = exit_stack.enter_context
        sock = enter_context(socket.fromfd(sock, socket.AF_UNIX, socket.SOCK_STREAM))

        # Receive file descriptors for stdin, stdout, and stderr
        fds = multiprocessing.reduction.recvfds(sock, 3)
        stdin = enter_context(os.fdopen(fds[0], 'r'))
        stdout = enter_context(os.fdopen(fds[1], 'w'))
        stderr = enter_context(os.fdopen(fds[2], 'w'))

        data = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
        if not data:
            return

        enter_context(temp_set_attr(sys, 'stdin', stdin))
        enter_context(temp_set_attr(sys, 'stdout', stdout))
        enter_context(temp_set_attr(sys, 'stderr', stderr))

        try:
            args = json.loads(data)
        except json.JSONDecodeError as e:
            # Print error to stderr and return
            print(f"Error: Invalid JSON received - {e}", file=sys.stderr)
            sock.sendall(str(1).encode())
            return

        if not args or not isinstance(args, list):
            print("Error: Invalid arguments received - expected a list", file=sys.stderr)
            sock.sendall(str(1).encode())
            return

        # Extract environment variables from the first element of the list
        env_vars = args.pop(0)
        if not isinstance(env_vars, dict):
            print("Error: Invalid environment variables received - expected a dict", file=sys.stderr)
            sock.sendall(str(1).encode())
            return

        environ = environ.copy()
        environ.update(env_vars)
        enter_context(temp_set_attr(os, 'environ', environ))

        try:
            # Construct a new session and assign it to the session context var
            driver.session = botocore.session.get_session()
            driver.session.register_component('data_loader', loader)
            driver._update_config_chain()
            # Make a copy of the driver
            driver = copy.copy(driver)

            exit_code = driver.main(args)
            # Write the exit code back to the client
            sock.sendall(str(exit_code).encode())
        except:
            sock.sendall(str(1).encode())
            raise

def start_worker(driver, loader, semaphore, queue, environ, fork=True, inactivity_timeout=300):
    if fork and (pid := os.fork()) > 0:
        return pid

    def timeout_handler(signum, frame):
        semaphore.acquire()
        print(f'''DEBUG(craig) \t{'no activity, dying' = }''', file=sys.__stderr__)
        os._exit(0)

    # Set the signal handler and a 5-second alarm
    signal.signal(signal.SIGALRM, timeout_handler)

    while True:
        signal.alarm(inactivity_timeout)
        # get some work
        semaphore.release()
        sock = multiprocessing.reduction.recvfds(queue, 1)[0]
        signal.alarm(0)
        work(driver, loader, sock, environ)
        #  import objgraph
        #  print(f'''DEBUG(mahout)\t{objgraph.show_most_common_types(limit=20) = }''', file=sys.__stderr__)
        #  print(f'''DEBUG(love)  \t{objgraph.by_type('OrderedDict') = }''', file=sys.__stderr__)

def start_server(driver, argv, opts):

    def sigchld_handler(signum, frame):
        wait = os.wait()
        print(f'''DEBUG(sung)  \t{wait = }''', file=sys.__stderr__)

    signal.signal(signal.SIGCHLD, sigchld_handler)

    # Set up the Unix socket server
    socket_path = os.path.expanduser("~/.aws/cli/start_server.sock")
    if os.path.exists(socket_path):
        print(f"Socket already exists at {socket_path}.", file=sys.stderr)
        return

    loader = driver.session.get_component('data_loader')
    class SessionInjection:
        def __getattribute__(self, key):
            return object.__getattribute__(driver.session, key)
    driver.session.__class__ = SessionInjection

    # this hangs in the forked process otherwise
    import awscrt
    awscrt.io.init_logging = lambda *a: None

    pid = os.getpid()
    print(f'''DEBUG(screws)\t{os.getpid() = }''', file=sys.__stderr__)
    environ = os.environ.copy()
    try:
        server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server_socket.bind(socket_path)
        server_socket.listen()

        hub, spoke = socket.socketpair()
        hub.setblocking(True)
        spoke.setblocking(True)
        semaphore = multiprocessing.Semaphore(value=0)

        #  threading.Thread(target=start_worker, args=(driver, semaphore, spoke, False)).start()

        while True:
            client, addr = server_socket.accept()
            if not semaphore.acquire(False):
                start_worker(driver, loader, semaphore, spoke, environ, fork=True)
                semaphore.acquire()
            multiprocessing.reduction.sendfds(hub, [client.fileno()])
    finally:
        server_socket.close()
        if pid == os.getpid() and os.path.exists(socket_path):
            os.unlink(socket_path)
