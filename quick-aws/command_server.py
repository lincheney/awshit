#!/usr/bin/env python3

import sys
import copy
import signal
import os
import traceback
from functools import partial
import json
import socket
import contextlib
import multiprocessing.reduction
import botocore.session

def awscli_initialize(event_hooks):
    event_hooks.register('building-command-table.main', inject_commands)

def inject_commands(command_table, session, command_object, **kwargs):
    command_table['.start-command-server'] = partial(start_server, command_object)

@contextlib.contextmanager
def temp_set_attr(var, name, file):
    original = getattr(var, name)
    try:
        setattr(var, name, file)
        yield
    finally:
        setattr(var, name, original)

@contextlib.contextmanager
def timeouter(timeout, condition=None):
    def timeout_handler(signum, frame):
        if condition is None or condition():
            raise TimeoutError(timeout)
        else:
            # restart timer
            signal.alarm(timeout)
    try:
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)
        yield
    finally:
        signal.alarm(0)

class WorkerState:
    def __init__(self, sock, driver, loader, environ, semaphore):
        self.sock = sock
        self.driver = driver
        self.loader = loader
        self.environ = environ
        self.semaphore = semaphore
        self.fd_cache = {}

    @contextlib.contextmanager
    def temp_dup_fd(self, src, dest):
        if dest not in self.fd_cache:
            self.fd_cache[dest] = os.dup(dest)

        try:
            os.dup2(src, dest)
            yield
        finally:
            os.dup2(self.fd_cache[dest], dest)

    def work(self, sock):
        with contextlib.ExitStack() as exit_stack:
            enter_context = exit_stack.enter_context
            sock = enter_context(socket.fromfd(sock, socket.AF_UNIX, socket.SOCK_STREAM))
            exit_code = 1
            try:
                exit_code = self._work(sock, enter_context) or 0
            except Exception:
                traceback.print_exc()
            finally:
                # Write the exit code back to the client
                sock.sendall(str(exit_code).encode())

    def _work(self, sock, enter_context):
        # Receive file descriptors for stdin, stdout, and stderr
        fds = multiprocessing.reduction.recvfds(sock, 3)
        stdin = enter_context(os.fdopen(fds[0], 'r'))
        stdout = enter_context(os.fdopen(fds[1], 'w'))
        stderr = enter_context(os.fdopen(fds[2], 'w'))

        # read everything
        data = b""
        while chunk := sock.recv(4096):
            data += chunk

        enter_context(self.temp_dup_fd(fds[0], 0))
        enter_context(self.temp_dup_fd(fds[1], 1))
        enter_context(self.temp_dup_fd(fds[2], 2))

        args = json.loads(data)
        assert isinstance(args, list) and len(args) > 1, 'arguments is not a non-empty list'

        # custom commands
        if args[1] == '/reload':
            os.kill(os.getppid(), signal.SIGUSR1)
            return

        # Extract environment variables from the first element of the list
        env_vars = args.pop(0)
        assert isinstance(env_vars, dict), 'first argument should be a dict of env vars'
        environ = self.environ.copy()
        environ.update(env_vars)
        enter_context(temp_set_attr(os, 'environ', environ))

        # Construct a new session
        self.driver.session = botocore.session.get_session()
        # reuse the same loader
        self.driver.session.register_component('data_loader', self.loader)
        self.driver._update_config_chain()
        # Make a copy of the driver
        driver = copy.copy(self.driver)
        # always rebuild aliases
        driver.alias_loader._aliases = None
        return driver.main(args)

    def fork(self, *args, **kwargs):
        if (pid := os.fork()) > 0:
            return pid
        self.run(*args, **kwargs)

    def run(self, inactivity_timeout=300):
        while True:
            # available
            self.semaphore.release()
            try:
                with timeouter(inactivity_timeout):
                    # get the client socket
                    sockfd = multiprocessing.reduction.recvfds(self.sock, 1)[0]
            except TimeoutError:
                return
            finally:
                # unavailable
                self.semaphore.acquire(False)
            # do the work
            self.work(sockfd)

class State:
    def __init__(self, *args, **kwargs):
        self.semaphore = multiprocessing.Semaphore(value=0)
        self.pid = os.getpid()
        # make socket that workers use
        recv, send = socket.socketpair()
        recv.setblocking(True)
        send.setblocking(True)
        self.sock = send

        self.worker = WorkerState(*args, sock=recv, semaphore=self.semaphore, **kwargs)
        self.worker_pids = set()

    def queue_work(self, sock, **kwargs):
        # check if any worker available
        if self.semaphore.acquire(False):
            # reset it
            self.semaphore.release()
        else:
            # no workers available, start one
            pid = self.worker.fork(**kwargs)
            if pid is None:
                return
            self.worker_pids.add(pid)
        multiprocessing.reduction.sendfds(self.sock, [sock.fileno()])

    def run(self, socket_path, inactivity_timeout=300):
        if os.path.exists(socket_path):
            raise FileExistsError(socket_path)

        # reap and keep track of workers
        def sigchld_handler(signum, frame):
            pid, status = os.wait()
            if pid in self.worker_pids:
                self.worker_pids.remove(pid)
        signal.signal(signal.SIGCHLD, sigchld_handler)

        # Set up the Unix socket server
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
                server.bind(socket_path)
                server.listen()

                def sigusr1_handler(signum, frame):
                    # reexec
                    self.cleanup(socket_path)
                    self.sock.close()
                    server.close()
                    args = sys.argv
                    os.execvp(args[0], args)
                signal.signal(signal.SIGUSR1, sigusr1_handler)

                while True:
                    try:
                        with timeouter(inactivity_timeout, condition=lambda: not self.worker_pids):
                            client, addr = server.accept()
                    except TimeoutError:
                        return
                    else:
                        self.queue_work(client, inactivity_timeout=inactivity_timeout)
        finally:
            self.cleanup(socket_path)

    def cleanup(self, socket_path):
        if self.pid == os.getpid() and os.path.exists(socket_path):
            os.unlink(socket_path)

def start_server(driver, argv, opts=None):
    state = State(
        driver=driver,
        loader=driver.session.get_component('data_loader'),
        environ=os.environ.copy()
    )

    # awscrt starts a thread for logging, but it won't work post fork
    # so we just disable it
    import awscrt
    awscrt.io.init_logging = lambda *a: None

    class SessionInjection:
        def __getattribute__(self, key):
            return object.__getattribute__(driver.session, key)
    driver.session.__class__ = SessionInjection

    socket_path = os.environ.get('AWS_SOCKET', os.path.expanduser('~/.aws/cli/start_server.sock'))
    state.run(socket_path)

def main():
    try:
        import awscli.clidriver
    except ImportError:
        # can't import directly, hopefully you have installed as a plugin
        args = ['aws', '.start-command-server']
        os.execvp(args[0], args)
    else:
        start_server(awscli.clidriver.CLIDriver(), sys.argv[1:])

if __name__ == '__main__':
    sys.exit(main())
