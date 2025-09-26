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
def temp_set_environ(environ):
    original = os.environ.copy()
    try:
        os.environ.clear()
        os.environ.update(environ)
        yield
    finally:
        os.environ.clear()
        os.environ.update(original)

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

def waitpids():
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                break
            yield pid
        except ChildProcessError:
            break

class WorkerState:
    def __init__(self, sock, driver, components, semaphore):
        self.sock = sock
        self.driver = driver
        self.components = components
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
            sock = exit_stack.enter_context(socket.fromfd(sock, socket.AF_UNIX, socket.SOCK_STREAM))
            exit_code = 1
            try:
                exit_code = self._work(sock, exit_stack.enter_context) or 0
            except SystemExit as e:
                exit_code = e.code
            except BaseException:
                traceback.print_exc()
            finally:
                # Write the exit code back to the client
                sock.sendall(str(exit_code).encode())

    def _work(self, sock, enter_context):
        import botocore.session

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
        assert isinstance(args, list) and len(args) >= 1, 'arguments is not a non-empty list'

        # custom commands
        if len(args) > 1 and args[1] == '/reload':
            os.kill(os.getppid(), signal.SIGUSR1)
            return

        # Extract environment variables from the first element of the list
        env_vars = args.pop(0)
        assert isinstance(env_vars, dict), 'first argument should be a dict of env vars'
        enter_context(temp_set_environ(env_vars))

        # Construct a new session
        self.driver.session = botocore.session.get_session()
        # reuse the same loader etc
        for k, v in self.components.items():
            self.driver.session.register_component(k, v)
        self.driver._update_config_chain()
        # Make a copy of the driver
        driver = copy.copy(self.driver)
        # always rebuild aliases
        driver.alias_loader._aliases = None
        return driver.main(args)

    def run(self, inactivity_timeout=300):
        while True:
            # available
            self.semaphore.release()
            try:
                with timeouter(inactivity_timeout):
                    # get the client socket
                    sockfd = multiprocessing.reduction.recvfds(self.sock, 1)[0]
            except (TimeoutError, EOFError):
                return
            finally:
                # unavailable
                self.semaphore.acquire(False)
            # do the work
            self.work(sockfd)

class State:
    def __init__(self, *args, **kwargs):
        self.exit_stack = contextlib.ExitStack()
        self.semaphore = multiprocessing.Semaphore(value=0)
        self.pid = os.getpid()
        # make socket that workers use
        recv, send = socket.socketpair()
        recv.setblocking(True)
        send.setblocking(True)
        self.sock = send
        self.exit_stack.enter_context(self.sock)

        self.worker = WorkerState(*args, sock=recv, semaphore=self.semaphore, **kwargs)
        self.worker_pids = set()

    def queue_work(self, server, sock, **kwargs):
        # check if any worker available
        if self.semaphore.acquire(False):
            # reset it
            self.semaphore.release()
        # no workers available, start one
        elif (pid := os.fork()) > 0:
            self.worker_pids.add(pid)
        else:
            self.exit_stack.close()
            self.worker.run(**kwargs)
            return

        multiprocessing.reduction.sendfds(self.sock, [sock.fileno()])
        return True

    def run(self, socket_path, inactivity_timeout=300):
        if os.path.exists(socket_path):
            raise FileExistsError(socket_path)

        # reap and keep track of workers
        def sigchld_handler(signum, frame):
            for pid in waitpids():
                self.worker_pids -= {pid}
        signal.signal(signal.SIGCHLD, sigchld_handler)
        # make sigterm trigger context managers
        signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))

        def sigusr1_handler(signum, frame):
            # reexec
            self.exit_stack.close()
            os.execvp(sys.argv[0], sys.argv)
        signal.signal(signal.SIGUSR1, sigusr1_handler)

        # Set up the Unix socket server
        with self.exit_stack:
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.exit_stack.enter_context(server)
            self.exit_stack.callback(self.cleanup, socket_path)
            server.bind(socket_path)
            server.listen()

            while True:
                try:
                    with timeouter(inactivity_timeout, condition=lambda: not self.worker_pids):
                        client, addr = server.accept()
                except TimeoutError:
                    return
                else:
                    if not self.queue_work(server, client, inactivity_timeout=inactivity_timeout):
                        return

    def cleanup(self, socket_path):
        if self.pid == os.getpid() and os.path.exists(socket_path):
            os.unlink(socket_path)

def start_server(driver, argv, opts=None):
    # slurp up any zombie children
    list(waitpids())

    state = State(
        driver=driver,
        components={k: driver.session.get_component(k) for k in {'data_loader', 'event_emitter', 'response_parser_factory'}},
    )

    # awscrt starts a thread for logging, but it won't work post fork
    # so we just disable it
    import awscrt
    awscrt.io.init_logging = lambda *a: None

    class SessionInjection:
        def __getattribute__(self, key):
            return object.__getattribute__(driver.session, key)
    driver.session.__class__ = SessionInjection

    socket_path = os.environ.get('AWS_CLI_SOCKET', os.path.expanduser('~/.aws/cli/command_server.sock'))
    state.run(socket_path)

def main():
    try:
        import awscli.clidriver
    except ImportError:
        # can't import directly, hopefully you have installed as a plugin
        import client
        args = [client.find_aws(), '.start-command-server']
        os.execvp(args[0], args)
    else:
        start_server(awscli.clidriver.CLIDriver(), sys.argv[1:])

if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
