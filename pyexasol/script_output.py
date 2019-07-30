"""
TCP Server to capture UDF script output.
Unlike HTTP Transport, we have to use real server with proper bindings.

Incoming connections may be blocked by firewalls!
It mostly affects local laptops trying to connect to Exasol located in remote data centre.

This module can work in two modes:

    1) DEBUG MODE
    Useful for manual debugging during UDF script development.
    Accepts connections from all VM's, but displays output of first connected VM only.
    Runs forever, until stopped by Ctrl + C (SIGTERM).

    How to run: python -m pyexasol script_debug


    2) SCRIPT MODE
    Useful for production usage and during last stages of development.
    Accepts connections from all VM's and stores output into separate log files.
    Runs for one SQL statement only, stops automatically.

    How to run: ExaConnection.execute_with_udf_output()


We use ThreadingMixIn because:
a) Workload is pure I/O, so GIL should not be a problem.
b) Potential amount of VM's connected in parallel is huge and may surpass 1000+, which may cause problems with forks.

"""
import socket
import socketserver
import sys
import os
import shutil
import subprocess
import pathlib

from . import utils


class ExaScriptOutputProcess(object):
    def __init__(self, host, port, output_dir=None, initial_ppid=None):
        self.host = host
        self.port = port

        self.output_dir = output_dir
        self.initial_ppid = initial_ppid

        self.server = None
        self.output_address = None

        self.proc = None

    def start(self):
        args = [sys.executable,
                '-m', 'pyexasol', 'script_output',
                '--output-dir', str(self.output_dir),
                '--ppid', str(utils.get_pid())
                ]

        if self.host:
            args.append('--host')
            args.append(self.host)

        if self.port:
            args.append('--port')
            args.append(str(self.port))

        self.proc = subprocess.Popen(args, stdout=subprocess.PIPE)
        self.output_address = self.proc.stdout.readline().decode().rstrip('\n')

        self.proc.stdout.close()

    def init_server_script_mode(self):
        output_dir = pathlib.Path(self.output_dir)

        if not output_dir.is_dir():
            raise ValueError(f"Output_dir does not exist or not a directory: {output_dir}")

        self.server = ExaScriptOutputServer((self.host, self.port), ExaScriptOutputScriptModeHandler)
        self.server.output_dir = output_dir
        self.server.initial_ppid = self.initial_ppid

    def handle_requests_script_mode(self):
        # Server is stopped by shutdown() call in handler after closing last connection
        self.server.serve_forever()
        self.server.server_close()

    def init_server_debug_mode(self):
        self.server = ExaScriptOutputServer((self.host, self.port), ExaScriptOutputDebugModeHandler)
        self.output_address = self.server.get_output_address()

    def handle_requests_debug_mode(self):
        # Stop server with SIGTERM (Ctrl + C, etc.)
        try:
            self.server.serve_forever()
        except KeyboardInterrupt:
            pass

        self.server.server_close()

    def send_output_address(self):
        sys.stdout.buffer.write(f'{self.server.get_output_address()}\n'.encode())
        sys.stdout.buffer.flush()

    def get_output_address(self):
        if self.output_address is None:
            raise RuntimeError("Script output address 'host:port' is not available")

        return self.output_address

    def join(self):
        code = self.proc.wait()

        if code != 0:
            raise RuntimeError(f"Script output server process finished with exitcode: {code}")

    def terminate(self):
        if self.proc:
            self.proc.terminate()


class ExaScriptOutputServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    connected_clients = 0
    total_clients = 0

    # Stop all sub-threads immediately
    daemon_threads = True
    allow_reuse_address = True

    output_dir = None
    initial_ppid = None

    def get_output_address(self):
        return f"{socket.gethostbyname(socket.getfqdn())}:{self.socket.getsockname()[1]}"

    def service_actions(self):
        utils.check_orphaned(self.initial_ppid)


class ExaScriptOutputHandler(socketserver.StreamRequestHandler):
    def setup(self):
        super().setup()
        self.server.connected_clients += 1
        self.server.total_clients += 1

    def finish(self):
        super().finish()
        self.server.connected_clients -= 1


class ExaScriptOutputDebugModeHandler(ExaScriptOutputHandler):
    def handle(self):
        if self.server.connected_clients == 1:
            print('\n-------- NEW STATEMENT --------', flush=True)

            # Read and flush line-by-line, show log to user as soon as possible
            for line in self.rfile:
                sys.stdout.buffer.write(line)
                sys.stdout.buffer.flush()
        else:
            dst = open(os.devnull, 'wb')
            shutil.copyfileobj(self.rfile, dst)
            dst.close()


class ExaScriptOutputScriptModeHandler(ExaScriptOutputHandler):
    def handle(self):
        path = self.server.output_dir / (str(self.server.total_clients).rjust(5, '0') + '.log')
        dst = open(path, 'wb')

        shutil.copyfileobj(self.rfile, dst)
        dst.close()

    def finish(self):
        super().finish()

        # No more opened connections? -> Shutdown server
        if self.server.connected_clients == 0:
            self.server.shutdown()
