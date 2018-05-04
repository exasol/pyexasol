"""
TCP Server to capture UDF script output.
Unlike HTTP Transport, we have to use real server with proper bindings.

Incoming connections may be blocked by firewalls!
It mostly affects local laptops trying to connect to Exasol located in remote data center.

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
import multiprocessing
import pathlib


class ExaScriptOutput(object):
    def __init__(self, server_host=None, server_port=None):
        self.server_host = str(server_host) if server_host else '0.0.0.0'
        self.server_port = int(server_port) if server_port else 0

        self.server = None
        self.proc = None

    def init_debug_mode(self):
        self.server = ExaScriptOutputServer((self.server_host, self.server_port), ExaScriptOutputDebugModeHandler)
        return self.server.get_output_address()

    def wait_debug_mode(self):
        # Stop server with Ctrl + C
        try:
            self.server.serve_forever()
        except KeyboardInterrupt:
            pass

        self.server.server_close()

    def init_script_mode(self, output_dir):
        output_dir = pathlib.Path(output_dir)

        if not output_dir.is_dir():
            raise ValueError(f"Output_dir does not exist or not a directory: {output_dir}")

        self.proc = ExaScriptOutputProcess(self.server_host, self.server_port, output_dir)
        self.proc.start()

        output_address = self.proc.read_pipe.recv()

        self.proc.write_pipe.close()
        self.proc.read_pipe.close()

        return output_address

    def wait_script_mode(self):
        self.proc.join()

        if self.proc.exitcode != 0:
            raise RuntimeError(f"Output server process was terminated with exitcode: {self.proc.exitcode}")

    def terminate_script_mode(self):
        self.proc.terminate()


class ExaScriptOutputProcess(multiprocessing.Process):
    def __init__(self, server_host, server_port, output_dir):
        self.server_host = server_host
        self.server_port = server_port
        self.output_dir = output_dir

        self.read_pipe, self.write_pipe = multiprocessing.Pipe(False)
        super().__init__()

    def run(self):
        self.read_pipe.close()

        server = ExaScriptOutputServer((self.server_host, self.server_port), ExaScriptOutputScriptModeHandler)
        server.output_dir = self.output_dir

        self.write_pipe.send(server.get_output_address())
        self.write_pipe.close()

        # Stopped inside handler, after closing last connection
        server.serve_forever()
        server.server_close()


class ExaScriptOutputServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    connected_clients = 0
    total_clients = 0

    # Stop all sub-threads immediately
    daemon_threads = True
    allow_reuse_address = True

    def get_output_address(self):
        return f"{socket.getfqdn()}:{self.socket.getsockname()[1]}"


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
            print('\n-------- NEW STATEMENT --------')
            dst = sys.stdout.buffer
        else:
            dst = open(os.devnull, 'wb')

        shutil.copyfileobj(self.rfile, dst)

        if dst == sys.stdout.buffer:
            dst.flush()
        else:
            dst.close()


class ExaScriptOutputScriptModeHandler(ExaScriptOutputHandler):
    def handle(self):
        path = self.server.output_dir / (str(self.server.total_clients).rjust(5, '0') + '.log')
        dst = open(path, 'w+b')

        shutil.copyfileobj(self.rfile, dst)
        dst.close()

    def finish(self):
        super().finish()

        if self.server.connected_clients == 0:
            self.server.shutdown()
