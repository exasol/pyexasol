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

    How to run: python -m pyexasol_utils.script_output


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
import pathlib


class ExaScriptOutputServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    connected_clients = 0
    total_clients = 0

    # Stop all sub-threads immediately
    daemon_threads = True
    allow_reuse_address = True

    output_dir_path = None
    initial_ppid = None

    def get_output_address(self):
        return f"{socket.gethostbyname(socket.getfqdn())}:{self.socket.getsockname()[1]}"

    def service_actions(self):
        self.check_orphaned(self.initial_ppid)

    @staticmethod
    def check_orphaned(initial_ppid):
        """
        Raise exception if current process is "orphaned" (parent process is dead)
        It is useful to stop PyEXASOL socket servers from being stuck in process list after parent process was killed

        Currently it works only for POSIX OS
        Please let me know if you know a good way to detect orphans on Windows
        """
        current_ppid = os.getppid()

        if sys.platform != "win32" and initial_ppid and current_ppid != initial_ppid:
            raise RuntimeError(f"Current process is orphaned, initial ppid={initial_ppid}, current ppid={current_ppid}")


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
        path = self.server.output_dir_path / (str(self.server.total_clients).rjust(5, '0') + '.log')
        dst = open(path, 'wb')

        shutil.copyfileobj(self.rfile, dst)
        dst.close()

    def finish(self):
        super().finish()

        # No more opened connections? -> Shutdown server
        if self.server.connected_clients == 0:
            self.server.shutdown()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(prog=f'python -m pyexasol_utils.script_output')

    parser.add_argument('--host', default='0.0.0.0', help='Specific address to bind TCPServer (default: 0.0.0.0)')
    parser.add_argument('--port', default=0, help='Specific port to bind TCPServer (default: random port)', type=int)
    parser.add_argument('--output-dir', default=None, help='Directory to write enumerated log files, one file per VM')
    parser.add_argument('--ppid', default=0, help='PID of parent process', type=int)

    args = parser.parse_args()

    # Capture mode: collect output of all VM's into specific "output_dir"
    if args.output_dir:
        output_dir_path = pathlib.Path(args.output_dir)

        if not output_dir_path.is_dir():
            raise ValueError(f"Output_dir does not exist or not a directory: {output_dir_path}")

        # Start TCP server
        server = ExaScriptOutputServer((args.host, args.port), ExaScriptOutputScriptModeHandler)
        server.output_dir_path = output_dir_path
        server.initial_ppid = args.ppid

        # Send output address to the main process
        sys.stdout.buffer.write(f'{server.get_output_address()}\n'.encode())
        sys.stdout.buffer.flush()

        # Handle incoming connections
        # Server is stopped by shutdown() call in handler after closing last incoming connection
        server.serve_forever()
        server.server_close()

    # Debug mode: display output of one VM into terminal, discard output of other VM's
    else:
        # Start TCP server with debug handler
        server = ExaScriptOutputServer((args.host, args.port), ExaScriptOutputDebugModeHandler)

        # Send pre-generated SQL with output address to user terminal
        sys.stdout.write(f"ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '{server.get_output_address()}';\n")
        sys.stdout.flush()

        # Stop server manually with SIGTERM (Ctrl + C, etc.) when debugging is finished
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass

        server.server_close()
