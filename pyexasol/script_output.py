import os
import sys
import subprocess


class ExaScriptOutputProcess(object):
    def __init__(self, host, port, output_dir):
        self.host = host
        self.port = port

        self.output_dir = output_dir
        self.output_address = None

        self.proc = None

    def start(self):
        args = [sys.executable,
                '-m', 'pyexasol_utils.script_output',
                '--output-dir', str(self.output_dir),
                '--ppid', str(os.getpid())
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

    def get_output_address(self):
        if self.output_address is None:
            raise RuntimeError("Script output address 'host:port' is not available")

        return self.output_address

    def join(self):
        if self.proc:
            return self.proc.wait()

        return None

    def join_with_exc(self):
        code = self.join()

        if code != 0:
            raise RuntimeError(f"Script output server process finished with exitcode: {code}")

        return code

    def terminate(self):
        if self.proc:
            self.proc.terminate()
