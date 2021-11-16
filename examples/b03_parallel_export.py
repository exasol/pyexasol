"""
Parallel HTTP transport

EXPORT into multiple independent processes running in parallel
"""

import pyexasol
import _config as config

import multiprocessing
import pyexasol.callback as cb

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)


class ExportProc(multiprocessing.Process):
    def __init__(self, node):
        self.node = node
        self.read_pipe, self.write_pipe = multiprocessing.Pipe(False)

        super().__init__()

    def start(self):
        super().start()
        self.write_pipe.close()

    @property
    def exa_address(self):
        return self.read_pipe.recv()

    def run(self):
        self.read_pipe.close()

        # Init HTTP transport connection
        http = pyexasol.http_transport(self.node['host'], self.node['port'])

        # Send internal Exasol address to parent process
        self.write_pipe.send(http.exa_address)
        self.write_pipe.close()

        # Read data from HTTP transport to DataFrame
        pd = http.export_to_callback(cb.export_to_pandas, None)
        print(f"Child process {self.node['idx']} finished, exported rows: {len(pd)}")


# This condition is required for 'spawn' multiprocessing implementation (Windows)
# Feel free to skip it for POSIX operating systems
if __name__ == '__main__':
    pool_size = 5
    pool = []
    exa_address_list = []

    C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)

    for n in C.get_nodes(pool_size):
        proc = ExportProc(n)
        proc.start()

        pool.append(proc)
        exa_address_list.append(proc.exa_address)

    printer.pprint(pool)
    printer.pprint(exa_address_list)

    C.export_parallel(exa_address_list, "SELECT * FROM payments", export_params={'with_column_names': True})

    stmt = C.last_statement()
    print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

    for i in range(pool_size):
        pool[i].join()
