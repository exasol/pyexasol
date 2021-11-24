"""
Parallel HTTP transport

IMPORT from multiple independent processes running in parallel
"""

import pyexasol
import _config as config

import multiprocessing
import pyexasol.callback as cb

import pandas

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)


class ImportProc(multiprocessing.Process):
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
        http = pyexasol.http_transport(self.node['ipaddr'], self.node['port'])

        # Send internal Exasol address to parent process
        self.write_pipe.send(http.exa_address)
        self.write_pipe.close()

        data = [
            {'user_id': 1, 'user_name': 'John', 'shard_id': self.node['idx']},
            {'user_id': 2, 'user_name': 'Foo', 'shard_id': self.node['idx']},
            {'user_id': 3, 'user_name': 'Bar', 'shard_id': self.node['idx']},
        ]

        pd = pandas.DataFrame(data, columns=['user_id', 'user_name', 'shard_id'])

        # Send data from DataFrame to HTTP transport
        http.import_from_callback(cb.import_from_pandas, pd)
        print(f"Child process {self.node['idx']} finished, imported rows: {len(pd)}")


if __name__ == '__main__':
    pool_size = 5
    pool = []
    exa_address_list = []

    C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)

    C.execute('TRUNCATE TABLE parallel_import')

    for n in C.get_nodes(pool_size):
        proc = ImportProc(n)
        proc.start()

        pool.append(proc)
        exa_address_list.append(proc.exa_address)

    printer.pprint(pool)
    printer.pprint(exa_address_list)

    try:
        C.import_parallel(exa_address_list, 'parallel_import')
    except (Exception, KeyboardInterrupt):
        for p in pool:
            p.terminate()
    else:
        stmt = C.last_statement()
        print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')
    finally:
        for p in pool:
            p.join()
