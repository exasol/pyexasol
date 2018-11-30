"""
Example 20
Parallel import from multiple independent processes
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

    def get_proxy(self):
        return self.read_pipe.recv()

    def run(self):
        self.read_pipe.close()

        http = pyexasol.http_transport(self.node['host'], self.node['port'], pyexasol.HTTP_IMPORT)
        self.write_pipe.send(http.get_proxy())
        self.write_pipe.close()

        data = [
            {'user_id': 1, 'user_name': 'John', 'shard_id': self.node['idx']},
            {'user_id': 2, 'user_name': 'Foo', 'shard_id': self.node['idx']},
            {'user_id': 3, 'user_name': 'Bar', 'shard_id': self.node['idx']},
        ]

        pd = pandas.DataFrame(data, columns=['user_id', 'user_name', 'shard_id'])

        http.import_from_callback(cb.import_from_pandas, pd)


# This condition is required for 'spawn' multiprocessing implementation (Windows)
# Feel free to skip it for POSIX operating systems
if __name__ == '__main__':
    pool_size = 5
    pool = list()
    proxy_list = list()

    C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)

    C.execute('TRUNCATE TABLE parallel_import')

    for n in C.get_nodes(pool_size):
        proc = ImportProc(n)
        proc.start()

        proxy_list.append(proc.get_proxy())
        pool.append(proc)

    printer.pprint(pool)
    printer.pprint(proxy_list)

    C.import_parallel(proxy_list, 'parallel_import')

    stmt = C.last_statement()
    print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

    for i in range(pool_size):
        pool[i].join()
