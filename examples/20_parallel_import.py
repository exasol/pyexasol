"""
Example 20
Parallel import from multiple independent processes
"""

import pyexasol as E
import _config as config

import multiprocessing
import pyexasol.callback as cb

import pandas

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)


class ImportProc(multiprocessing.Process):
    def __init__(self, shard_id):
        self.shard_id = shard_id
        self.read_pipe, self.write_pipe = multiprocessing.Pipe(False)

        super().__init__()

    def start(self):
        super().start()
        self.write_pipe.close()

    def get_proxy(self):
        return self.read_pipe.recv()

    def run(self):
        self.read_pipe.close()

        http = E.http_transport(self.shard_id, config.dsn, E.HTTP_IMPORT)
        self.write_pipe.send(http.get_proxy())
        self.write_pipe.close()

        data = [
            {'user_id': 1, 'user_name': 'John', 'shard_id': self.shard_id},
            {'user_id': 2, 'user_name': 'Foo', 'shard_id': self.shard_id},
            {'user_id': 3, 'user_name': 'Bar', 'shard_id': self.shard_id},
        ]

        pd = pandas.DataFrame(data, columns=['user_id', 'user_name', 'shard_id'])

        http.import_from_callback(cb.import_from_pandas, pd)


# This condition is required for 'spawn' multiprocessing implementation (Windows)
# Feel free to skip it for POSIX operating systems
if __name__ == '__main__':
    pool_size = 5
    pool = list()
    proxy_list = list()

    C = E.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)

    C.execute('TRUNCATE TABLE parallel_import')

    for i in range(pool_size):
        proc = ImportProc(i)
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
