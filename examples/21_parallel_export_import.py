"""
Example 21
Parallel EXPORT followed by IMPORT with multiple independent processes
Optional encryption and compression are enabled
"""

import pyexasol
import _config as config

import multiprocessing
import pyexasol.callback as cb

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)


class ExportProc(multiprocessing.Process):
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

        http_export = pyexasol.http_transport(self.shard_id, config.dsn, pyexasol.HTTP_EXPORT, compression=True, encryption=True)
        http_import = pyexasol.http_transport(self.shard_id, config.dsn, pyexasol.HTTP_IMPORT, compression=True, encryption=True)

        # Send list of proxy strings, one element per HTTP transport instance
        self.write_pipe.send([http_export.get_proxy(), http_import.get_proxy()])
        self.write_pipe.close()

        pd = http_export.export_to_callback(cb.export_to_pandas, None)
        print(f'EXPORT shard_id:{self.shard_id}, affected_rows:{len(pd)}')

        http_import.import_from_callback(cb.import_from_pandas, pd)
        print(f'IMPORT shard_id:{self.shard_id}, affected_rows:{len(pd)}')


# This condition is required for 'spawn' multiprocessing implementation (Windows)
# Feel free to skip it for POSIX operating systems
if __name__ == '__main__':
    pool_size = 8
    pool = list()
    proxy_export_list = list()
    proxy_import_list = list()

    C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema,
                         compression=True, encryption=True)

    C.execute("TRUNCATE TABLE payments_copy")

    for i in range(pool_size):
        proc = ExportProc(i)
        proc.start()

        proxy = proc.get_proxy()

        proxy_export_list.append(proxy[0])
        proxy_import_list.append(proxy[1])
        pool.append(proc)

    printer.pprint(pool)
    printer.pprint(proxy_export_list)
    printer.pprint(proxy_import_list)

    C.export_parallel(proxy_export_list, "SELECT * FROM payments", export_params={'with_column_names': True})

    stmt = C.last_statement()
    print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

    C.import_parallel(proxy_import_list, "payments_copy")

    stmt = C.last_statement()
    print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

    for i in range(pool_size):
        pool[i].join()
