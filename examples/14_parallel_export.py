"""
Example 14
Parallel export into multiple independent processes
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

    def get_proxy(self):
        return self.read_pipe.recv()

    def run(self):
        self.read_pipe.close()

        http = pyexasol.http_transport(self.node['host'], self.node['port'], pyexasol.HTTP_EXPORT)
        self.write_pipe.send(http.get_proxy())
        self.write_pipe.close()

        pd = http.export_to_callback(cb.export_to_pandas, None)
        print(f"{self.node['idx']}:{len(pd)}")


# This condition is required for 'spawn' multiprocessing implementation (Windows)
# Feel free to skip it for POSIX operating systems
if __name__ == '__main__':
    pool_size = 5
    pool = list()
    proxy_list = list()

    C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)

    for n in C.get_nodes(pool_size):
        proc = ExportProc(n)
        proc.start()

        proxy_list.append(proc.get_proxy())
        pool.append(proc)

    printer.pprint(pool)
    printer.pprint(proxy_list)

    C.export_parallel(proxy_list, "SELECT * FROM payments", export_params={'with_column_names': True})

    stmt = C.last_statement()
    print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

    for i in range(pool_size):
        pool[i].join()
