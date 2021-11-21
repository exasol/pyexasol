"""
Parallel HTTP transport

EXPORT data, process it and IMPORT back to Exasol
Do it in multiple independent processes running in parallel

Compression and encryption are enabled in this example
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
    def exa_address_pair(self):
        return self.read_pipe.recv()

    def run(self):
        self.read_pipe.close()

        # Init separate HTTP transport connections for EXPORT and IMPORT
        http_export = pyexasol.http_transport(self.node['host'], self.node['port'], compression=True, encryption=True)
        http_import = pyexasol.http_transport(self.node['host'], self.node['port'], compression=True, encryption=True)

        # Send pairs of internal Exasol address to parent process
        self.write_pipe.send([http_export.exa_address, http_import.exa_address])
        self.write_pipe.close()

        # Read data from HTTP transport to DataFrame
        pd = http_export.export_to_callback(cb.export_to_pandas, None)
        print(f"EXPORT child process {self.node['idx']} finished, exported rows:{len(pd)}")

        # Modify data set
        pd['GROSS_AMT'] = pd['GROSS_AMT'] + 1

        # Write data back to HTTP transport
        http_import.import_from_callback(cb.import_from_pandas, pd)
        print(f"IMPORT child process {self.node['idx']} finished, imported rows:{len(pd)}")


if __name__ == '__main__':
    pool_size = 8
    pool = []
    exa_address_export = []
    exa_address_import = []

    C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema,
                         compression=True, encryption=True)

    C.execute("TRUNCATE TABLE payments_copy")

    for i in C.get_nodes(pool_size):
        proc = ExportProc(i)
        proc.start()

        pool.append(proc)
        pair = proc.exa_address_pair

        exa_address_export.append(pair[0])
        exa_address_import.append(pair[1])

    printer.pprint(pool)
    printer.pprint(exa_address_export)
    printer.pprint(exa_address_import)

    try:
        C.export_parallel(exa_address_export, "SELECT * FROM payments", export_params={'with_column_names': True})
    except (Exception, KeyboardInterrupt):
        for p in pool:
            p.terminate()
            p.join()
    else:
        stmt = C.last_statement()
        print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

    try:
        C.import_parallel(exa_address_import, "payments_copy")
    except (Exception, KeyboardInterrupt):
        for p in pool:
            p.terminate()
            p.join()
    else:
        stmt = C.last_statement()
        print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

    for p in pool:
        p.join()
