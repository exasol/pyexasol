"""
Parallel HTTP transport

EXPORT into multiple independent processes running in parallel
"""

import multiprocessing
import pprint

import _config as config

import pyexasol
import pyexasol.callback as cb

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
        http = pyexasol.http_transport(self.node["ipaddr"], self.node["port"])

        # Send internal Exasol address to parent process
        self.write_pipe.send(http.exa_address)
        self.write_pipe.close()

        # Read data from HTTP transport to DataFrame
        pd = http.export_to_callback(cb.export_to_pandas, None)
        print(f"Child process {self.node['idx']} finished, exported rows: {len(pd)}")


if __name__ == "__main__":
    pool_size = 5
    pool = []
    exa_address_list = []

    C = pyexasol.connect(
        dsn=config.dsn,
        user=config.user,
        password=config.password,
        schema=config.schema,
        websocket_sslopt=config.websocket_sslopt,
    )

    for n in C.get_nodes(pool_size):
        proc = ExportProc(n)
        proc.start()

        pool.append(proc)
        exa_address_list.append(proc.exa_address)

    printer.pprint(pool)
    printer.pprint(exa_address_list)

    try:
        C.export_parallel(
            exa_address_list,
            "SELECT * FROM payments",
            export_params={"with_column_names": True},
        )
    except (Exception, KeyboardInterrupt):
        for p in pool:
            p.terminate()
    else:
        stmt = C.last_statement()
        print(f"EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s")
    finally:
        for p in pool:
            p.join()
