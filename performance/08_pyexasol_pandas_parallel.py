import multiprocessing

import performance._config as config
import pyexasol
import pyexasol.callback as cb


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

        http = pyexasol.http_transport(self.shard_id, config.dsn, pyexasol.HTTP_EXPORT)
        self.write_pipe.send(http.get_proxy())
        self.write_pipe.close()

        df = http.export_to_callback(cb.export_to_pandas, None)
        print(f"{self.shard_id}:{len(df)}")


# This condition is required for 'spawn' multiprocessing implementation (Windows)
# Feel free to skip it for POSIX operating systems
if __name__ == "__main__":
    pool_size = 5
    pool = list()
    proxy_list = list()

    C = pyexasol.connect(
        dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
    )

    for i in range(pool_size):
        proc = ExportProc(i)
        proc.start()

        proxy_list.append(proc.get_proxy())
        pool.append(proc)

    C.export_parallel(
        proxy_list, config.table_name, export_params={"with_column_names": True}
    )

    for i in range(pool_size):
        pool[i].join()
