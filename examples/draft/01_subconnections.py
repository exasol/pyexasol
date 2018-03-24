"""
Draft example 1
Sub-connections for parallel reading of data in multiple processes
"""

import pyexasol as E
import multiprocessing

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

config = {
    'dsn': 'exasolpool1..5.mlan',
    'user': 'sys',
    'password': 'xxx',
    'schema': 'ingres',
    'table_name': 'xxx',
    'num_parallel': 20,
}


class SelectProc(multiprocessing.Process):
    def __init__(self, shard_id, dsn, token, handle_id):
        self.shard_id = shard_id
        self.dsn = dsn
        self.token = token
        self.handle_id = handle_id

        super().__init__()

    def run(self):
        # Show full debug for only one sub-connection
        if self.shard_id == 2:
            dbg = True
        else:
            dbg = False

        C = E.connect(dsn=self.dsn, user=config['user'], password=config['password']
                      , subc_id=self.shard_id, subc_token=self.token, debug=dbg)

        st = C.subc_open_handle(self.handle_id)

        print(f"Shard {self.shard_id}, rows: {st.rowcount()}")
        st.fetchall()
        st.close()


C = E.connect(dsn=config['dsn'], user=config['user'], password=config['password'], schema=config['schema'], debug=True)

token, nodes = C.enter_parallel(config['num_parallel'])
st = C.execute("SELECT * FROM {table!i} LIMIT 100000", {'table': config['table_name']})

pool = list()
i = 0

for node in nodes:
    i += 1

    dsn = f'nexasol{int(node[7:9]) - 29}.mlan:{int(node[-5:])}'

    proc = SelectProc(i, dsn, token, st.result_set_handle)
    pool.append(proc)
    proc.start()

for proc in pool:
    proc.join()

st.close()
