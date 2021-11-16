"""
Ensure ExaConnection and ExaStatement objects are garbage collected properly
"""

import gc

import pyexasol
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)


class ExaStatementVerbose(pyexasol.ExaStatement):
    def __del__(self):
        print(f'ExaStatement session_id={self.connection.session_id()}, stmt_idx={self.stmt_idx} is about to be collected')
        super().__del__()


class ExaConnectionVerbose(pyexasol.ExaConnection):
    cls_statement = ExaStatementVerbose

    def __del__(self):
        print(f'ExaConnection {self.session_id()} is about to be collected')
        super().__del__()


def run_test():
    C = ExaConnectionVerbose(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
                             , debug=False, fetch_size_bytes=1024)

    # Execute statement, read some data
    st = C.execute('SELECT * FROM users')
    st.fetchmany(5)

    # Execute another statement, no more references for the first statement
    st = C.execute('SELECT * FROM payments')
    st.fetchmany(5)

    del st
    del C

    print('Collect call 1 start')
    gc.collect()
    print('Collect call 1 finish')

    C = ExaConnectionVerbose(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
                             , debug=False, fetch_size_bytes=1024)

    st = C.execute('SELECT * FROM users')
    st.fetchmany(5)


run_test()

print('Collect call 2 start')
gc.collect()
print('Collect call 2 finish')

print('Finishing script, nothing should be collected beyond this point!')
