"""
Attempt to access connection object from multiple threads simultaneously
"""

import pyexasol
import time
import threading
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)


class QueryThread(threading.Thread):
    def __init__(self, connection):
        self.connection = connection
        super().__init__()

    def run(self):
        # Run heavy query
        try:
            self.connection.execute("SELECT * FROM users a, users b, users c, payments d")
        except pyexasol.ExaQueryAbortError as e:
            print(e.message)
        except pyexasol.ExaConcurrencyError as e:
            print(e.message)


# Basic connect
C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)

# Try to run multiple query threads in parallel
query_thread_1 = QueryThread(C)
query_thread_2 = QueryThread(C)

query_thread_1.start()
query_thread_2.start()

time.sleep(1)
C.abort_query()

query_thread_1.join()
query_thread_2.join()
