"""
Abort long running query from another thread
"""

import pprint
import threading
import time

import _config as config

import pyexasol

printer = pprint.PrettyPrinter(indent=4, width=140)


class QueryThread(threading.Thread):
    def __init__(self, connection):
        self.connection = connection
        super().__init__()

    def run(self):
        try:
            # Run heavy query
            self.connection.execute(
                "SELECT * FROM users a, users b, users c, payments d"
            )
        except pyexasol.ExaQueryAbortError as e:
            print(e.message)


# Basic connect
C = pyexasol.connect(
    dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
)

# Start query thread
query_thread = QueryThread(C)
query_thread.start()

# Abort query after 1 second
time.sleep(1)
C.abort_query()

query_thread.join()
