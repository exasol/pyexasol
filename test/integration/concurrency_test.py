import threading

import pytest

import pyexasol
from pyexasol import ExaConcurrencyError


class QueryThread(threading.Thread):

    def __init__(self, connection, timeout):
        self.connection = connection
        self.seconds = timeout
        self.exception = None
        super().__init__()

    def run(self):
        try:
            # run heavy query
            self.connection.execute(f'SELECT "$SLEEP"({self.seconds})')
        except Exception as ex:
            self.exception = ex

    def join(self, timeout=None):
        threading.Thread.join(self, timeout=timeout)
        if self.exception:
            raise self.exception


@pytest.mark.exceptions
def test_concurrency_error(connection):

    # Note all timeouts and sleeps in this test case have been chosen by well-educated guesses
    # TL;DR: Adjust timeouts if required/reasonable
    query_time_in_seconds = 0.5

    q1 = QueryThread(connection, timeout=query_time_in_seconds)
    q2 = QueryThread(connection, timeout=query_time_in_seconds)

    with pytest.raises(ExaConcurrencyError):
        q1.start()
        q2.start()
        q1.join()
        q2.join()
