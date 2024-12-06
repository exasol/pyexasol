import threading
import time

import pytest

from pyexasol import ExaQueryAbortError


class BlockingQuery(threading.Thread):

    def __init__(self, connection, timeout):
        self.connection = connection
        self.seconds = timeout
        self.exception = None
        super().__init__()

    def run(self):
        try:
            self.connection.execute(f'SELECT "$SLEEP"({self.seconds})')
        except Exception as ex:
            self.exception = ex

    def join(self, timeout=None):
        threading.Thread.join(self, timeout=timeout)
        if self.exception:
            raise self.exception


@pytest.mark.exceptions
def test_abort_query(connection):
    # Note all timeouts and sleeps in this test case have been chosen by well-educated guesses
    # TL;DR: Adjust timeouts if required/reasonable
    test_delay_in_seconds = 0.5
    query_time_in_seconds = 20
    join_timeout_in_seconds = 5

    blocking_query = BlockingQuery(connection, timeout=query_time_in_seconds)
    blocking_query.start()

    # Give the blocking query some time to start the query
    time.sleep(test_delay_in_seconds)

    with pytest.raises(ExaQueryAbortError):
        connection.abort_query()
        blocking_query.join(
            # make sure test don't get stuck
            timeout=join_timeout_in_seconds
        )
