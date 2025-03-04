import gc
import weakref

import pytest

import pyexasol


@pytest.mark.misc
def test_exa_statement_gets_garbage_collected(connection):
    # Execute statement, read some data
    stmt = connection.execute("SELECT * FROM users")
    stmt_ref = weakref.ref(stmt)
    stmt.fetchmany(5)

    assert stmt_ref() is not None

    # Execute another statement, no more references for the first statement
    stmt = connection.execute("SELECT * FROM payments")
    stmt.fetchmany(5)

    # collect unreferenced objects
    gc.collect()

    assert stmt_ref() is None


@pytest.mark.misc
def test_exa_connection_gets_garbage_collected(
    dsn, user, password, schema, websocket_sslopt
):
    # create connection
    con = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        websocket_sslopt=websocket_sslopt,
    )
    stmt_ref = weakref.ref(con)

    assert stmt_ref() is not None

    # replace binding to old connection object
    con = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        websocket_sslopt=websocket_sslopt,
    )

    # collect unreferenced objects
    gc.collect()

    assert stmt_ref() is None
