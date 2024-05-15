import pytest
import pyexasol


# For the fetch_dict tests we need to configure the connection accordingly (autocommit=False)
@pytest.fixture
def connection(dsn, user, password, schema):
    con = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        autocommit=False
    )
    yield con
    con.close()


@pytest.mark.transaction
def test_rollback(connection):
    count = "SELECT COUNT(*) FROM users;"
    delete_rows = "TRUNCATE TABLE users;"
    initial_rowcount = connection.execute(count).fetchval()

    connection.execute(delete_rows)
    expected = 0
    actual = connection.execute(count).fetchval()
    assert expected == actual

    connection.rollback()

    actual = connection.execute(count).fetchval()
    expected = initial_rowcount

    assert expected == actual


@pytest.mark.transaction
def test_commit(connection):
    count = "SELECT COUNT(*) FROM users;"
    delete_rows = "TRUNCATE TABLE users;"
    initial_rowcount = connection.execute(count).fetchval()

    assert initial_rowcount != 0

    connection.execute(delete_rows)
    connection.commit()
    connection.rollback()

    actual = connection.execute(count).fetchval()
    expected = 0

    assert expected == actual
