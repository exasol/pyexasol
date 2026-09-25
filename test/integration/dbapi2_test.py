import pytest

from pyexasol import db2


@pytest.fixture
def query():
    statement = "SELECT * FROM USERS ORDER BY USER_ID LIMIT 5;"
    # fmt: off
    expected_values = [
        (
            0, 'Jessica Mccoy', '2018-07-12',
            '2018-04-03 18:36:40.553000', True, '0.7', None, 'ACTIVE'
        ),
        (
            1, 'Beth James', '2018-05-24', '2018-03-24 08:08:46.251000',
            False, '0.53', 22.07, 'ACTIVE'
        ),
        (
            2, 'Mrs. Teresa Ryan', '2018-08-21',
            '2018-11-07 01:53:08.727000', False, '0.03', 24.88, 'PENDING'
        ),
        (
            3, 'Tommy Henderson', '2018-04-18',
            '2018-04-28 21:39:59.300000', True, '0.5', 27.43, 'DISABLED'
        ),
        (
            4, 'Jessica Christian', '2018-12-18',
            '2018-11-29 14:11:55.450000', True, '0.1', 62.59, 'SUSPENDED'
        )
    ]
    # fmt: on
    yield statement, expected_values


@pytest.fixture
def connection(dsn, user, password, schema, websocket_sslopt):
    con = db2.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        websocket_sslopt=websocket_sslopt,
    )
    yield con
    con.close()


@pytest.fixture
def cursor(connection):
    cursor = connection.cursor()
    yield cursor
    cursor.close()


@pytest.mark.dbapi2
def test_fetchone(cursor, query):
    stmt, expected_values = query
    cursor.execute(stmt)
    for expected in expected_values:
        actual = cursor.fetchone()
        assert actual == expected


@pytest.mark.dbapi2
def test_fetch_many(cursor, query):
    stmt, expected_values = query
    cursor.execute(stmt)

    actual = set(cursor.fetchmany(3))
    expected = set(expected_values[:3])
    assert actual == expected

    actual = set(cursor.fetchmany(3))
    expected = set(expected_values[3:])
    assert actual == expected
