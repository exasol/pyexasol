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


@pytest.mark.dbapi2
def test_fetch_all(cursor, query):
    stmt, expected_values = query
    cursor.execute(stmt)

    actual = set(cursor.fetchall())
    expected = set(expected_values)
    assert actual == expected


@pytest.mark.dbapi2
def test_description(cursor, query, expected_user_table_column_last_visit_ts):
    stmt, expected_values = query
    cursor.execute(stmt)
    cursor.fetchall()

    expected_size = expected_user_table_column_last_visit_ts.size

    expected = {
        ("USER_ID", "DECIMAL", None, None, 18, 0, True),
        ("USER_NAME", "VARCHAR", 255, 255, None, None, True),
        ("REGISTER_DT", "DATE", 4, 4, None, None, True),
        ("LAST_VISIT_TS", "TIMESTAMP", expected_size, expected_size, None, None, True),
        ("IS_FEMALE", "BOOLEAN", None, None, None, None, True),
        ("USER_RATING", "DECIMAL", None, None, 10, 5, True),
        ("USER_SCORE", "DOUBLE", None, None, None, None, True),
        ("STATUS", "VARCHAR", 50, 50, None, None, True),
    }
    actual = set(cursor.description)
    assert actual == expected


@pytest.mark.dbapi2
def test_rowcount(cursor, query):
    stmt, expected_values = query
    cursor.execute(stmt)
    cursor.fetchall()

    expected = len(expected_values)
    actual = cursor.rowcount
    assert actual == expected


@pytest.mark.dbapi2
def test_autocommit(connection):
    assert not connection.attr["autocommit"]
