from inspect import cleandoc

import pytest

from exasol.driver.websocket.dbapi2 import (
    Error,
    NotSupportedError,
    TypeCode,
    connect,
)


def test_websocket_dbapi(dsn, user, password, schema):
    connection = connect(
        dsn=dsn,
        username=user,
        password=password,
        schema=schema,
        certificate_validation=False,
    )
    assert connection
    connection.close()


def test_websocket_dbapi_connect_fails():
    dsn = "127.0.0.2:9999"
    username = "ShouldNotExist"
    password = "ThisShouldNotBeAValidPasswordForTheUser"
    with pytest.raises(Error) as e_info:
        connect(dsn=dsn, username=username, password=password)
    assert "Connection failed" in f"{e_info.value}"


def test_retrieve_cursor_from_connection(connection):
    cursor = connection.cursor()
    assert cursor
    cursor.close()


@pytest.mark.parametrize(
    "sql_statement", ["SELECT 1;", "SELECT * FROM VALUES 1, 2, 3, 4;"]
)
def test_cursor_execute(cursor, sql_statement):
    # Because the dbapi does not specify a required return value, this is just a smoke test
    # to ensure the execute call won't crash.
    cursor.execute(sql_statement)


@pytest.mark.parametrize(
    "sql_statement, expected",
    [
        ("SELECT 1;", (1,)),
        ("SELECT * FROM VALUES (1, 2, 3);", (1, 2, 3)),
        ("SELECT * FROM VALUES 1, 5, 9, 13;", (1,)),
    ],
    ids=str,
)
def test_cursor_fetchone(cursor, sql_statement, expected):
    cursor.execute(sql_statement)
    assert cursor.fetchone() == expected


@pytest.mark.parametrize("method", ("fetchone", "fetchmany", "fetchall"))
def test_cursor_function_raises_exception_if_no_result_has_been_produced(
    cursor, method
):
    expected = "No result has been produced."
    cursor_method = getattr(cursor, method)
    with pytest.raises(Error) as e_info:
        cursor_method()
    assert f"{e_info.value}" == expected


@pytest.mark.parametrize(
    "sql_statement, size, expected",
    [
        ("SELECT 1;", None, ((1,),)),
        ("SELECT 1;", 1, ((1,),)),
        ("SELECT 1;", 10, ((1,),)),
        ("SELECT * FROM VALUES ((1,2), (3,4), (5,6));", None, ((1, 2),)),
        ("SELECT * FROM VALUES ((1,2), (3,4), (5,6));", 1, ((1, 2),)),
        (
            "SELECT * FROM VALUES ((1,2), (3,4), (5,6));",
            2,
            (
                (1, 2),
                (3, 4),
            ),
        ),
        (
            "SELECT * FROM VALUES ((1,2), (3,4), (5,6));",
            10,
            (
                (1, 2),
                (3, 4),
                (5, 6),
            ),
        ),
    ],
    ids=str,
)
def test_cursor_fetchmany(cursor, sql_statement, size, expected):
    cursor.execute(sql_statement)
    assert cursor.fetchmany(size) == expected


@pytest.mark.parametrize(
    "sql_statement, expected",
    [
        ("SELECT 1;", ((1,),)),
        (
            "SELECT * FROM VALUES ((1,2), (3,4));",
            (
                (1, 2),
                (3, 4),
            ),
        ),
        (
            "SELECT * FROM VALUES ((1,2), (3,4), (5,6));",
            (
                (1, 2),
                (3, 4),
                (5, 6),
            ),
        ),
        (
            "SELECT * FROM VALUES ((1,2), (3,4), (5,6), (7, 8));",
            (
                (1, 2),
                (3, 4),
                (5, 6),
                (7, 8),
            ),
        ),
    ],
    ids=str,
)
def test_cursor_fetchall(cursor, sql_statement, expected):
    cursor.execute(sql_statement)
    assert cursor.fetchall() == expected


def test_description_returns_none_if_no_query_has_been_executed(cursor):
    assert cursor.description is None


@pytest.mark.parametrize(
    "sql_statement, expected",
    [
        (
            "SELECT CAST(A as INT) A FROM VALUES 1, 2, 3 as T(A);",
            (("A", TypeCode.Decimal, None, None, 18, 0, None),),
        ),
        (
            "SELECT CAST(A as DOUBLE) A FROM VALUES 1, 2, 3 as T(A);",
            (("A", TypeCode.Double, None, None, None, None, None),),
        ),
        (
            "SELECT CAST(A as BOOL) A FROM VALUES TRUE, FALSE, TRUE as T(A);",
            (("A", TypeCode.Bool, None, None, None, None, None),),
        ),
        (
            "SELECT CAST(A as VARCHAR(10)) A FROM VALUES 'Foo', 'Bar' as T(A);",
            (("A", TypeCode.String, None, 10, None, None, None),),
        ),
        (
            cleandoc(
                # fmt: off
                    """
                    SELECT CAST(A as INT) A, CAST(B as VARCHAR(100)) B, CAST(C as BOOL) C, CAST(D as DOUBLE) D
                    FROM VALUES ((1,'Some String', TRUE, 1.0), (3,'Other String', FALSE, 2.0)) as TB(A, B, C, D);
                    """
                # fmt: on
            ),
            (
                ("A", TypeCode.Decimal, None, None, 18, 0, None),
                ("B", TypeCode.String, None, 100, None, None, None),
                ("C", TypeCode.Bool, None, None, None, None, None),
                ("D", TypeCode.Double, None, None, None, None, None),
            ),
        ),
    ],
    ids=str,
)
def test_description_attribute(cursor, sql_statement, expected):
    cursor.execute(sql_statement)
    assert cursor.description == expected


@pytest.mark.parametrize(
    "sql_statement,expected",
    (
        ("SELECT 1;", 1),
        ("SELECT * FROM VALUES TRUE, FALSE as T(A);", 2),
        ("SELECT * FROM VALUES TRUE, FALSE, TRUE as T(A);", 3),
        # ATTENTION: As of today 03.02.2023 it seems there is no trivial way to make this test pass.
        #            Also, it is unclear if this semantic is required in order to function correctly
        #            with SQLA.
        #
        #            NOTE: In order to implement this semantic, subclassing pyexasol.ExaConnection and
        #                  pyexasol.ExaStatement most likely will be required.
        pytest.param("DROP SCHEMA IF EXISTS FOOBAR;", -1, marks=pytest.mark.xfail),
    ),
)
def test_rowcount_attribute(cursor, sql_statement, expected):
    cursor.execute(sql_statement)
    assert cursor.rowcount == expected


def test_rowcount_attribute_returns_minus_one_if_no_statement_was_executed_yet(cursor):
    expected = -1
    assert cursor.rowcount == expected


def test_callproc_is_not_supported(cursor):
    expected = "Optional and therefore not supported"
    with pytest.raises(NotSupportedError) as exec_info:
        cursor.callproc(None)
    assert f"{exec_info.value}" == expected


def test_cursor_nextset_is_not_supported(cursor):
    expected = "Optional and therefore not supported"
    with pytest.raises(NotSupportedError) as exec_info:
        cursor.nextset()
    assert f"{exec_info.value}" == expected


@pytest.mark.parametrize("property", ("arraysize", "description", "rowcount"))
def test_cursor_closed_cursor_raises_exception_on_property_access(connection, property):
    expected = (
        f"Unable to execute operation <{property}>, because cursor was already closed."
    )

    cursor = connection.cursor()
    cursor.close()

    with pytest.raises(Error) as exec_info:
        _ = getattr(cursor, property)

    assert f"{exec_info.value}" == expected


@pytest.mark.parametrize(
    "method,args",
    (
        ("callproc", [None]),
        ("execute", ["SELECT 1;"]),
        ("executemany", ["SELECT 1;", []]),
        ("fetchone", []),
        ("fetchmany", []),
        ("fetchall", []),
        ("nextset", []),
        ("setinputsizes", [None]),
        ("setoutputsize", [None, None]),
        ("close", []),
    ),
    ids=str,
)
def test_cursor_closed_cursor_raises_exception_on_method_usage(
    connection, method, args
):
    expected = (
        f"Unable to execute operation <{method}>, because cursor was already closed."
    )

    cursor = connection.cursor()
    cursor.execute("SELECT 1;")
    cursor.close()

    with pytest.raises(Error) as exec_info:
        method = getattr(cursor, method)
        method(*args)

    assert f"{exec_info.value}" == expected


@pytest.fixture
def test_schema(cursor):
    schema = "TEST"
    cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    cursor.execute(f"CREATE SCHEMA {schema};")
    yield schema
    cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.fixture
def users_table(cursor, test_schema):
    table = "USERS"
    cursor.execute(f"DROP TABLE IF EXISTS {test_schema}.{table}")
    cursor.execute(
        # fmt: off
        cleandoc(
            f"""
            CREATE TABLE {test_schema}.{table} (
                firstname VARCHAR(100) ,
                lastname VARCHAR(100),
                id DECIMAL
            );
            """
        )
        # fmt: on
    )
    yield f"{test_schema}.{table}"
    cursor.execute(f"DROP TABLE IF EXISTS {test_schema}.{table}")


def test_cursor_executemany(users_table, cursor):
    values = [("John", "Doe", 0), ("Donald", "Duck", 1)]

    cursor.execute(f"SELECT * FROM {users_table};")
    before = cursor.fetchall()

    cursor.executemany(f"INSERT INTO {users_table} VALUES (?, ?, ?);", values)

    cursor.execute(f"SELECT * FROM {users_table};")
    after = cursor.fetchall()

    expected = len(values)
    actual = len(after) - len(before)

    assert actual == expected
