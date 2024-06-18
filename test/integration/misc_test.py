import pytest
from inspect import cleandoc


@pytest.fixture
def disable_query_cache_for_session(connection):
    query = (
        "SELECT session_value FROM EXA_PARAMETERS "
        "WHERE parameter_name = 'QUERY_CACHE';"
    )
    stmt = "ALTER SESSION SET QUERY_CACHE = '{mode}'"
    mode = connection.execute(query).fetchval()
    connection.execute(stmt.format(mode="OFF"))
    yield
    connection.execute(stmt.format(mode=mode))


@pytest.fixture
def enable_session_profiling(connection):
    stmt = "ALTER SESSION SET PROFILE = '{mode}'"
    connection.execute(stmt.format(mode="ON"))
    yield
    connection.execute(stmt.format(mode="OFF"))


@pytest.fixture
def query():
    # fmt: off
    yield cleandoc(
        """
        SELECT u.user_id, sum(p.gross_amt) AS total_gross_amt
        FROM users u
            LEFT JOIN payments p ON (u.user_id=p.user_id)
        GROUP BY 1
        ORDER BY 2 DESC NULLS LAST
        LIMIT 10
        """
    )
    # fmt: on


@pytest.mark.misc
def test_normal_profiling(
    connection, disable_query_cache_for_session, enable_session_profiling, query
):
    expected = {
        "cpu",
        "duration",
        "hdd_read",
        "hdd_write",
        "in_rows",
        "mem_peak",
        "net",
        "object_name",
        "object_rows",
        "object_schema",
        "out_rows",
        "part_id",
        "part_info",
        "part_name",
        "remarks",
        "start_time",
        "stop_time",
        "temp_db_ram_peak",
    }
    result = connection.ext.explain_last()
    actual = set(result[0])
    assert actual == expected


@pytest.mark.misc
def test_detailed_profiling(
    connection, disable_query_cache_for_session, enable_session_profiling, query
):
    expected = {
        "cpu",
        "duration",
        "hdd_read",
        "hdd_write",
        "in_rows",
        "iproc",
        "mem_peak",
        "net",
        "object_name",
        "object_rows",
        "object_schema",
        "out_rows",
        "part_id",
        "part_info",
        "part_name",
        "remarks",
        "start_time",
        "stop_time",
        "temp_db_ram_peak",
    }
    result = connection.ext.explain_last(details=True)
    actual = set(result[0])
    assert actual == expected
