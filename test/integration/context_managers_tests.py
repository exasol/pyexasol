import pytest
import pyexasol
from pyexasol import ExaQueryError


@pytest.mark.context_managers
def test_context_manager_for_connections(dsn, user, password, schema):
    with pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema
    ) as connection:
        connection.execute("SELECT 1;")

    expected = True
    actual = connection.is_closed

    assert actual == expected


@pytest.mark.context_managers
def test_context_manager_for_statements(connection):
    statement = "SELECT * FROM users LIMIT 5;"
    with connection.execute(statement) as stmt:
        _ = stmt.fetchall()

    expected = True
    actual = stmt.is_closed

    assert actual == expected


@pytest.mark.exceptions
@pytest.mark.context_managers
def test_context_manger_closes_everything_on_exception(dsn, user, password, schema):
    with pytest.raises(ExaQueryError):
        with pyexasol.connect(
            dsn=dsn,
            user=user,
            password=password,
            schema=schema
        ) as connection:
            statement = "SELECT * FROM unkown_table_so_query_will_fail LIMIT 5;"
            with connection.execute(statement) as stmt:
                stmt.fetchall()

            expected = True
            actual = stmt.is_closed
            assert actual == expected

        expected = True
        actual = connection.is_closed
        assert actual == expected
