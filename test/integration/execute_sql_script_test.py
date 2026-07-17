import uuid
from inspect import cleandoc

import pytest

from pyexasol import ExaQueryError


def unique_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4()}".replace("-", "_").upper()


@pytest.mark.basic
def test_execute_sql_script_executes_script_and_returns_statements(connection):
    table_name = unique_name("SCRIPT_TABLE")
    script = cleandoc(f"""
        CREATE OR REPLACE TABLE {table_name} ("VALUE;TEXT" VARCHAR(20));
        -- line comments may contain semicolons; they must not split statements
        INSERT INTO {table_name} ("VALUE;TEXT") VALUES ('a;b');
        /* block comments may contain semicolons; they must not split statements */
        SELECT "VALUE;TEXT" FROM {table_name};
        """)

    try:
        statements = connection.execute_sql_script(script)

        assert len(statements) == 3
        assert statements[1].rowcount() == 1
        assert statements[2].fetchall() == [("a;b",)]
    finally:
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.udf
def test_execute_sql_script_executes_slash_terminated_exasol_script(connection):
    script_name = unique_name("SCRIPT_FUNC")
    script = cleandoc(f"""
        CREATE OR REPLACE LUA SCALAR SCRIPT {script_name}()
            RETURNS DOUBLE AS

        function run(ctx)
            local value = 1;
            return value + 41;
        end
        /
        SELECT {script_name}();
        """)

    try:
        statements = connection.execute_sql_script(script)

        assert len(statements) == 2
        assert statements[1].fetchval() == 42
    finally:
        connection.execute(f"DROP SCRIPT IF EXISTS {script_name}")


@pytest.mark.exceptions
def test_execute_sql_script_stops_after_first_failing_statement(connection):
    table_name = unique_name("SCRIPT_FAILURE")
    script = cleandoc(f"""
        CREATE TABLE {table_name} (val INT);
        SELECT * FROM {table_name}_DOES_NOT_EXIST;
        INSERT INTO {table_name} VALUES 1;
        """)

    try:
        with pytest.raises(
            ExaQueryError, match=rf"object {table_name}_DOES_NOT_EXIST not found"
        ):
            connection.execute_sql_script(script)

        assert connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchval() == 0
    finally:
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")
