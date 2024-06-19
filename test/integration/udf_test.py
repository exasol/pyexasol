import pytest
import pyexasol
from inspect import cleandoc


@pytest.fixture
def connection(dsn, user, password, schema):
    con = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        # 172.17.0.1 is an IP address of docker host in Linux
        udf_output_bind_address=("", 8580),
        udf_output_connect_address=("172.17.0.1", 8580),
    )

    yield con

    con.close()


@pytest.fixture
def echo(connection):
    name = "ECHO"
    udf = cleandoc(
        f"""
        --/
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {name}(text VARCHAR(2000)) RETURNS VARCHAR(2000) AS
        def run(ctx):
          import sys
          print(ctx.text)
          sys.stdout.flush()
          return ctx.text
        /
        """
    )
    connection.execute(udf)

    def executor(text):
        udf_call = f"SELECT {name}('{text}');"
        stmt, logfiles = connection.execute_udf_output(udf_call)
        result = stmt.fetchval()
        log = "LOG: " + "".join((log.read_text() for log in logfiles))
        return result, log

    yield executor

    drop = f"DROP SCRIPT IF EXISTS {name};"
    connection.execute(drop)


@pytest.mark.udf
def test_udf_output(connection, echo):
    text = "Some text which should be echoed"

    result, log = echo(text)

    expected_result = text
    expected_log = f"LOG: {text}\n"

    actual_result = result
    actual_log = log

    assert actual_result == expected_result
    assert actual_log == expected_log
