from inspect import cleandoc

import pytest

import pyexasol


@pytest.fixture
def logging_address():
    # 172.17.0.1 is an IP address of docker host in Linux
    yield ("172.17.0.1", 8580)


@pytest.fixture
def connection(dsn, user, password, schema, websocket_sslopt, logging_address):
    _, port = logging_address
    con = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        websocket_sslopt=websocket_sslopt,
        udf_output_bind_address=("", port),
        udf_output_connect_address=logging_address,
    )

    yield con

    con.close()


@pytest.fixture
def echo(connection, logging_address):
    name = "ECHO"
    ip, port = logging_address
    udf = cleandoc(
        f"""
        --/
        CREATE OR REPLACE LUA SCALAR SCRIPT {name}(text VARCHAR(2000))
            RETURNS VARCHAR(2000) AS

        function run(ctx)
            local socket = require("socket")
            local tcp_socket = socket.tcp()
            local ok, err = tcp_socket:connect("{ip}", {port})
            tcp_socket:send(ctx.text)
            tcp_socket:shutdown()
            return ctx.text
        end
        /
        """
    )
    connection.execute(udf)

    def executor(text):
        udf_call = f"SELECT {name}('{text}');"
        stmt, logfiles = connection.execute_udf_output(udf_call)
        result = stmt.fetchval()
        log = "LOG: " + "".join(log.read_text() for log in logfiles)
        return result, log

    yield executor

    drop = f"DROP SCRIPT IF EXISTS {name};"
    connection.execute(drop)


@pytest.mark.udf
def test_udf_output(connection, echo):
    text = "Some text which should be echoed"

    result, log = echo(text)

    expected_result = text
    expected_log = f"LOG: {text}"

    actual_result = result
    actual_log = log

    assert actual_result == expected_result
    assert actual_log == expected_log
