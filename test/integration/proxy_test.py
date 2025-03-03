import platform
import subprocess

import pytest

import pyexasol


@pytest.fixture
def proxy_port():
    yield 8562


@pytest.fixture
def proxy(proxy_port):
    os_specific_flags = ["--reuse"] if platform.system() == "Linux" else []
    command = ["pproxy", "-l", f"http://:{proxy_port}/"] + os_specific_flags
    pproxy = subprocess.Popen(command)

    yield f"http://127.0.0.1:{proxy_port}"

    pproxy.kill()


@pytest.fixture
def proxy_user():
    yield "johndoe"


@pytest.fixture
def proxy_password():
    yield "JohndoesPassword"


@pytest.fixture
def proxy_with_auth(proxy_port, proxy_user, proxy_password):
    os_specific_flags = ["--reuse"] if platform.system() == "Linux" else []
    command = [
        "pproxy",
        "-l",
        f"http://:{proxy_port}/#{proxy_user}:{proxy_password}",
    ] + os_specific_flags
    pproxy = subprocess.Popen(command)

    yield f"http://{proxy_user}:{proxy_password}@localhost:{proxy_port}"

    pproxy.kill()


@pytest.mark.configuration
def test_connect_through_proxy(dsn, user, password, schema, websocket_sslopt, proxy):
    with pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        websocket_sslopt=websocket_sslopt,
        http_proxy=proxy,
    ) as connection:
        result = connection.execute("SELECT 1;")
        expected = 1
        actual = result.fetchval()
        assert expected == actual


@pytest.mark.configuration
def test_connect_through_proxy_without_resolving_host_names(
    dsn, user, password, schema, websocket_sslopt, proxy
):
    with pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        websocket_sslopt=websocket_sslopt,
        http_proxy=proxy,
        resolve_hostnames=False,
    ) as connection:
        result = connection.execute("SELECT 1;")
        expected = 1
        actual = result.fetchval()
        assert expected == actual


@pytest.mark.configuration
def test_connect_through_proxy_with_authentication(
    dsn, user, password, schema, websocket_sslopt, proxy_with_auth
):
    with pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        websocket_sslopt=websocket_sslopt,
        http_proxy=proxy_with_auth,
    ) as connection:
        result = connection.execute("SELECT 1;")
        expected = 1
        actual = result.fetchval()
        assert expected == actual
