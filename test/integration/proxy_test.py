import pytest
import pyexasol
import subprocess
import platform


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
def test_connect_through_proxy(dsn, user, password, schema, proxy):
    with pyexasol.connect(
        dsn=dsn, user=user, password=password, schema=schema, http_proxy=proxy
    ) as connection:
        result = connection.execute("SELECT 1;")
        expected = 1
        actual = result.fetchval()
        assert expected == actual


@pytest.mark.configuration
def test_connect_through_proxy_with_authentication(
    dsn, user, password, schema, proxy_with_auth
):
    with pyexasol.connect(
        dsn=dsn, user=user, password=password, schema=schema, http_proxy=proxy_with_auth
    ) as connection:
        result = connection.execute("SELECT 1;")
        expected = 1
        actual = result.fetchval()
        assert expected == actual
