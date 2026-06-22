import platform
import socket
import ssl
import subprocess
import time

import pytest

import pyexasol


@pytest.fixture
def proxy_port():
    yield 8562


def _start_proxy(command, proxy_port):
    proxy_process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if proxy_process.poll() is not None:
            stdout, stderr = proxy_process.communicate()
            raise RuntimeError(
                "pproxy exited before the proxy port became ready "
                f"(code={proxy_process.returncode}, stdout={stdout!r}, stderr={stderr!r})"
            )

        try:
            with socket.create_connection(("127.0.0.1", proxy_port), timeout=0.2):
                return proxy_process
        except OSError:
            time.sleep(0.1)

    proxy_process.kill()
    stdout, stderr = proxy_process.communicate()
    raise TimeoutError(
        "Timed out waiting for pproxy to listen on the proxy port "
        f"(stdout={stdout!r}, stderr={stderr!r})"
    )


@pytest.fixture
def proxy(proxy_port):
    os_specific_flags = ["--reuse"] if platform.system() == "Linux" else []
    command = ["pproxy", "-l", f"http://:{proxy_port}/"] + os_specific_flags
    pproxy = _start_proxy(command, proxy_port)

    yield f"http://127.0.0.1:{proxy_port}"

    pproxy.kill()
    pproxy.communicate()


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
    pproxy = _start_proxy(command, proxy_port)

    yield f"http://{proxy_user}:{proxy_password}@localhost:{proxy_port}"

    pproxy.kill()
    pproxy.communicate()


@pytest.mark.configuration
def test_connect_through_proxy(connection_factory, proxy):
    with connection_factory(http_proxy=proxy) as con:
        result = con.execute("SELECT 1;")
        expected = 1
        actual = result.fetchval()
        assert expected == actual


def test_connect_through_proxy_without_resolving_host_names(
    dsn_resolved, user, password, schema, proxy
):
    # cannot run with unresolved host name, as resolution turned off
    with pyexasol.connect(
        dsn=dsn_resolved,
        user=user,
        password=password,
        schema=schema,
        websocket_sslopt={"cert_reqs": ssl.CERT_NONE},
        http_proxy=proxy,
        resolve_hostnames=False,
    ) as con:
        result = con.execute("SELECT 1;")
        expected = 1
        actual = result.fetchval()
        assert expected == actual


@pytest.mark.configuration
def test_connect_through_proxy_with_authentication(connection_factory, proxy_with_auth):
    with connection_factory(http_proxy=proxy_with_auth) as con:
        result = con.execute("SELECT 1;")
        expected = 1
        actual = result.fetchval()
        assert expected == actual
