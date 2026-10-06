import os
import ssl
from dataclasses import dataclass
from unittest import mock
from unittest.mock import create_autospec

import pytest
import websocket

from pyexasol.connection import ExaConnection

# pylint: disable=protected-access/W0212


@dataclass(frozen=True)
class ConnectionMockFixture:
    connection: ExaConnection
    get_hostname_mock: mock.Mock
    create_websocket_connection_mock: mock.Mock

    def simulate_resolve_hostname(self, host: str, ips: list[str]):
        self.get_hostname_mock.return_value = (host, [], ips)

    def assert_websocket_created(self, url: str, **args: dict):
        self.create_websocket_connection_mock.assert_called_once_with(url, **args)

    def init_ws(self):
        self.connection._init_ws()


@pytest.fixture
def connection_mock(connection):
    org_ws = connection._ws
    org_ws_send = connection._ws_send
    org_ws_recv = connection._ws_recv
    try:
        with mock.patch("socket.gethostbyname_ex") as get_hostname_mock:
            with mock.patch(
                "websocket.create_connection"
            ) as create_websocket_connection_mock:
                create_websocket_connection_mock.return_value = create_autospec(
                    websocket.WebSocket, instance=True
                )
                yield ConnectionMockFixture(
                    connection, get_hostname_mock, create_websocket_connection_mock
                )
    finally:
        connection._ws = org_ws
        connection._ws_send = org_ws_send
        connection._ws_recv = org_ws_recv


def test_init_ws_connects_via_ipaddress(connection_mock, websocket_sslopt):
    connection_mock.simulate_resolve_hostname("localhost", ["ip1"])
    connection_mock.init_ws()
    ssl_options = websocket_sslopt.copy()
    ssl_options["server_hostname"] = "localhost"
    connection_mock.assert_websocket_created(
        "wss://ip1:8563",
        timeout=10,
        skip_utf8_validation=True,
        enable_multithread=True,
        sslopt=ssl_options,
    )


def test_init_ws_connects_without_encryption_via_hostname(connection_mock, dsn):
    connection_mock.connection.options["encryption"] = False
    connection_mock.connection.options["resolve_hostnames"] = False
    connection_mock.simulate_resolve_hostname("localhost", ["ip1"])
    connection_mock.init_ws()
    connection_mock.assert_websocket_created(
        f"ws://{dsn}",
        timeout=10,
        skip_utf8_validation=True,
        enable_multithread=True,
    )


def test_init_ws_connects_via_hostname(connection_mock, dsn, websocket_sslopt):
    connection_mock.connection.options["resolve_hostnames"] = False
    connection_mock.simulate_resolve_hostname("localhost", ["ip1"])
    connection_mock.init_ws()
    connection_mock.assert_websocket_created(
        f"wss://{dsn}",
        timeout=10,
        skip_utf8_validation=True,
        enable_multithread=True,
        sslopt=websocket_sslopt,
    )


@pytest.mark.parametrize(
    "nocertcheckvalue", ["nocertcheck", "NOCERTCHECK", "NoCertcheck"]
)
def test_websocket_connection_no_cert_check_if_fingerprint_has_value_nocertcheck(
    connection_mock,
    dsn,
    certificate_type,
    default_ipaddr,
    default_port,
    websocket_sslopt,
    nocertcheckvalue,
):
    def build_dsn(certificate_type, ipaddr, port) -> str:
        if certificate_type == ssl.CERT_NONE:
            return os.environ.get("EXAHOST", f"{ipaddr}/{nocertcheckvalue}:{port}")
        # The host name is different for this case. As it is required to be the same
        # host name that the certificate is signed. This comes from the ITDE.
        return os.environ.get(
            "EXAHOST", f"exasol-test-database/{nocertcheckvalue}:{port}"
        )

    connection_mock.connection.options["resolve_hostnames"] = False
    connection_mock.connection.options["dsn"] = build_dsn(
        certificate_type, default_ipaddr, default_port
    )
    connection_mock.connection.options["websocket_sslopt"] = None
    connection_mock.simulate_resolve_hostname("localhost", ["ip1"])
    connection_mock.init_ws()
    expected_websocket_sslopt = websocket_sslopt.copy()
    expected_websocket_sslopt["cert_reqs"] = ssl.CERT_NONE
    if "ca_certs" in websocket_sslopt:
        del expected_websocket_sslopt["ca_certs"]

    connection_mock.assert_websocket_created(
        f"wss://{dsn}",
        timeout=10,
        skip_utf8_validation=True,
        enable_multithread=True,
        sslopt=expected_websocket_sslopt,
    )
