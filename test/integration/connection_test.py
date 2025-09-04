import os
import ssl
from dataclasses import dataclass
from typing import Optional
from unittest import mock
from unittest.mock import create_autospec

import pytest
import websocket

from pyexasol.connection import (
    ExaConnection,
    Host,
)
from pyexasol.exceptions import ExaConnectionDsnError

# pylint: disable=protected-access/W0212


@dataclass(frozen=True)
class ConnectionMockFixture:
    connection: ExaConnection
    get_hostname_mock: mock.Mock
    create_websocket_connection_mock: mock.Mock

    def simulate_resolve_hostname(self, host: str, ips: list[str]):
        self.get_hostname_mock.return_value = (host, [], ips)

    def simulate_resolve_hostnames(self, hosts: list[tuple[str, list[str], list[str]]]):
        self.get_hostname_mock.side_effect = hosts

    def assert_websocket_created(self, url: str, **args: dict):
        self.create_websocket_connection_mock.assert_called_once_with(url, **args)

    def resolve_hostname(self, hostname: str, port: int, fingerprint: Optional[str]):
        return self.connection._resolve_hostname(hostname, port, fingerprint)

    def process_dsn(self, dsn: str):
        return self.connection._process_dsn(dsn)

    def init_ws(self):
        self.connection._init_ws()

    def get_websocket_connection_string(
        self, hostname: str, ipaddr: Optional[str], port: int
    ) -> str:
        return self.connection._get_websocket_connection_string(hostname, ipaddr, port)


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


def test_resolve_hostname(connection_mock):
    connection_mock.simulate_resolve_hostname("host", ["ip1", "ip2"])
    actual = connection_mock.resolve_hostname("host", 1234, "fingerprint")
    expected = [
        ("host", "ip1", 1234, "fingerprint"),
        ("host", "ip2", 1234, "fingerprint"),
    ]
    assert len(actual) == len(expected)
    for i in range(0, len(expected)):
        assert expected[i] in actual


@pytest.mark.parametrize("empty_dsn", [None, "", " ", "\t"])
def test_process_empty_dsn_fails(connection_mock, empty_dsn):
    with pytest.raises(ExaConnectionDsnError, match="Connection string is empty"):
        connection_mock.process_dsn(empty_dsn)


def test_process_dsn_resolves_hostname_to_ip_address(connection_mock):
    connection_mock.simulate_resolve_hostnames([("host1", [], ["ip1"])])
    actual = connection_mock.process_dsn("host1:1234")
    expected = [Host("host1", "ip1", 1234, None)]
    assert expected == actual


def test_process_dsn_does_not_resolve_hostname(connection_mock):
    connection_mock.connection.options["resolve_hostnames"] = False
    actual = connection_mock.process_dsn("host1:1234")
    expected = [Host("host1", None, 1234, None)]
    assert expected == actual


def test_process_dsn_shuffles_hosts(connection_mock):
    dsn = "host1:1234,host2:4321"

    def resolve_hostname(con):
        connection_mock.simulate_resolve_hostnames(
            [("host1", [], ["ip11", "ip12"]), ("host2", [], ["ip21", "ip22"])]
        )
        return tuple(con.process_dsn(dsn))

    count = 100
    results = {resolve_hostname(connection_mock) for _ in range(0, count)}
    assert len(results) > 1


def test_process_dsn_with_fallback_to_default_port(connection_mock):
    connection_mock.simulate_resolve_hostname("host1", ["ip1"])
    actual = connection_mock.process_dsn("host1")
    expected = [Host("host1", "ip1", 8563, None)]
    assert actual == expected


def test_process_dsn_with_fingerprint(connection_mock):
    connection_mock.simulate_resolve_hostname("host1", ["ip1"])
    actual = connection_mock.process_dsn(
        "host1/135a1d2dce102de866f58267521f4232153545a075dc85f8f7596f57e588a181:1234"
    )
    expected = [
        Host(
            "host1",
            "ip1",
            1234,
            "135A1D2DCE102DE866F58267521F4232153545A075DC85F8F7596F57E588A181",
        )
    ]
    assert actual == expected


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


def test_init_ws_connects_without_encryption(connection_mock):
    connection_mock.connection.options["encryption"] = False
    connection_mock.simulate_resolve_hostname("localhost", ["ip1"])
    connection_mock.init_ws()
    connection_mock.assert_websocket_created(
        "ws://ip1:8563", timeout=10, skip_utf8_validation=True, enable_multithread=True
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


def test_get_websocket_connection_string(connection_mock):
    actual = connection_mock.get_websocket_connection_string("host1", "ip1", 1234)
    assert "wss://ip1:1234" == actual


def test_get_websocket_connection_string_unencrypted(connection_mock):
    connection_mock.connection.options["encryption"] = False
    actual = connection_mock.get_websocket_connection_string("host1", "ip1", 1234)
    assert "ws://ip1:1234" == actual


def test_get_websocket_connection_string_do_not_resolve_hostname(connection_mock):
    connection_mock.connection.options["resolve_hostnames"] = False
    actual = connection_mock.get_websocket_connection_string("host1", "ip1", 1234)
    assert "wss://host1:1234" == actual


def test_get_websocket_connection_string_missing_ip_address(connection_mock):
    with pytest.raises(ValueError, match="IP address was not resolved"):
        connection_mock.get_websocket_connection_string("host1", None, 1234)

@pytest.mark.parametrize("nocertcheckvalue", ["nocertcheck", "NOCERTCHECK", "NoCertcheck"])
def test_websocket_connection_no_cert_check_if_fingerprint_has_value_nocertcheck(connection_mock, dsn, certificate_type, ipaddr, port, websocket_sslopt, nocertcheckvalue):
    def build_dsn(certificate_type, ipaddr, port) -> str:
        if certificate_type == ssl.CERT_NONE:
            return os.environ.get("EXAHOST", f"{ipaddr}/{nocertcheckvalue}:{port}")
        # The host name is different for this case. As it is required to be the same
        # host name that the certificate is signed. This comes from the ITDE.
        return os.environ.get("EXAHOST", f"exasol-test-database/{nocertcheckvalue}:{port}")

    connection_mock.connection.options["resolve_hostnames"] = False
    connection_mock.connection.options['dsn'] = build_dsn(certificate_type, ipaddr, port)
    connection_mock.connection.options['websocket_sslopt'] = None
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
