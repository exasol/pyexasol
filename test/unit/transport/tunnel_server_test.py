import struct
from base64 import b64decode
from socketserver import BaseRequestHandler
from unittest.mock import (
    MagicMock,
    patch,
)

import pytest

from pyexasol.transport.tunnel_server import ExaTunnelServer


def exasol_reply(port: int, ipaddr: str) -> bytes:
    return struct.pack("ii16s", 0, port, ipaddr.encode())


@pytest.fixture
def make_server():
    servers = []

    def _make(**kwargs):
        # bind_and_activate=False skips server_bind, which needs a real Exasol node
        server = ExaTunnelServer(
            ("127.0.0.1", 0), BaseRequestHandler, bind_and_activate=False, **kwargs
        )
        servers.append(server)
        return server

    yield _make
    for server in servers:
        server.socket.close()


@pytest.fixture
def fake_socket():
    sock = MagicMock()
    sock.recv.return_value = exasol_reply(port=4242, ipaddr="10.0.0.7")
    return sock


class TestServerBind:
    @staticmethod
    def test_sends_tunnel_handshake_to_exasol(make_server, fake_socket):
        server = make_server(encryption=False)
        server.socket = fake_socket

        server.server_bind()

        fake_socket.connect.assert_called_once_with(server.server_address)
        fake_socket.sendall.assert_called_once_with(
            struct.pack("iii", 0x02212102, 1, 1)
        )

    @staticmethod
    def test_without_encryption_exposes_exasol_address_and_no_public_key(
        make_server, fake_socket
    ):
        server = make_server(encryption=False)
        server.socket = fake_socket

        server.server_bind()

        assert server.exa_address_ipaddr == "10.0.0.7"
        assert server.exa_address_port == 4242
        assert server.socket is fake_socket
        assert server.exa_address_public_key is None

    @staticmethod
    def test_with_encryption_exposes_exasol_address_and_public_key(
        make_server, fake_socket
    ):
        server = make_server(encryption=True)
        server.socket = fake_socket
        context = MagicMock()

        with patch.object(
            ExaTunnelServer,
            "generate_adhoc_ssl_context",
            return_value=(context, "fingerprint"),
        ):
            server.server_bind()

        assert server.exa_address_ipaddr == "10.0.0.7"
        assert server.exa_address_port == 4242
        context.wrap_socket.assert_called_once_with(
            fake_socket, server_side=True, do_handshake_on_connect=False
        )
        assert server.socket is context.wrap_socket.return_value
        assert server.exa_address_public_key == "fingerprint"


class TestRequestAdapters:
    """The tunnel socket is already connected, so it is reused as the request."""

    @staticmethod
    def test_request_is_not_closed_between_requests(make_server):
        server = make_server()
        request = MagicMock()

        server.shutdown_request(request)
        server.close_request(request)

        assert request.method_calls == []


class TestAdhocSslContext:
    @staticmethod
    def test_returns_context_and_sha256_fingerprint():
        context, fingerprint = ExaTunnelServer.generate_adhoc_ssl_context()

        assert context is not None
        assert len(b64decode(fingerprint)) == 32  # SHA-256 digest
