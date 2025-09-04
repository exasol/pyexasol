import hashlib
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


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("nocertcheck", id="fingerpint=nocertcheck"),
        pytest.param("NOCERTCHECK", id="fingerpint=NOCERTCHECK"),
        pytest.param("NoCertcheck", id="fingerpint=NoCertcheck"),
        pytest.param("<actual_fingerprint>", id="fingerpint=actual_fingerprint"),
    ],
)
def fingerprint(request, ipaddr, port):
    if request.param == "<actual_fingerprint>":
        import websocket

        ws = websocket.create_connection(
            f"wss://exasol-test-database:{port}", sslopt={"cert_reqs": ssl.CERT_NONE}
        )
        cert = ws.sock.getpeercert(True)
        ws.close()
        return hashlib.sha256(cert).hexdigest().upper()

    return request.param


@pytest.fixture(scope="session")
def dsn(certificate_type, ipaddr, port, fingerprint):
    if certificate_type == ssl.CERT_NONE:
        return os.environ.get("EXAHOST", f"{ipaddr}/{fingerprint}:{port}")
    # The host name is different for this case. As it is required to be the same
    # host name that the certificate is signed. This comes from the ITDE.
    return os.environ.get("EXAHOST", f"exasol-test-database/{fingerprint}:{port}")


def test_connection(dsn, user, password, certificate_type, ipaddr, port, fingerprint):
    connection = ExaConnection(dsn=dsn, user=user, password=password)
    connection.execute("SELECT 1")


def test_connection_fails_with_incorrect_fingerpint(
    dsn, user, password, certificate_type, ipaddr, port
):
    def build_dsn(certificate_type, ipaddr, port):
        if certificate_type == ssl.CERT_NONE:
            return os.environ.get("EXAHOST", f"{ipaddr}/invalid:{port}")
        # The host name is different for this case. As it is required to be the same
        # host name that the certificate is signed. This comes from the ITDE.
        return os.environ.get("EXAHOST", f"exasol-test-database/invalid:{port}")

    with pytest.raises(ExaConnectionDsnError):
        _ = ExaConnection(
            dsn=build_dsn(certificate_type, ipaddr, port), user=user, password=password
        )
