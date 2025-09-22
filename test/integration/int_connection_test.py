import hashlib
import os
import ssl
from typing import Optional

import pytest

from pyexasol.connection import (
    ExaConnection,
)
from pyexasol.exceptions import ExaConnectionDsnError


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("nocertcheck", id="fingerpint=nocertcheck"),
        pytest.param("NOCERTCHECK", id="fingerpint=NOCERTCHECK"),
        pytest.param("NoCertcheck", id="fingerpint=NoCertcheck"),
        pytest.param("<actual_fingerprint>", id="fingerpint=actual_fingerprint"),
    ],
)
def fingerprint(request, dsn):
    if request.param == "<actual_fingerprint>":
        import websocket

        ws = websocket.create_connection(
            f"wss://{dsn}", sslopt={"cert_reqs": ssl.CERT_NONE}
        )
        cert = ws.sock.getpeercert(True)
        ws.close()
        return hashlib.sha256(cert).hexdigest().upper()

    return request.param


@pytest.fixture()
def dsn_builder(certificate_type, ipaddr, port, fingerprint):

    def build_dsn(custom_fingerprint: Optional[str] = None):
        fp = custom_fingerprint or fingerprint
        if certificate_type == ssl.CERT_NONE:
            return os.environ.get("EXAHOST", f"{ipaddr}/{fp}:{port}")
        # The host name is different for this case. As it is required to be the same
        # host name that the certificate is signed. This comes from the ITDE.
        return os.environ.get("EXAHOST", f"exasol-test-database/{fp}:{port}")

    return build_dsn


def test_connection(
    dsn_builder, user, password, certificate_type, ipaddr, port, fingerprint
):
    connection = ExaConnection(dsn=dsn_builder(), user=user, password=password)
    connection.execute("SELECT 1")


def test_connection_fails_with_incorrect_fingerpint(
    dsn_builder, user, password, certificate_type, ipaddr, port
):

    with pytest.raises(ExaConnectionDsnError):
        _ = ExaConnection(dsn=dsn_builder("invalid"), user=user, password=password)
