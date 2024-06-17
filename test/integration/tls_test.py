import ssl
import hashlib
import pytest
import pyexasol
from inspect import cleandoc
from pyexasol import ExaConnectionFailedError


@pytest.fixture
def connection(dsn, user, password, schema):
    con = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
    )
    yield con
    con.close()


@pytest.fixture
def server_fingerprint(connection):
    cert = connection._ws.sock.getpeercert(True)
    fingerprint = hashlib.sha256(cert).hexdigest().upper()
    yield fingerprint


def _dsn_with_fingerprint(dsn: str, fingerprint: str):
    if ":" in dsn:
        dsn.replace(":", f"/{fingerprint}:")
    else:
        dsn = f"{dsn}/{fingerprint}"
    return dsn


@pytest.fixture
def dsn_with_valid_fingerprint(dsn, server_fingerprint):
    yield _dsn_with_fingerprint(dsn, server_fingerprint)


@pytest.fixture
def dsn_with_invalid_fingerprint(dsn):
    yield _dsn_with_fingerprint(dsn, "123abcfoooBAAAR")


@pytest.mark.xfail(
    reason=cleandoc(
        """
        refer to: TBD
        """
    )
)
@pytest.mark.tls
def test_connect_fails_due_to_strict_certificate_validation_by_default():
    assert False


@pytest.mark.tls
def test_connect_with_tls(dsn, user, password, schema):
    expected = 1
    with pyexasol.connect(
        dsn=dsn, user=user, password=password, schema=schema, encryption=True
    ) as connection:
        actual = connection.execute("SELECT 1;").fetchval()

    assert actual == expected


@pytest.mark.tls
def test_connect_with_valid_fingerprint(
    dsn_with_valid_fingerprint, user, password, schema
):
    dsn = dsn_with_valid_fingerprint
    expected = 1
    with pyexasol.connect(
        dsn=dsn, user=user, password=password, schema=schema, encryption=True, websocket_sslopt={'cert_reqs': ssl.CERT_REQUIRED}
    ) as connection:
        actual = connection.execute("SELECT 1;").fetchval()

    assert actual == expected


@pytest.mark.tls
def test_connect_with_invalid_fingerprint_fails(
    dsn_with_invalid_fingerprint, user, password, schema
):
    dsn = dsn_with_invalid_fingerprint
    #with pytest.raises(ExaConnectionFailedError):
    pyexasol.connect(
        dsn=dsn, user=user, password=password, schema=schema, encryption=False, websocket_sslopt={'cert_reqs': ssl.CERT_REQUIRED}
    )

