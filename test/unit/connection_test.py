import ssl
from unittest.mock import (
    MagicMock,
    patch,
)

import pytest

from pyexasol import ExaConnection


@pytest.fixture(scope="session")
def mock_exaconnection_factory():
    def _exaconnection_fixture(**kwargs) -> ExaConnection:
        defaults = {
            "dsn": "localhost:8563",
            "user": "dummy",
            "password": "dummy",
            "schema": "dummy",
        }
        config = {**defaults, **kwargs}

        default_mock = MagicMock(return_value=None)
        mocks = {
            "_init_ws": default_mock,
            "_login": default_mock,
            "get_attr": default_mock,
        }
        with patch.multiple(ExaConnection, **mocks):
            return ExaConnection(**config)

    return _exaconnection_fixture


class TestGetWsOptions:
    @staticmethod
    def test_no_verification(mock_exaconnection_factory, recwarn):
        connection = mock_exaconnection_factory()
        result = connection._get_ws_options(fingerprint=None)

        assert result == {
            "timeout": 10,
            "skip_utf8_validation": True,
            "enable_multithread": True,
            "sslopt": {"cert_reqs": ssl.CERT_REQUIRED},
        }
        assert len(recwarn.list) == 1
        assert "From PyExasol version ``1.0.0``," in str(recwarn.list[0].message)

    @staticmethod
    def test_verification_with_fingerprint(mock_exaconnection_factory, recwarn):
        fingerprint = "7BBBF74F1F2B993BB81FF5F795BCA2340CC697B8DEFEB768DD6BABDF13FB2F05"
        dsn = f"localhost/{fingerprint}:8563"

        connection = mock_exaconnection_factory(dsn=dsn)
        result = connection._get_ws_options(fingerprint=fingerprint)

        assert result == {
            "timeout": 10,
            "skip_utf8_validation": True,
            "enable_multithread": True,
            "sslopt": {"cert_reqs": ssl.CERT_NONE},
        }
        assert len(recwarn.list) == 0

    @staticmethod
    def test_verification_with_certificate(mock_exaconnection_factory, recwarn):
        # if websocket_sslopt is defined, like here, this is propagated as is without
        # any checks for this function

        websocket_sslopt = {"cert_reqs": ssl.CERT_REQUIRED, "ca_certs": "rootCA.crt"}

        connection = mock_exaconnection_factory(websocket_sslopt=websocket_sslopt)
        result = connection._get_ws_options(fingerprint=None)

        assert result == {
            "timeout": 10,
            "skip_utf8_validation": True,
            "enable_multithread": True,
            "sslopt": websocket_sslopt,
        }
        assert len(recwarn.list) == 0

    @staticmethod
    def test_verification_with_fingerprint_nocertcheck(mock_exaconnection_factory, recwarn):
        fingerprint = "nocertcheck"
        dsn = f"localhost/{fingerprint}:8563"

        connection = mock_exaconnection_factory(dsn=dsn)
        result = connection._get_ws_options(fingerprint=fingerprint)

        assert result == {
            "timeout": 10,
            "skip_utf8_validation": True,
            "enable_multithread": True,
            "sslopt": {"cert_reqs": ssl.CERT_NONE},
        }
        assert len(recwarn.list) == 0
