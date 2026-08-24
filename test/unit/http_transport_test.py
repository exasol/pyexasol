from unittest.mock import (
    Mock,
    patch,
)

import pytest

from pyexasol.http_transport import (
    ExaHttpThread,
    ExaHTTPTransportWrapper,
)

ERROR_MESSAGE = "Error from callback"


def export_callback(pipe, dst, **kwargs):
    raise Exception(ERROR_MESSAGE)


def import_callback(pipe, src, **kwargs):
    raise Exception(ERROR_MESSAGE)


@pytest.fixture
def mock_http_thread():
    return Mock(ExaHttpThread)


@pytest.fixture
def http_transport_wrapper_with_mocks(mock_http_thread):
    with patch.object(ExaHTTPTransportWrapper, "__init__", return_value=None):
        http_wrapper = ExaHTTPTransportWrapper(ipaddr="dummy", port=8000)
        http_wrapper.http_thread = mock_http_thread
        return http_wrapper


class TestExaHTTPTransportWrapper:
    @staticmethod
    def test_export_to_callback_fails_as_not_a_callback(
        http_transport_wrapper_with_mocks,
    ):
        with pytest.raises(ValueError, match="is not callable"):
            http_transport_wrapper_with_mocks.export_to_callback(
                callback="string", dst=None
            )

    @staticmethod
    def test_import_to_callback_fails_as_not_a_callback(
        http_transport_wrapper_with_mocks,
    ):
        with pytest.raises(ValueError, match="is not callable"):
            http_transport_wrapper_with_mocks.import_from_callback(
                callback="string", src=None
            )
