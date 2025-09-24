import pytest

from pyexasol.http_transport import ExaHTTPTransportWrapper


@pytest.fixture
def http_transport_wrapper_with_mocks(default_ipaddr, default_port):
    return ExaHTTPTransportWrapper(ipaddr=default_ipaddr, port=default_port)


ERROR_MESSAGE = "Error from callback"


def export_callback(pipe, dst, **kwargs):
    raise Exception(ERROR_MESSAGE)


def import_callback(pipe, src, **kwargs):
    raise Exception(ERROR_MESSAGE)


class TestExaHTTPTransportWrapper:
    @staticmethod
    def test_export_to_callback_fails_at_callback(http_transport_wrapper_with_mocks):
        with pytest.raises(Exception, match=ERROR_MESSAGE):
            http_transport_wrapper_with_mocks.export_to_callback(
                callback=export_callback, dst=None
            )

    @staticmethod
    def test_import_from_callback_fails_at_callback(http_transport_wrapper_with_mocks):
        with pytest.raises(Exception, match=ERROR_MESSAGE):
            http_transport_wrapper_with_mocks.import_from_callback(
                callback=import_callback, src=None
            )
