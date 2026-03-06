from ssl import CERT_REQUIRED

import pytest

from pyexasol import ExaConnection


class CustomExaConnection(ExaConnection):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class TestOptions:
    """
    To handle the large number of input parameters, the initial design of
    :class:`pyexasol.ExaConnection` has all of these being put into a dictionary.
    As code is used to generate this dictionary based on the signature of
    the :class:`pyexasol.ExaConnection`, this test has been created to ensure
    that the parameters & given arguments propagate as expected for both the
    direct usage of :class:`pyexasol.ExaConnection`, as well as for its children.
    """

    expected_defaults = expected = {
        "access_token": None,
        "autocommit": True,
        "client_name": None,
        "client_os_username": None,
        "client_version": None,
        "compression": False,
        "connection_timeout": 10,
        "debug": False,
        "debug_logdir": None,
        "dsn": "localhost:8563",
        "encryption": True,
        "fetch_dict": False,
        "fetch_mapper": None,
        "fetch_size_bytes": 5242880,
        "http_proxy": None,
        "json_lib": "json",
        "lower_ident": False,
        "password": "dummy",
        "protocol_version": 3,
        "query_timeout": 0,
        "quote_ident": False,
        "refresh_token": None,
        "resolve_hostnames": True,
        "schema": "dummy",
        "snapshot_transactions": None,
        "socket_timeout": 30,
        "udf_output_bind_address": None,
        "udf_output_connect_address": None,
        "udf_output_dir": None,
        "user": "dummy",
        "verbose_error": True,
        "websocket_sslopt": None,
    }

    @pytest.mark.parametrize(
        "value_dict",
        [
            pytest.param({}, id="no_additional_value_changed"),
            pytest.param(
                {
                    "websocket_sslopt": {
                        "cert_reqs": CERT_REQUIRED,
                        "ca_certs": "rootCA.crt",
                    }
                },
                id="one_modified_value",
            ),
        ],
    )
    def test_init_sets_defaults(self, mock_exaconnection_factory, value_dict):
        expected = {**self.expected_defaults, **value_dict}

        connection = mock_exaconnection_factory(**value_dict)
        assert connection.options == expected

    def test_works_for_custom_child_class(self, mock_exaconnection_factory):
        mocked_connection = mock_exaconnection_factory(
            connection_class=CustomExaConnection
        )
        assert mocked_connection.options == self.expected_defaults
