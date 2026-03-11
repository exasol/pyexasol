"""
Throughout the development the :class:`pyexasol.ExaConnection`has grown to include a
number of features. In the long-term, these will be broken it up into smaller
encapsulated units. This would allow for more explicit testing, make the code easier to
read & interpret, & help reduce difficulties associated with changes and potentially
overlooked edge cases.

To aid in a potential refactoring, it's useful to add mocked unit tests to cover
the aspects of the :class:`pyexasol.ExaConnection` that are used to define the
connection and its settings, but that these aspects themselves do not require
the connection to be made. The components tested here are prime candidates for being
broken out into separate, more manageable units.
"""

import ssl
from unittest.mock import (
    MagicMock,
    patch,
)

import pytest

from pyexasol import ExaConnection


class CustomExaConnection(ExaConnection):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


def mock_exaconnection(connection_class, **kwargs):
    """
    Within the :func:`pyexasol.ExaConnection.__init__` function, external services
    are being sent requests. To explicitly test the code around these calls, these are
    mocked.
    """

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
    with patch.multiple(connection_class, **mocks):
        return connection_class(**config)


@pytest.fixture(scope="session")
def mock_exaconnection_factory():
    def _exaconnection_fixture(**kwargs) -> ExaConnection:
        return mock_exaconnection(connection_class=ExaConnection, **kwargs)

    return _exaconnection_fixture


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
                        "cert_reqs": ssl.CERT_REQUIRED,
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

    def test_works_for_custom_child_class(self):
        mocked_connection = mock_exaconnection(CustomExaConnection)
        assert mocked_connection.options == self.expected_defaults


class TestGetWsOptions:
    """
    To create the websocket connection, specific parameters are passed in. In PyExasol,
    users may connect 1) without any verification (only for non-productive environments,
    2) with fingerprint verification, 3) with certificate verification, and 4) with
    fingerprint & certification verification. Each of these has slightly different
    defaults that have been decided upon based on Exasol's security guidelines. These
    are explicitly tested here to ensure that the defaults do not wander over time
    without explicit intention.
    """

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
    def test_verification_with_fingerprint_nocertcheck(
        mock_exaconnection_factory, recwarn
    ):
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


@pytest.mark.parametrize(
    "sql,num_columns",
    [
        ("INSERT INTO S.T VALUES (?, ?)", 2),
        ("UPDATE S.T SET name='Updated' WHERE id = ?", 1),
        ("SELECT * FROM S.T", 0),
    ],
)
def test_create_prepared_statement(mock_exaconnection_factory, sql, num_columns):
    connection = mock_exaconnection_factory()
    connection.req = MagicMock(
        return_value={
            "responseData": {
                "statementHandle": 1,
                "parameterData": {
                    "numColumns": num_columns,
                    "columns": [{}] * num_columns,
                },
                "results": [{"resultType": "rowCount", "rowCount": ...}],
            }
        }
    )

    stmt = connection.create_prepared_statement(sql)

    connection.req.assert_called_once_with(
        {
            "command": "createPreparedStatement",
            "sqlText": sql,
        }
    )
    assert stmt.query == sql
    assert stmt.statement_handle is not None
    assert stmt.parameter_data["numColumns"] == num_columns
    assert stmt.result_type == "rowCount" or stmt.result_type == "resultSet"


def test_execute_prepared(mock_exaconnection_factory):
    connection = mock_exaconnection_factory()
    connection.req = MagicMock(
        side_effect=[
            {
                "responseData": {
                    "statementHandle": 7,
                    "parameterData": {
                        "numColumns": 2,
                        "columns": [{}, {}],
                    },
                    "results": [{"resultType": "rowCount", "rowCount": 0}],
                }
            },
            {
                "responseData": {
                    "results": [
                        {
                            "resultType": "resultSet",
                            "resultSet": {
                                "resultSetHandle": 11,
                                "columns": [
                                    {"name": "ID", "dataType": {}},
                                    {"name": "NAME", "dataType": {}},
                                ],
                                "numColumns": 2,
                                "numRows": 2,
                                "numRowsInMessage": 2,
                                "data": [[0, 1], ["A", "B"]],
                            },
                        }
                    ]
                }
            },
        ]
    )

    stmt = connection.create_prepared_statement(
        "SELECT ID, NAME FROM S.T WHERE ID = ? AND NAME = ?"
    )
    stmt.execute_prepared([(0, "A"), (1, "B")])

    assert connection.req.call_args_list[1].args[0] == {
        "command": "executePreparedStatement",
        "statementHandle": 7,
        "numColumns": 2,
        "numRows": 2,
        "columns": [{}, {}],
        "data": [(0, 1), ("A", "B")],
    }
    assert stmt.result_type == "resultSet"
    assert stmt.rowcount() == 2
    assert stmt.column_names() == ["ID", "NAME"]
    assert stmt.fetchall() == [(0, "A"), (1, "B")]


def test_prepared_statement_close_closes_handles(mock_exaconnection_factory):
    connection = mock_exaconnection_factory()
    connection.req = MagicMock(
        return_value={
            "responseData": {
                "statementHandle": 7,
                "parameterData": {
                    "numColumns": 0,
                    "columns": [],
                },
                "results": [{"resultType": "rowCount", "rowCount": 0}],
            }
        }
    )

    stmt = connection.create_prepared_statement("SELECT * FROM S.T")
    stmt.result_set_handle = 99
    stmt.statement_handle = 7

    stmt.close()

    assert connection.req.call_args_list[-2].args[0] == {
        "command": "closeResultSet",
        "resultSetHandles": [99],
    }
    assert connection.req.call_args_list[-1].args[0] == {
        "command": "closePreparedStatement",
        "statementHandle": 7,
    }
    assert stmt.result_set_handle is None
    assert stmt.statement_handle is None
