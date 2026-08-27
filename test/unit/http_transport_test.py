import threading
from importlib import import_module
from unittest.mock import (
    Mock,
    patch,
)

import pytest
from packaging.version import Version

from pyexasol import (
    ExaConnection,
    ExaFormatter,
)
from pyexasol.http_transport import (
    ExaHttpThread,
    ExaHTTPTransportWrapper,
    ExaSQLThread,
    ExportQuery,
    ImportQuery,
    SqlQuery,
)
from pyexasol.query_builders.common_formattings import (
    MIN_DATABASE_VERSION_FOR_TLS_PUBLIC_KEY,
    TransportEndpoint,
)
from pyexasol.query_builders.csv.clause_formatter import ClauseFormatter

http_transport_module = import_module("pyexasol.http_transport")


@pytest.fixture
def mock_connection():
    mock = Mock(ExaConnection)
    mock.options = {"encryption": True, "quote_ident": "'"}
    mock.exasol_db_version = MIN_DATABASE_VERSION_FOR_TLS_PUBLIC_KEY
    mock.format = ExaFormatter(connection=mock)
    return mock


@pytest.fixture
def sql_query(mock_connection):
    return SqlQuery(connection=mock_connection, compression=True)


@pytest.fixture
def import_sql_query(mock_connection):
    return ImportQuery(connection=mock_connection, compression=True)


@pytest.fixture
def export_sql_query(mock_connection):
    return ExportQuery(connection=mock_connection, compression=True)


class TestSqlQuery:
    @staticmethod
    @pytest.mark.parametrize(
        "db_version,expected_end",
        [
            pytest.param(Version("7.1.19"), "FILE '000.gz'", id="lower_version"),
            pytest.param(
                MIN_DATABASE_VERSION_FOR_TLS_PUBLIC_KEY,
                "PUBLIC KEY 'sha256//YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o=' FILE '000.gz'",
                id="greater_than_or_equal_version",
            ),
        ],
    )
    def test_get_file_list(mock_connection, sql_query, db_version, expected_end):
        mock_connection.exasol_db_version = db_version
        exa_address_list = [
            "127.18.0.2:8364/YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o="
        ]

        clause_formatter = ClauseFormatter(mock_connection.format)
        transport_endpoint = TransportEndpoint(
            database_version=mock_connection.exasol_db_version,
            encryption=mock_connection.options["encryption"],
        )
        result = clause_formatter.file_clauses(
            transport_endpoint=transport_endpoint,
            exa_address_list=exa_address_list,
            file_ext="gz",
            csv_cols=None,
        )

        assert result == [f"AT 'https://127.18.0.2:8364' {expected_end}"]


class TestImportQuery:
    @staticmethod
    def test_build_query(import_sql_query, mock_connection):
        result = import_sql_query.build_query(
            table="TABLE",
            exa_address_list=[
                "127.18.0.2:8364/YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o="
            ],
        )
        assert (
            result
            == "IMPORT INTO TABLE FROM CSV\nAT 'https://127.18.0.2:8364' PUBLIC KEY 'sha256//YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o=' FILE '000.gz'"
        )

    @staticmethod
    def test_load_from_dict(mock_connection):
        import_query = ImportQuery.load_from_dict(
            connection=mock_connection, compression=False, params={"skip": 2}
        )
        import_query.skip = 3
        mock_connection.options["encryption"] = False

        query = import_query.build_query("TABLE", ["127.18.0.2:8364"])

        assert "SKIP = 3" in query

    @staticmethod
    def test_load_from_dict_uses_mutated_columns(mock_connection):
        mock_connection.options["encryption"] = False
        import_query = ImportQuery.load_from_dict(
            connection=mock_connection,
            compression=False,
            params={"columns": ["FIRST"]},
        )
        import_query.columns = ["SECOND"]

        query = import_query.build_query("TABLE", ["127.18.0.2:8364"])

        assert 'IMPORT INTO TABLE("SECOND") FROM CSV' in query

    @staticmethod
    def test_build_query_can_be_called_repeatedly_with_columns(mock_connection):
        mock_connection.options["encryption"] = False
        import_query = ImportQuery.load_from_dict(
            connection=mock_connection,
            compression=False,
            params={"columns": ["FIRST", "SECOND"]},
        )
        exa_address_list = ["127.18.0.2:8364"]

        first_query = import_query.build_query("TABLE", exa_address_list)
        second_query = import_query.build_query("TABLE", exa_address_list)

        assert 'IMPORT INTO TABLE("FIRST","SECOND") FROM CSV' in first_query
        assert second_query == first_query


class TestExportQuery:
    @staticmethod
    def test_build_query(export_sql_query, mock_connection):
        result = export_sql_query.build_query(
            table="TABLE",
            exa_address_list=[
                "127.18.0.2:8364/YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o="
            ],
        )
        assert (
            result
            == "EXPORT TABLE INTO CSV\nAT 'https://127.18.0.2:8364' PUBLIC KEY 'sha256//YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o=' FILE '000.gz'"
        )

    #
    @staticmethod
    def test_load_from_dict(mock_connection):
        export_query = ExportQuery.load_from_dict(
            connection=mock_connection, compression=False, params={"delimit": "auto"}
        )
        assert export_query.delimit == "auto"

    @staticmethod
    def test_load_from_dict_uses_mutated_columns(mock_connection):
        mock_connection.options["encryption"] = False
        export_query = ExportQuery.load_from_dict(
            connection=mock_connection,
            compression=False,
            params={"columns": ["FIRST"]},
        )
        export_query.columns = ["SECOND"]

        query = export_query.build_query("TABLE", ["127.18.0.2:8364"])

        assert 'EXPORT TABLE("SECOND") INTO CSV' in query

    @staticmethod
    def test_load_from_dict_rejects_unsupported_parameter(mock_connection):
        with pytest.raises(
            TypeError, match="unexpected keyword argument 'unsupported'"
        ):
            ExportQuery.load_from_dict(
                connection=mock_connection,
                compression=False,
                params={"unsupported": True},
            )

    @staticmethod
    def test_build_query_can_be_called_repeatedly_with_columns(mock_connection):
        mock_connection.options["encryption"] = False
        export_query = ExportQuery.load_from_dict(
            connection=mock_connection,
            compression=False,
            params={"columns": ("FIRST", "SECOND")},
        )
        exa_address_list = ["127.18.0.2:8364"]

        first_query = export_query.build_query("TABLE", exa_address_list)
        second_query = export_query.build_query("TABLE", exa_address_list)

        assert 'EXPORT TABLE("FIRST","SECOND") INTO CSV' in first_query
        assert second_query == first_query


ERROR_MESSAGE = "Error from callback"


def export_callback(pipe, dst, **kwargs):
    raise Exception(ERROR_MESSAGE)


def import_callback(pipe, src, **kwargs):
    raise Exception(ERROR_MESSAGE)


class TestImportTransportThreadLifecycle:
    @staticmethod
    def test_sql_thread_signals_worker_finished_event_after_successful_query():
        worker_finished_event = threading.Event()

        class SuccessfulSQLThread(ExaSQLThread):
            def run_sql(self):
                pass

        http_thread = Mock()
        thread = SuccessfulSQLThread(
            connection=Mock(),
            compression=False,
            worker_finished_event=worker_finished_event,
        )
        thread.set_http_thread(http_thread)

        thread.run()

        assert thread.exc is None
        assert worker_finished_event.is_set()
        http_thread.terminate.assert_not_called()

    @staticmethod
    def test_sql_thread_terminates_http_and_signals_worker_finished_event_on_failure():
        worker_finished_event = threading.Event()
        expected_error = RuntimeError("SQL query failed")

        class FailingSQLThread(ExaSQLThread):
            def run_sql(self):
                raise expected_error

        http_thread = Mock()
        thread = FailingSQLThread(
            connection=Mock(),
            compression=False,
            worker_finished_event=worker_finished_event,
        )
        thread.set_http_thread(http_thread)

        thread.run()

        assert thread.exc is expected_error
        assert worker_finished_event.is_set()
        http_thread.terminate.assert_called_once_with()

    @staticmethod
    def test_http_thread_closes_server_and_signals_worker_finished_event_after_success():
        worker_finished_event = threading.Event()
        server = Mock(
            total_clients=0,
            is_terminated=False,
            read_pipe=Mock(),
            write_pipe=Mock(),
        )
        server.can_finish_get = Mock()

        def handle_request():
            server.total_clients = 1

        server.handle_request.side_effect = handle_request

        with patch.object(http_transport_module, "ExaTCPServer", return_value=server):
            thread = ExaHttpThread(
                "127.0.0.1",
                8563,
                False,
                False,
                worker_finished_event=worker_finished_event,
            )
            thread.run()

        assert thread.exc is None
        server.handle_request.assert_called_once_with()
        server.server_close.assert_called_once_with()
        assert worker_finished_event.is_set()

    @staticmethod
    def test_http_thread_closes_server_and_signals_worker_finished_event_after_failure():
        worker_finished_event = threading.Event()
        expected_error = BrokenPipeError("HTTP request failed")
        server = Mock(
            total_clients=0,
            is_terminated=False,
            read_pipe=Mock(),
            write_pipe=Mock(),
        )
        server.can_finish_get = Mock()
        server.handle_request.side_effect = expected_error

        with patch.object(http_transport_module, "ExaTCPServer", return_value=server):
            thread = ExaHttpThread(
                "127.0.0.1",
                8563,
                False,
                False,
                worker_finished_event=worker_finished_event,
            )
            thread.run()

        assert thread.exc is expected_error
        server.server_close.assert_called_once_with()
        assert worker_finished_event.is_set()


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
