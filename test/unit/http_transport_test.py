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
)

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
        "columns,expected",
        [(None, ""), ([], ""), (["LASTNAME", "FIRSTNAME"], '("LASTNAME","FIRSTNAME")')],
    )
    def test_column_spec(sql_query, columns, expected):
        sql_query.columns = columns
        assert sql_query._column_spec == expected

    @staticmethod
    @pytest.mark.parametrize(
        "csv_cols,expected",
        [
            pytest.param(None, "", id="none_specified"),
            pytest.param([], "", id="empty_iterable_specified"),
            pytest.param(["1..3"], "(1..3)", id="col_gap_specified"),
            pytest.param(["123"], "(123)", id="col_without_spaces"),
            pytest.param(
                ["1..3", "4 FORMAT='DD-MM-YYYY'"],
                "(1..3,4 FORMAT='DD-MM-YYYY')",
                id="multi_specifier_with_format",
            ),
        ],
    )
    def test_build_csv_cols(sql_query, csv_cols: list[str] | None, expected: str):
        sql_query.csv_cols = csv_cols
        assert sql_query._build_csv_cols() == expected

    @staticmethod
    def test_build_csv_cols_raises_exception(sql_query):
        sql_query.csv_cols = ["1.2"]
        with pytest.raises(ValueError, match="is not a safe csv_cols part"):
            sql_query._build_csv_cols()

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

        result = sql_query._get_file_list(exa_address_list)

        assert result == [f"AT 'https://127.18.0.2:8364' {expected_end}"]

    @staticmethod
    def test_get_query_str():
        query_lines = [None, "test", None, "this"]
        assert SqlQuery._get_query_str(query_lines) == "test\nthis"

    @staticmethod
    @pytest.mark.parametrize(
        "compression,file_ext,expected",
        [
            pytest.param(True, None, "gz", id="compressed_defaults_to_format_gz"),
            pytest.param(False, None, "csv", id="uncompressed_defaults_to_format_csv"),
            pytest.param(True, "gz", "gz", id="format_gz_accepted"),
        ],
    )
    def test_file_ext(
        sql_query, compression: bool, file_ext: str | None, expected: str
    ):
        sql_query.compression = compression
        sql_query.format = file_ext
        assert sql_query._file_ext == expected

    @staticmethod
    def test_file_ext_raises_exception(sql_query):
        sql_query.format = "not_a_valid_format"
        with pytest.raises(ValueError, match=f"'format' {sql_query.format} not in"):
            sql_query._file_ext


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
        ImportQuery.load_from_dict(
            connection=mock_connection, compression=False, params={"skip": 2}
        )

    @staticmethod
    @pytest.mark.parametrize(
        "columns,expected",
        [
            (
                ["LASTNAME", "FIRSTNAME"],
                'IMPORT INTO TABLE("LASTNAME","FIRSTNAME") FROM CSV',
            ),
            (None, "IMPORT INTO TABLE FROM CSV"),
        ],
    )
    def test_get_import(import_sql_query, columns, expected):
        import_sql_query.columns = columns
        result = import_sql_query._get_import(table="TABLE")
        assert result == expected


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
        assert export_query._export_builder is not None
        assert export_query._export_builder.delimit == "AUTO"

    @staticmethod
    def test_load_from_dict_rejects_unsupported_parameter(mock_connection):
        with pytest.raises(ValueError, match="Extra inputs are not permitted"):
            ExportQuery.load_from_dict(
                connection=mock_connection,
                compression=False,
                params={"unsupported": True},
            )

    @staticmethod
    @pytest.mark.parametrize(
        "columns,expected",
        [
            (
                ["LASTNAME", "FIRSTNAME"],
                'EXPORT TABLE("LASTNAME","FIRSTNAME") INTO CSV',
            ),
            (None, "EXPORT TABLE INTO CSV"),
        ],
    )
    def test_get_export(export_sql_query, columns, expected):
        export_sql_query.columns = columns
        result = export_sql_query._get_export(table="TABLE")
        assert result == expected

    @staticmethod
    @pytest.mark.parametrize(
        "value,expected",
        [(True, "WITH COLUMN NAMES"), (False, None)],
    )
    def test_with_column_names(export_sql_query, value, expected):
        export_sql_query.with_column_names = value
        assert export_sql_query._with_column_names == expected

    @staticmethod
    @pytest.mark.parametrize("value", ["False", "true", "abc", 1, 0])
    def test_with_column_names_wrong_value_raises_exception(export_sql_query, value):
        export_sql_query.with_column_names = value
        with pytest.raises(
            ValueError, match="Invalid value for export parameter WITH_COLUMNS"
        ):
            _ = export_sql_query._with_column_names


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
