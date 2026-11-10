from unittest.mock import (
    MagicMock,
    patch,
)

import pytest
from packaging.version import Version
from pydantic import ValidationError

from pyexasol.connection import ExaConnection
from pyexasol.database_versions import MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT


@pytest.fixture
def mock_sql_thread():
    """Mock ExaSQLThread instances used by export callbacks."""
    with patch("pyexasol.connection.ExaSQLThread") as mock_cls:
        yield mock_cls


@pytest.fixture
def exa_conn():
    """
    Create a mock ExaConnection. We use a real instance but mock
    attributes to avoid actual network/socket initialization.
    """

    def mock_format_logic(query_or_table):
        return query_or_table

    conn = MagicMock(spec=ExaConnection)
    conn.options = {"compression": True, "encryption": True}
    conn.ws_ipaddr = "127.0.0.1"
    conn.ws_port = 8563
    conn.format = MagicMock()
    conn.format.format.side_effect = mock_format_logic

    # Attach the actual methods to the mock instance
    conn.export_to_callback = ExaConnection.export_to_callback.__get__(conn)
    conn.import_from_callback = ExaConnection.import_from_callback.__get__(conn)
    return conn


@pytest.fixture
def callback_spy():
    """Create a callback function with the additional benefits of a mock"""

    def callback_logic(pipe, src, **kwargs):
        pipe.write(b"data")
        return "success_marker"

    return MagicMock(side_effect=callback_logic)


class TestExportToCallback:
    @staticmethod
    def test_not_a_callable_raises_an_exception(
        exa_conn, mock_http_thread, mock_sql_thread
    ):
        with pytest.raises(TypeError) as ex:
            exa_conn.export_to_callback(
                callback="not_a_function", dst=None, query_or_table="dummy_table"
            )

        assert mock_http_thread.call_count == 0
        assert mock_sql_thread.call_count == 0
        assert str(ex.value) == (
            "`callback` must be callable. " "Received: 'not_a_function' (type: str)"
        )

    @staticmethod
    def test_set_defaults_as_expected(
        exa_conn,
        mock_http_thread,
        mock_sql_thread,
        callback_spy,
    ):

        result = exa_conn.export_to_callback(
            callback=callback_spy, dst=None, query_or_table="dummy_table"
        )

        mock_http_thread.return_value.start.assert_called_once()
        mock_sql_thread.return_value.start.assert_called_once()
        assert result == "success_marker"

        # verify compression set as expected when export_params=None, then
        # this is set to self.options["compression"]
        _, http_kwargs = mock_http_thread.call_args
        assert http_kwargs["compression"] is exa_conn.options["compression"]

        _, sql_kwargs = mock_sql_thread.call_args
        assert sql_kwargs["query_builder"].query_or_table == "dummy_table"
        assert sql_kwargs["worker_finished_event"].is_set()

        # verify callback_params=None maps to empty dictionary
        _, callback_kwargs = callback_spy.call_args
        assert callback_kwargs == {}

    @staticmethod
    def test_rejects_multiple_export_parameter_errors_before_starting_threads(
        exa_conn,
        mock_http_thread,
        mock_sql_thread,
        callback_spy,
    ):
        with pytest.raises(ValidationError) as exception:
            exa_conn.export_to_callback(
                callback=callback_spy,
                dst=None,
                query_or_table="dummy_table",
                export_params={"format": "invalid", "delimit": "invalid"},
            )

        assert len(exception.value.errors()) >= 2
        assert mock_http_thread.call_count == 0
        assert mock_sql_thread.call_count == 0


class TestImportFromCallback:
    @staticmethod
    def test_not_a_callable_raises_an_exception(
        exa_conn, mock_http_thread, mock_sql_import_thread
    ):
        with pytest.raises(TypeError) as ex:
            exa_conn.import_from_callback(
                callback="not_a_function", src="src_data", table="dummy_table"
            )

        assert mock_http_thread.call_count == 0
        assert mock_sql_import_thread.call_count == 0
        assert str(ex.value) == (
            "`callback` must be callable. " "Received: 'not_a_function' (type: str)"
        )

    @staticmethod
    def test_set_defaults_as_expected(
        exa_conn,
        mock_http_thread,
        mock_sql_import_thread,
        callback_spy,
    ):
        result = exa_conn.import_from_callback(
            callback=callback_spy, src="src_data", table="dummy_table"
        )

        mock_http_thread.return_value.start.assert_called_once()
        mock_sql_import_thread.return_value.start.assert_called_once()
        assert result == "success_marker"

        # verify compression set as expected when import_params=None, then
        # this is set to self.options["compression"]
        _, http_kwargs = mock_http_thread.call_args
        assert http_kwargs["compression"] is exa_conn.options["compression"]
        assert http_kwargs["worker_finished_event"].is_set()

        # verify the import builder receives default parameters
        _, sql_kwargs = mock_sql_import_thread.call_args
        assert sql_kwargs["query_builder"].table == "dummy_table"
        assert sql_kwargs["worker_finished_event"].is_set()

        # verify callback_params=None maps to empty dictionary
        _, callback_kwargs = callback_spy.call_args
        assert callback_kwargs == {}

    @staticmethod
    def test_passes_schema_qualified_table_to_import_thread(
        exa_conn,
        mock_http_thread,
        mock_sql_import_thread,
        callback_spy,
    ):
        table = ("SCHEMA", "TABLE")

        exa_conn.import_from_callback(
            callback=callback_spy, src="src_data", table=table
        )

        _, sql_kwargs = mock_sql_import_thread.call_args
        assert sql_kwargs["query_builder"].table == table


class TestImportFromParquetPathSelection:
    NATIVE_VERSION = MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT.version

    @staticmethod
    @pytest.fixture
    def parquet_file(tmp_path):
        parquet_file = tmp_path / "data.parquet"
        parquet_file.write_bytes(b"PAR1")
        return parquet_file

    @staticmethod
    @pytest.fixture
    def conn(exa_conn):
        exa_conn.exasol_db_version = TestImportFromParquetPathSelection.NATIVE_VERSION
        exa_conn.logger = MagicMock()
        exa_conn._select_native_parquet_files = (
            ExaConnection._select_native_parquet_files.__get__(exa_conn)
        )
        exa_conn.import_from_callback = MagicMock(return_value="legacy_result")
        exa_conn._import_from_native_parquet = MagicMock(return_value="native_result")
        return exa_conn

    @staticmethod
    def _import(conn, source, callback_params=None, import_params=None):
        return ExaConnection.import_from_parquet(
            conn,
            source=source,
            table="TARGET_TABLE",
            callback_params=callback_params,
            import_params=import_params,
        )

    @staticmethod
    def test_uses_native_import_for_one_file(conn, parquet_file):
        result = TestImportFromParquetPathSelection._import(conn, parquet_file)

        assert result == "native_result"
        conn.import_from_callback.assert_not_called()
        conn._import_from_native_parquet.assert_called_once_with(
            [parquet_file], "TARGET_TABLE", import_params=None
        )

    @staticmethod
    def test_uses_native_import_for_several_files(conn, tmp_path):
        files = [tmp_path / f"{index}.parquet" for index in range(2)]
        for file in files:
            file.write_bytes(b"PAR1")

        TestImportFromParquetPathSelection._import(conn, tmp_path)

        conn._import_from_native_parquet.assert_called_once_with(
            files, "TARGET_TABLE", import_params=None
        )

    @staticmethod
    def test_uses_native_import_for_native_import_params(conn, parquet_file):
        import_params = {"MaxRows": 10}

        TestImportFromParquetPathSelection._import(
            conn, parquet_file, import_params=import_params
        )

        conn._import_from_native_parquet.assert_called_once_with(
            [parquet_file], "TARGET_TABLE", import_params=import_params
        )

    @staticmethod
    def test_uses_callback_import_before_native_version(conn, parquet_file):
        conn.exasol_db_version = Version("2025.2.0")

        result = TestImportFromParquetPathSelection._import(conn, parquet_file)

        assert result == "legacy_result"
        conn._import_from_native_parquet.assert_not_called()

    @staticmethod
    @pytest.mark.parametrize(
        "callback_params, import_params",
        [
            pytest.param({"columns": ["FIRST"]}, None, id="callback_params"),
            pytest.param(None, {"columns": ["FIRST"]}, id="legacy_import_params"),
        ],
    )
    def test_uses_callback_import_for_unsupported_params(
        conn, parquet_file, callback_params, import_params
    ):
        result = TestImportFromParquetPathSelection._import(
            conn, parquet_file, callback_params, import_params
        )

        assert result == "legacy_result"
        conn._import_from_native_parquet.assert_not_called()
        conn.logger.debug.assert_called_once()

    @staticmethod
    def test_uses_callback_import_when_no_file_matches(conn, tmp_path):
        result = TestImportFromParquetPathSelection._import(conn, tmp_path)

        assert result == "legacy_result"
        conn._import_from_native_parquet.assert_not_called()
