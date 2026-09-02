from unittest.mock import (
    MagicMock,
    patch,
)

import pytest
from packaging.version import Version
from pydantic import ValidationError

from pyexasol import callback as callback_module
from pyexasol.connection import ExaConnection
from pyexasol.database_versions import MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT


@pytest.fixture
def mock_http_thread():
    """Patch ExaHttpThread where ExaConnection looks it up."""
    with patch("pyexasol.connection.ExaHttpThread") as mock_cls:
        instance = mock_cls.return_value
        instance.write_pipe = MagicMock()
        instance.write_pipe.__enter__.return_value = MagicMock(spec=["write"])

        def construct_http_thread(*args, **kwargs):
            worker_finished_event = kwargs.get("worker_finished_event")
            if worker_finished_event is not None:
                # The real HTTP thread signals this event when it finishes.
                worker_finished_event.set()
            return instance

        mock_cls.side_effect = construct_http_thread
        yield mock_cls


@pytest.fixture
def mock_sql_import_thread():
    """Mock ExaSQLThread instances used by import callbacks."""
    with patch("pyexasol.connection.ExaSQLThread") as mock_cls:
        yield mock_cls


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


class TestImportFromParquetVersionSelection:
    @staticmethod
    @pytest.mark.parametrize(
        "database_version, use_native",
        [
            (Version("2025.2.0"), False),
            (MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT.version, True),
            (Version("2026.1.1"), True),
        ],
    )
    def test_selects_import_path_by_database_version(
        exa_conn, database_version, use_native
    ):
        source = "data.parquet"
        table = "TARGET_TABLE"
        callback_params = {"columns": ["FIRST"]}
        import_params = {"columns": ["FIRST"]}
        exa_conn.exasol_db_version = database_version
        exa_conn.import_from_callback = MagicMock(return_value="legacy_result")
        exa_conn._import_from_native_parquet = MagicMock(return_value="native_result")

        result = ExaConnection.import_from_parquet(
            exa_conn,
            source=source,
            table=table,
            callback_params=callback_params,
            import_params=import_params,
        )
        runner = exa_conn._import_from_native_parquet

        if use_native:
            assert result == "native_result"
            exa_conn.import_from_callback.assert_not_called()
            runner.assert_called_once()
            assert runner.call_args.args == (source, table)
            assert runner.call_args.kwargs == {"import_params": import_params}
        else:
            assert result == "legacy_result"
            exa_conn.import_from_callback.assert_called_once_with(
                callback_module.import_from_parquet,
                source,
                table,
                callback_params,
                import_params,
            )

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
