import time
from unittest.mock import patch

import pytest

from pyexasol.exceptions import (
    ExaCommunicationError,
    ExaExportError,
    ExaQueryError,
    ExaRuntimeError,
)
from pyexasol.http_transport import (
    ExaHttpRequestHandler,
    ExaSQLExportThread,
    ExaTCPServer,
)


@pytest.fixture
def output_filepath(tmp_path):
    return tmp_path / "test_output.csv"


@pytest.fixture
def export_cb():
    """Provides a standard custom export callback function."""

    def _callback(pipe, dst, **kwargs):
        dst.write_bytes(pipe.read())

    return _callback


@pytest.mark.etl
class TestExportParams:
    @staticmethod
    def test_with_no_params(connection, fill_table, output_filepath, all_data):
        connection.export_to_file(dst=output_filepath, query_or_table=fill_table)

        assert output_filepath.read_text() == all_data.csv_str()

    @staticmethod
    def test_csv_cols(connection, fill_table, output_filepath, all_data):
        params = {"csv_cols": ["1..7"]}

        connection.export_to_file(
            dst=output_filepath, query_or_table=fill_table, export_params=params
        )

        assert output_filepath.read_text() == all_data.csv_str()

    @staticmethod
    def test_delimit(connection, fill_table, output_filepath, all_data):
        params = {"delimit": "AUTO"}

        connection.export_to_file(
            dst=output_filepath, query_or_table=fill_table, export_params=params
        )

        assert output_filepath.read_text() == all_data.csv_str()

    @staticmethod
    def test_without_column_names(connection, fill_table, output_filepath, all_data):
        connection.export_to_file(dst=output_filepath, query_or_table=fill_table)

        assert output_filepath.read_text() == all_data.csv_str()

    @staticmethod
    def test_with_column_names(connection, fill_table, output_filepath, all_data):
        params = {"with_column_names": True}

        connection.export_to_file(
            dst=output_filepath, query_or_table=fill_table, export_params=params
        )

        expected_header = ",".join(all_data.columns) + "\n"
        assert output_filepath.read_text() == expected_header + all_data.csv_str()


@pytest.mark.etl
class TestExportGeneral:
    @staticmethod
    def test_without_resolving_hostname(
        connection_without_resolving_hostnames,
        fill_table,
        output_filepath,
        all_data,
    ):
        connection_without_resolving_hostnames.export_to_file(
            dst=output_filepath, query_or_table=fill_table
        )

        assert output_filepath.read_text() == all_data.csv_str()

    @staticmethod
    def test_custom_export_callback(
        connection, fill_table, output_filepath, export_cb, all_data
    ):
        connection.export_to_callback(
            callback=export_cb, dst=output_filepath, query_or_table=fill_table
        )

        assert output_filepath.read_text() == all_data.csv_str()


@pytest.mark.etl
@pytest.mark.exceptions
class TestExportToCallbackExceptions:
    @staticmethod
    def test_export_callback_has_exception(
        connection, empty_table, capture_callback_threads
    ):
        error = ValueError("Error from callback")

        def raise_error(pipe, dst, **kwargs):
            raise error

        with capture_callback_threads(ExaSQLExportThread) as (
            http_thread,
            sql_thread,
        ):
            with pytest.raises(ExaExportError, match="1 sub-exception") as ex:
                connection.export_to_callback(
                    callback=raise_error, dst=None, query_or_table=empty_table
                )

        assert len(ex.value.exceptions) == 1
        assert ex.value.exceptions[0] == error
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()

    @staticmethod
    def test_closed_ws_connection(
        connection_factory, empty_table, export_cb, capture_callback_threads
    ):
        new_connection = connection_factory()

        def export_cb_with_close(pipe, dst, **kwargs):
            new_connection.close(disconnect=False)
            time.sleep(2)
            export_cb(pipe, dst, **kwargs)

        with capture_callback_threads(ExaSQLExportThread) as (
            http_thread,
            sql_thread,
        ):
            with pytest.raises(ExaExportError) as ex:
                new_connection.export_to_callback(
                    export_cb_with_close, None, empty_table
                )

        assert len(ex.value.exceptions) == 2
        assert type(ex.value.exceptions[1]) in (
            ExaCommunicationError,
            ExaRuntimeError,
            OSError,
        )
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()

    @staticmethod
    def test_http_handler_failure_propagates_to_sql_thread(
        connection,
        output_filepath,
        fill_table,
        export_cb,
        capture_callback_threads,
    ):
        def write_final_chunk_with_exception(_handler):
            raise BrokenPipeError("Broken pipe in http_thread")

        with patch.object(
            ExaHttpRequestHandler,
            "write_final_chunk",
            autospec=True,
            side_effect=write_final_chunk_with_exception,
        ):
            with capture_callback_threads(ExaSQLExportThread) as (
                http_thread,
                sql_thread,
            ):
                with pytest.raises(ExaExportError, match="1 sub-exception") as ex:
                    connection.export_to_callback(
                        callback=export_cb,
                        dst=output_filepath,
                        query_or_table=fill_table,
                    )

        assert len(ex.value.exceptions) == 1
        assert isinstance(ex.value.exceptions[0], ExaQueryError)
        assert isinstance(sql_thread.exc, ExaQueryError)
        assert not sql_thread.is_alive()
        assert not http_thread.is_alive()

    @staticmethod
    def test_http_thread_captures_transport_exception(
        connection, output_filepath, empty_table, export_cb, capture_callback_threads
    ):
        error = BrokenPipeError("Broken pipe in http_thread")

        def handle_request_with_exception(_server):
            raise error

        with patch.object(
            ExaTCPServer,
            "handle_request",
            autospec=True,
            side_effect=handle_request_with_exception,
        ):
            with capture_callback_threads(ExaSQLExportThread) as (
                http_thread,
                sql_thread,
            ):
                with pytest.raises(ExaExportError, match="2 sub-exceptions") as ex:
                    connection.export_to_callback(
                        callback=export_cb,
                        dst=output_filepath,
                        query_or_table=empty_table,
                    )

        assert len(ex.value.exceptions) == 2
        assert isinstance(ex.value.exceptions[0], BrokenPipeError)
        assert isinstance(ex.value.exceptions[1], ExaQueryError)
        assert http_thread.exc is error
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()

    @staticmethod
    def test_sql_thread_has_outdated_database_license(
        connection,
        output_filepath,
        empty_table,
        export_cb,
        capture_callback_threads,
    ):
        license_error = ExaQueryError(
            connection=connection,
            query="EXPORT ...",
            code="42000",
            message="Database license is out of date.",
        )

        with patch.object(connection, "execute", side_effect=license_error):
            with capture_callback_threads(ExaSQLExportThread) as (
                http_thread,
                sql_thread,
            ):
                with pytest.raises(ExaExportError, match="1 sub-exception") as ex:
                    connection.export_to_callback(
                        callback=export_cb,
                        dst=output_filepath,
                        query_or_table=empty_table,
                    )

        assert len(ex.value.exceptions) == 1
        assert ex.value.exceptions[0] is license_error
        assert sql_thread.exc is license_error
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()
        assert http_thread.server.socket.fileno() == -1

    @staticmethod
    def test_http_thread_has_exception(
        connection, output_filepath, empty_table, export_cb
    ):
        with patch("pyexasol.connection.ExaHttpThread.join_with_exc") as mock:
            mock.side_effect = BrokenPipeError("Broken pipe in http_thread")

            with pytest.raises(ExaExportError, match="1 sub-exception") as ex:
                connection.export_to_callback(
                    callback=export_cb,
                    dst=output_filepath,
                    query_or_table=empty_table,
                )

        assert len(ex.value.exceptions) == 1
        assert isinstance(ex.value.exceptions[0], BrokenPipeError)

    @staticmethod
    def test_sql_thread_has_exception(connection, output_filepath, export_cb):
        with pytest.raises(ExaExportError, match="1 sub-exception") as ex:
            connection.export_to_callback(
                callback=export_cb, dst=output_filepath, query_or_table="DOES_NOT_EXIST"
            )

        assert len(ex.value.exceptions) == 1
        assert isinstance(ex.value.exceptions[0], ExaQueryError)
        assert "object DOES_NOT_EXIST not found" in ex.value.exceptions[0].message

    @staticmethod
    def test_abort_query(
        connection, output_filepath, empty_table, export_cb, capture_callback_threads
    ):
        """
        Due to a race condition, it's difficult to create a test with
        connection.abort_query() that ensures that an exception would be raised.
        Thus, we mock that here. Still, there is a race condition whether 1 or 2
        exceptions are raised.
        """
        with patch("pyexasol.connection.ExaSQLExportThread.run_sql") as mock:
            mock.side_effect = ExaQueryError(
                message="Client requested execution abort.",
                query="mock response",
                connection=connection,
                code="40007",
            )

            with capture_callback_threads(ExaSQLExportThread) as (
                http_thread,
                sql_thread,
            ):
                with pytest.raises(ExaExportError) as ex:
                    connection.export_to_callback(
                        callback=export_cb,
                        dst=output_filepath,
                        query_or_table=empty_table,
                    )

        query_error_loc = 0
        if len(ex.value.exceptions) == 2:
            query_error_loc = 1

        selected_exception = ex.value.exceptions[query_error_loc]
        assert isinstance(selected_exception, ExaQueryError)
        assert "Client requested execution abort." in selected_exception.message
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()

    @staticmethod
    def test_export_callback_and_sql_have_different_exceptions(
        connection, capture_callback_threads
    ):
        error = ValueError("Error from callback")

        def export_cb(pipe, dst, **kwargs):
            raise error

        with capture_callback_threads(ExaSQLExportThread) as (
            http_thread,
            sql_thread,
        ):
            with pytest.raises(ExaExportError) as ex:
                connection.export_to_callback(
                    callback=export_cb, dst=None, query_or_table="DOES_NOT_EXIST"
                )

        assert len(ex.value.exceptions) == 2
        assert ex.value.exceptions[0] == error
        assert isinstance(ex.value.exceptions[1], ExaQueryError)
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()


@pytest.mark.configuration
class TestExportWithConnectionSettings:
    @staticmethod
    def test_export_camel_case_table_without_quote_ident_fails(
        connection, filled_camel_case_table
    ):
        with pytest.raises(ExaExportError) as ex:
            connection.export_to_pandas(filled_camel_case_table[0])

        assert len(ex.value.exceptions) == 2
        assert isinstance(ex.value.exceptions[1], ExaQueryError)
        assert "object CAMELCASETABLE not found" in ex.value.exceptions[1].message

    @staticmethod
    def test_export_camel_case_table_with_quote_ident(
        connection_with_quote_indent, filled_camel_case_table
    ):
        df = connection_with_quote_indent.export_to_pandas(filled_camel_case_table[0])

        assert df.to_dict() == {"camelCaseColumn!": {0: 1, 1: 2, 2: 3}}
