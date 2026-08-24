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
        """
        The export callback running in the calling thread raises a ``ValueError``:
          - The callback fails before it can consume any exported data.
          - The HTTP thread is stopped during cleanup and does not raise an exception.
          - The SQL thread does not observe a separate failure.
        """
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
        """
        The callback closes the WebSocket while the SQL thread and the HTTP thread
        are using it:
          - The calling thread fails while reading from the callback pipe.
          - The SQL thread fails because its database request loses the WebSocket
            transport.
          - The SQL thread terminates the HTTP thread, and the HTTP thread does not
            raise an exception.
        """
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
        """
        The ``ExaHttpRequestHandler`` running through the ``ExaTCPServer`` (owned by
        the HTTP thread) raises ``BrokenPipeError`` while writing the final chunk:
          - The HTTP thread (``ExaHttpThread``) handles the failed transport and
            closes the HTTP response.
          - The SQL thread is executing the EXPORT that depends on that response,
            so it records an ``ExaQueryError``.
          - The callback is not the failing component and does not raise an
            exception. Only the SQL-thread exception is expected.
        """

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
        """
        The TCP server (owned by the HTTP thread) raises a ``BrokenPipeError``:
          - The HTTP thread captures the exception raised by its TCP server.
          - Because the TCP server can no longer carry the exported data, the
            concurrently running SQL thread's EXPORT receives a transport failure
            and raises an ``ExaQueryError``.
          - The callback only consumes data and does not raise an exception.
        """
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
        fill_table,
        capture_callback_threads,
    ):
        """
        The SQL thread raises a database-license ``ExaQueryError`` before EXPORT
        can deliver rows:
          - The SQL thread terminates the HTTP thread and closes the callback pipe.
          - The callback then attempts to read from that pipe and raises a
            ``ValueError``.
        """

        def streaming_export_cb(pipe, dst, **kwargs):
            with dst.open("wb") as output_file:
                while chunk := pipe.read(8192):
                    output_file.write(chunk)

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
                with pytest.raises(ExaExportError, match="2 sub-exceptions") as ex:
                    connection.export_to_callback(
                        callback=streaming_export_cb,
                        dst=output_filepath,
                        query_or_table=fill_table,
                    )

        assert len(ex.value.exceptions) == 2
        assert isinstance(ex.value.exceptions[0], ValueError)
        assert ex.value.exceptions[1] is license_error
        assert sql_thread.exc is license_error
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()
        assert http_thread.server.socket.fileno() == -1

    @staticmethod
    def test_http_thread_has_exception(
        connection, output_filepath, empty_table, export_cb
    ):
        """
        The HTTP thread raises a ``BrokenPipeError`` when the coordinator:
          - The calling thread receives the HTTP-thread exception after the
            callback has finished.
          - The SQL thread is not executing a failing query and does not raise an
            exception.
        """
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
        """
        The SQL thread executes the EXPORT query, so it detects the missing table
        and raises an ``ExaQueryError``:
          - The callback only consumes output and does not raise an exception.
          - The HTTP thread only transports the output and does not report an exception.
        """
        with pytest.raises(ExaExportError, match="1 sub-exception") as ex:
            connection.export_to_callback(
                callback=export_cb, dst=output_filepath, query_or_table="DOES_NOT_EXIST"
            )

        assert len(ex.value.exceptions) == 1
        assert isinstance(ex.value.exceptions[0], ExaQueryError)
        assert "object DOES_NOT_EXIST not found" in ex.value.exceptions[0].message

    @staticmethod
    def test_abort_query(
        connection, output_filepath, fill_table, export_cb, capture_callback_threads
    ):
        """
        The SQL thread raises an ``ExaQueryError`` because the EXPORT query is
        aborted:
          - The SQL thread terminates the HTTP thread.
          - The callback pipe is closed while the callback is still active, so the
            HTTP/callback path contributes a second exception.
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
                        query_or_table=fill_table,
                    )

        assert len(ex.value.exceptions) == 2

        selected_exception = ex.value.exceptions[1]
        assert isinstance(selected_exception, ExaQueryError)
        assert "Client requested execution abort." in selected_exception.message
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()

    @staticmethod
    def test_export_callback_and_sql_have_different_exceptions(
        connection, capture_callback_threads
    ):
        """
        The callback in the calling thread raises a ``ValueError`` before it can
        consume the export stream. The SQL thread independently executes the
        EXPORT:
          - The SQL thread reports the missing-table ``ExaQueryError`` because it
            is the only component executing the query.
          - The HTTP thread is terminated during cleanup and does not raise an exception.
        """
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
