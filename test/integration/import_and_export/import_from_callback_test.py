from test.integration.import_and_export.helper import select_result
from unittest.mock import patch

import pytest

from pyexasol.exceptions import (
    ExaCommunicationError,
    ExaImportError,
    ExaQueryError,
    ExaRuntimeError,
)
from pyexasol.http_transport import (
    ExaHttpRequestHandler,
    ExaSQLThread,
    ExaTCPServer,
)


@pytest.fixture
def input_filepath(tmp_path):
    filepath = tmp_path / "input.csv"
    filepath.touch()
    return filepath


@pytest.fixture
def import_cb():
    """Provides a standard custom import callback function."""

    def _callback(pipe, src, **kwargs):
        pipe.write(src.read_bytes())

    return _callback


@pytest.mark.etl
class TestImportParams:
    @staticmethod
    def test_with_no_params(connection, empty_table, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)

        connection.import_from_file(src=filepath, table=empty_table)

        assert select_result(connection) == all_data.list_tuple()

    @staticmethod
    def test_csv_cols(connection, empty_table, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)
        params = {"csv_cols": ["1..7"]}

        connection.import_from_file(
            src=filepath, table=empty_table, import_params=params
        )

        assert select_result(connection) == all_data.list_tuple()

    @staticmethod
    def test_swapped_columns(connection, empty_table, tmp_path, all_data):
        params = {
            "columns": [
                "FIRST_NAME",
                "LAST_NAME",
                "REGISTER_DT",
                "LAST_VISIT_TS",
                # These two columns are switched in the imported data
                # relative to the table's DDL definition.
                "AGE",
                "IS_GRADUATING",
                "SCORE",
            ]
        }
        filepath = all_data.write_csv(
            directory=tmp_path, selected_columns=params["columns"]
        )

        connection.import_from_file(
            src=filepath, table=empty_table, import_params=params
        )

        # Despite two columns being swapped in the input data, it was inserted
        # correctly into the table, as the user indicated in the parameters that
        # the columns were in a different order.
        assert select_result(connection) == all_data.list_tuple()

    @staticmethod
    def test_skip(connection, empty_table, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)
        offset = 2
        params = {"skip": offset}

        connection.import_from_file(
            src=filepath, table=empty_table, import_params=params
        )

        assert select_result(connection) == all_data.list_tuple()[offset:]

    @staticmethod
    def test_trim(connection, empty_table, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)
        params = {"trim": "TRIM"}

        connection.import_from_file(
            src=filepath, table=empty_table, import_params=params
        )

        assert select_result(connection) == all_data.list_tuple()


@pytest.mark.etl
class TestImportGeneral:
    @staticmethod
    def test_without_resolving_hostname(
        connection_without_resolving_hostnames, empty_table, tmp_path, all_data
    ):
        filepath = all_data.write_csv(directory=tmp_path)

        connection_without_resolving_hostnames.import_from_file(
            src=filepath, table=empty_table
        )

        assert (
            select_result(connection_without_resolving_hostnames)
            == all_data.list_tuple()
        )

    @staticmethod
    def test_custom_import_callback(
        connection, empty_table, tmp_path, all_data, import_cb
    ):
        filepath = all_data.write_csv(directory=tmp_path)

        connection.import_from_callback(
            callback=import_cb, src=filepath, table=empty_table
        )

        assert select_result(connection) == all_data.list_tuple()

    @staticmethod
    def test_custom_import_callback_with_schema_qualified_table(
        connection, schema, empty_table, tmp_path, all_data, import_cb
    ):
        filepath = all_data.write_csv(directory=tmp_path)

        connection.import_from_callback(
            callback=import_cb,
            src=filepath,
            table=(schema, empty_table),
        )

        assert select_result(connection) == all_data.list_tuple()


@pytest.mark.etl
@pytest.mark.exceptions
class TestImportFromCallbackExceptions:
    @staticmethod
    def test_import_callback_has_exception(
        connection, empty_table, capture_callback_threads
    ):
        """
        The defined callback function raises an exception:
          - That failure closes the write pipe before the IMPORT request can finish.
          - The SQL thread therefore receives an incomplete request from Exasol and
            raises an ``ExaQueryError``.
          - The HTTP thread is only stopped by cleanup; it does not perform the failed
          SQL operation, and therefore, it is not expected to have its own exception.
        """
        error = ValueError("Error from callback")

        def import_cb(pipe, src, **kwargs):
            raise error

        with capture_callback_threads(ExaSQLThread) as (
            http_thread,
            sql_thread,
        ):
            with pytest.raises(ExaImportError, match="2 sub-exceptions") as ex:
                connection.import_from_callback(
                    callback=import_cb, src=None, table=empty_table
                )

        assert len(ex.value.exceptions) == 2
        assert ex.value.exceptions[0] == error
        assert isinstance(ex.value.exceptions[1], ExaQueryError)
        assert (
            "Following error occured while reading data"
            in ex.value.exceptions[1].message
        )
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()

    @staticmethod
    def test_closed_ws_connection(
        connection_factory, empty_table, import_cb, capture_callback_threads
    ):
        """
        The callback closes the WebSocket while the SQL thread and the HTTP thread
        are using it:
          - Thus, the calling thread fails while using the callback pipe.
          - The SQL thread fails because its database request loses the WebSocket
            transport.
          - The SQL thread terminates the HTTP thread and its TCP server, but the
            HTTP thread does not raise an exception of its own.
        """
        new_connection = connection_factory()

        def import_cb_with_close(pipe, src, **kwargs):
            new_connection.close(disconnect=False)
            import_cb(pipe, src, **kwargs)

        with capture_callback_threads(ExaSQLThread) as (
            http_thread,
            sql_thread,
        ):
            with pytest.raises(ExaImportError) as ex:
                new_connection.import_from_callback(
                    import_cb_with_close, None, empty_table
                )

        assert len(ex.value.exceptions) == 2
        # race condition: the caught exception depends on how far the thread was
        assert type(ex.value.exceptions[1]) in (
            ExaCommunicationError,
            ExaRuntimeError,
            OSError,
        )
        assert sql_thread.exc is not None
        assert http_thread.exc is None
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()

    @staticmethod
    def test_http_handler_failure_propagates_to_sql_thread(
        connection,
        input_filepath,
        empty_table,
        import_cb,
        all_data,
        capture_callback_threads,
    ):
        """
        The TCP server (``ExaTCPServer``) owned by the HTTP thread raises an exception
        while writing a chunk of data:
          - The HTTP thread captures the handler exception and closes its transport.
          - The SQL thread is executing the IMPORT that depends on that transport,
            so it receives the truncated request and records an ``ExaQueryError``.
          - The callback is not the failing component and does not raise an
            exception.
        """
        error = BrokenPipeError("Broken pipe in http_thread")
        input_filepath.write_text(all_data.csv_str())

        def write_chunk_with_exception(_handler, _data):
            raise error

        with patch.object(
            ExaHttpRequestHandler,
            "write_chunk",
            autospec=True,
            side_effect=write_chunk_with_exception,
        ):
            with capture_callback_threads(ExaSQLThread) as (
                http_thread,
                sql_thread,
            ):
                with pytest.raises(ExaImportError, match="1 sub-exception") as ex:
                    connection.import_from_callback(
                        callback=import_cb,
                        src=input_filepath,
                        table=empty_table,
                    )

        assert len(ex.value.exceptions) == 1
        assert isinstance(ex.value.exceptions[0], ExaQueryError)
        assert isinstance(sql_thread.exc, ExaQueryError)
        assert (
            "Following error occured while reading data"
            in ex.value.exceptions[0].message
        )
        assert not sql_thread.is_alive()
        assert not http_thread.is_alive()

    @staticmethod
    def test_http_thread_captures_transport_exception(
        connection, input_filepath, empty_table, import_cb, capture_callback_threads
    ):
        """
        The TCP server (``ExaTCPServer``, owned by the HTTP thread) raises a transport
        exception:
          - The HTTP thread captures the exception raised by its TCP server.
          - Because the TCP server can no longer carry the data, the concurrently
            running SQL thread's IMPORT receives a transport failure and raises an
            ``ExaQueryError``.
          - The callback only supplies data and completes normally.
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
            with capture_callback_threads(ExaSQLThread) as (
                http_thread,
                sql_thread,
            ):
                with pytest.raises(ExaImportError, match="2 sub-exceptions") as ex:
                    connection.import_from_callback(
                        callback=import_cb,
                        src=input_filepath,
                        table=empty_table,
                    )

        assert len(ex.value.exceptions) == 2
        assert isinstance(ex.value.exceptions[0], BrokenPipeError)
        assert isinstance(ex.value.exceptions[1], ExaQueryError)
        assert (
            "Following error occured while reading data"
            in ex.value.exceptions[1].message
        )
        assert http_thread.exc is error
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()

    @staticmethod
    def test_sql_thread_has_outdated_database_license(
        connection,
        input_filepath,
        empty_table,
        import_cb,
        capture_callback_threads,
    ):
        """
        The SQL thread raises an exception before it can consume the import data:
          - The SQL thread terminates the HTTP thread and closes the pipe.
          - The callback then attempts to use that pipe and raises a ``ValueError``.
          - The HTTP thread is only being terminated by the SQL thread, so it does not
            report a separate exception.
        """
        license_error = ExaQueryError(
            connection=connection,
            query="IMPORT INTO ...",
            code="42000",
            message="Database license is out of date.",
        )

        with patch.object(connection, "execute", side_effect=license_error):
            with capture_callback_threads(ExaSQLThread) as (
                http_thread,
                sql_thread,
            ):
                with pytest.raises(ExaImportError, match="2 sub-exceptions") as ex:
                    connection.import_from_callback(
                        callback=import_cb,
                        src=input_filepath,
                        table=empty_table,
                    )

        assert len(ex.value.exceptions) == 2
        assert isinstance(ex.value.exceptions[0], ValueError)
        assert ex.value.exceptions[1] is license_error
        assert sql_thread.exc is license_error
        assert http_thread.exc is None
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()
        assert http_thread.server.socket.fileno() == -1

    @staticmethod
    def test_sql_thread_has_exception(
        connection, input_filepath, import_cb, capture_callback_threads
    ):
        """
        The SQL thread executes the IMPORT query, so it detects the missing table
        and records an ``ExaQueryError``:
          - The SQL thread terminates the HTTP thread, which exits without an exception.
          - The callback supplies the input data but does not raise an exception.
          - Cleanup joins the HTTP thread first, then joins the SQL thread and
            propagates its ``ExaQueryError``.
        The aggregated ``ExaImportError`` therefore contains only the SQL-thread
        exception.
        """
        with capture_callback_threads(ExaSQLThread) as (
            http_thread,
            sql_thread,
        ):
            with pytest.raises(ExaImportError, match="1 sub-exception") as ex:
                connection.import_from_callback(
                    callback=import_cb, src=input_filepath, table="DOES_NOT_EXIST"
                )

        assert len(ex.value.exceptions) == 1
        assert isinstance(ex.value.exceptions[0], ExaQueryError)
        assert isinstance(sql_thread.exc, ExaQueryError)
        assert http_thread.exc is None
        assert "object DOES_NOT_EXIST not found" in ex.value.exceptions[0].message
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()

    @staticmethod
    def test_abort_query(
        connection, input_filepath, empty_table, import_cb, capture_callback_threads
    ):
        """
        The SQL thread raises an ``ExaQueryError`` because the query is aborted:
          - The SQL thread terminates the HTTP thread.
          - The callback pipe is closed while the callback is still active, so the
            HTTP/callback path contributes a second exception.
        """
        with patch("pyexasol.connection.ExaSQLThread.run_sql") as mock:
            mock.side_effect = ExaQueryError(
                message="Client requested execution abort.",
                query="mock response",
                connection=connection,
                code="40007",
            )
            with capture_callback_threads(ExaSQLThread) as (
                http_thread,
                sql_thread,
            ):
                with pytest.raises(ExaImportError) as ex:
                    connection.import_from_callback(
                        callback=import_cb,
                        src=input_filepath,
                        table=empty_table,
                    )

        assert len(ex.value.exceptions) == 2

        selected_exception = ex.value.exceptions[1]
        assert isinstance(selected_exception, ExaQueryError)
        assert "Client requested execution abort." in selected_exception.message
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()

    @staticmethod
    def test_import_callback_and_sql_have_different_exceptions(
        connection, capture_callback_threads
    ):
        """
        The callback in the calling thread raises a ``ValueError`` before it can
        provide input. The SQL thread independently executes the IMPORT:
          - The SQL thread independently reports the missing-table
            ``ExaQueryError`` because it is the only component executing the
            query.
          - The HTTP thread is terminated during cleanup and does not raise an exception.
        """
        error = ValueError("Error from callback")

        def import_cb(pipe, src, **kwargs):
            raise error

        with capture_callback_threads(ExaSQLThread) as (
            http_thread,
            sql_thread,
        ):
            with pytest.raises(ExaImportError) as ex:
                connection.import_from_callback(
                    callback=import_cb, src=None, table="DOES_NOT_EXIST"
                )

        assert ex.value.exceptions[0] == error
        assert isinstance(ex.value.exceptions[1], ExaQueryError)
        assert len(ex.value.exceptions) == 2
        assert sql_thread.exc is ex.value.exceptions[1]
        assert http_thread.exc is None
        assert not http_thread.is_alive()
        assert not sql_thread.is_alive()
        assert http_thread.server.socket.fileno() == -1


@pytest.mark.configuration
class TestImportWithConnectionSettings:
    @staticmethod
    def test_import_to_camel_case_table_without_quote_ident_fails(
        connection, empty_camel_case_table
    ):
        import pandas as pd

        table_name, column_name = empty_camel_case_table

        df = pd.DataFrame({column_name: [1, 2, 3]})
        with pytest.raises(ExaImportError) as ex:
            connection.import_from_pandas(df, table_name)

        num_exceptions = len(ex.value.exceptions)

        query_exception_loc = 0
        if num_exceptions == 2:
            query_exception_loc = 1

        assert num_exceptions <= 2
        assert isinstance(ex.value.exceptions[query_exception_loc], ExaQueryError)
        assert (
            "object CAMELCASETABLE not found"
            in ex.value.exceptions[query_exception_loc].message
        )

    @staticmethod
    def test_import_to_camel_case_table_with_quote_ident(
        connection_with_quote_indent, empty_camel_case_table
    ):
        import pandas as pd

        table_name, column_name = empty_camel_case_table

        df = pd.DataFrame({column_name: [1, 2, 3]})
        connection_with_quote_indent.import_from_pandas(df, table_name)

        result = connection_with_quote_indent.export_to_pandas(table_name).to_dict()

        assert result == df.to_dict()
