import time
from unittest.mock import patch

import pytest
from integration.import_and_export.helper import select_result

from pyexasol.exceptions import (
    ExaCommunicationError,
    ExaImportError,
    ExaQueryError,
    ExaRuntimeError,
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


@pytest.mark.etl
@pytest.mark.exceptions
class TestImportFromCallbackExceptions:
    @staticmethod
    def test_import_callback_has_exception(connection, empty_table):
        error = ValueError("Error from callback")

        def import_cb(pipe, src, **kwargs):
            raise error

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

    @staticmethod
    def test_closed_ws_connection(
        connection_factory, connection, empty_table, import_cb
    ):
        new_connection = connection_factory()

        def import_cb_with_close(pipe, src, **kwargs):
            new_connection.close(disconnect=False)
            time.sleep(2)
            import_cb(pipe, src, **kwargs)

        with pytest.raises(ExaImportError) as ex:
            new_connection.import_from_callback(import_cb_with_close, None, empty_table)

        assert len(ex.value.exceptions) == 2
        # race condition: the caught exception depends on how far the thread was
        assert type(ex.value.exceptions[1]) in (ExaCommunicationError, ExaRuntimeError)

    @staticmethod
    def test_http_thread_has_exception(
        connection, input_filepath, empty_table, import_cb
    ):
        with patch("pyexasol.connection.ExaHttpThread.join_with_exc") as mock:
            mock.side_effect = BrokenPipeError("Broken pipe in http_thread")

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

    @staticmethod
    def test_sql_thread_has_exception(connection, input_filepath, import_cb):
        with pytest.raises(ExaImportError, match="1 sub-exception") as ex:
            connection.import_from_callback(
                callback=import_cb, src=input_filepath, table="DOES_NOT_EXIST"
            )

        assert len(ex.value.exceptions) == 1
        assert isinstance(ex.value.exceptions[0], ExaQueryError)
        assert "object DOES_NOT_EXIST not found" in ex.value.exceptions[0].message

    @staticmethod
    def test_abort_query(connection, input_filepath, empty_table, import_cb):
        """
        Due to a race condition, it's difficult to create a test with
        connection.abort_query() that ensures that an exception would be raised.
        Thus, we mock that here. Still, there is a race condition whether 1 or 2
        exceptions are raised.
        """
        with patch("pyexasol.connection.ExaSQLImportThread.run_sql") as mock:
            mock.side_effect = ExaQueryError(
                message="Client requested execution abort.",
                query="mock response",
                connection=connection,
                code="40007",
            )

            with pytest.raises(ExaImportError) as ex:
                connection.import_from_callback(
                    callback=import_cb,
                    src=input_filepath,
                    table=empty_table,
                )

        query_error_loc = 0
        if len(ex.value.exceptions) == 2:
            query_error_loc = 1

        selected_exception = ex.value.exceptions[query_error_loc]
        assert isinstance(selected_exception, ExaQueryError)
        assert "Client requested execution abort." in selected_exception.message

    @staticmethod
    def test_export_callback_and_sql_have_different_exceptions(connection):
        error = ValueError("Error from callback")

        def import_cb(pipe, src, **kwargs):
            raise error

        with pytest.raises(ExaImportError) as ex:
            connection.import_from_callback(
                callback=import_cb, src=None, table="DOES_NOT_EXIST"
            )

        assert ex.value.exceptions[0] == error
        assert isinstance(ex.value.exceptions[1], ExaQueryError)
        assert len(ex.value.exceptions) == 2
