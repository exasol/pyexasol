from os import devnull
from unittest.mock import patch

import pytest

from pyexasol.exceptions import (
    ExaExportError,
    ExaQueryError,
)


@pytest.fixture
def connection_without_resolving_hostnames(connection_factory):
    with connection_factory(compression=True, resolve_hostnames=False) as con:
        yield con


@pytest.fixture
def dev_null():
    with open(devnull, "wb") as f:
        yield f


@pytest.fixture
def output_filepath(tmp_path):
    return tmp_path / "test.csv"


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
    def test_custom_export_callback(connection, fill_table, output_filepath, all_data):
        def export_cb(pipe, dst):
            dst.write_bytes(pipe.read())

        connection.export_to_callback(
            callback=export_cb, dst=output_filepath, query_or_table=fill_table
        )

        assert output_filepath.read_text() == all_data.csv_str()


@pytest.mark.etl
@pytest.mark.exceptions
class TestExportToCallbackExceptions:
    @staticmethod
    def test_export_callback_has_exception(connection, empty_table):
        error = ValueError("Error from callback")

        def export_cb(pipe, dst, **kwargs):
            raise error

        with pytest.raises(ExaExportError, match="1 sub-exception") as ex:
            connection.export_to_callback(
                callback=export_cb, dst=None, query_or_table=empty_table
            )

        assert ex.value.exceptions == [error]

    @staticmethod
    def test_http_thread_has_exception(connection, output_filepath, empty_table):
        def export_cb(pipe, dst, **kwargs):
            dst.write_bytes(pipe.read())

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
    def test_sql_thread_has_exception(connection, output_filepath):
        def export_cb(pipe, dst, **kwargs):
            dst.write_bytes(pipe.read())

        with pytest.raises(ExaExportError, match="1 sub-exception") as ex:
            connection.export_to_callback(
                callback=export_cb, dst=output_filepath, query_or_table="DOES_NOT_EXIST"
            )

        assert len(ex.value.exceptions) == 1
        assert isinstance(ex.value.exceptions[0], ExaQueryError)
        assert "object DOES_NOT_EXIST not found" in ex.value.exceptions[0].message

    @staticmethod
    def test_abort_query(connection, output_filepath, empty_table):
        """
        Due to a race condition, it's difficult to create a test with
        connection.abort_query() that ensures that an exception would be raised.
        Thus, we mock that here. Still, there is a race condition whether 1 or 2
        exceptions are raised.
        """

        def export_cb(pipe, dst, **kwargs):
            dst.write_bytes(pipe.read())

        with patch("pyexasol.connection.ExaSQLExportThread.run_sql") as mock:
            mock.side_effect = ExaQueryError(
                message="Client requested execution abort.",
                query="mock response",
                connection=connection,
                code="40007",
            )

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

    @staticmethod
    def test_export_callback_and_sql_have_different_exceptions(connection):
        error = ValueError("Error from callback")

        def export_cb(pipe, dst, **kwargs):
            raise error

        with pytest.raises(ExaExportError) as ex:
            connection.export_to_callback(
                callback=export_cb, dst=None, query_or_table="DOES_NOT_EXIST"
            )

        assert len(ex.value.exceptions) == 2
        assert ex.value.exceptions[0] == error
        assert isinstance(ex.value.exceptions[1], ExaQueryError)
