import pytest

from pyexasol.exceptions import (
    ExaCallbackError,
    ExaQueryError,
)


@pytest.fixture
def connection_without_resolving_hostnames(connection_factory):
    with connection_factory(compression=True, resolve_hostnames=False) as con:
        yield con


@pytest.mark.etl
class TestExportParams:
    @staticmethod
    def test_with_no_params(connection, fill_table, tmp_path, table_name, all_data):
        actual_filepath = tmp_path / "actual.csv"

        connection.export_to_file(dst=actual_filepath, query_or_table=table_name)

        assert actual_filepath.read_text() == all_data.csv_str()

    @staticmethod
    def test_csv_cols(connection, fill_table, tmp_path, table_name, all_data):
        actual_filepath = tmp_path / "actual.csv"
        params = {"csv_cols": ["1..7"]}

        connection.export_to_file(
            dst=actual_filepath, query_or_table=table_name, export_params=params
        )

        assert actual_filepath.read_text() == all_data.csv_str()

    @staticmethod
    def test_delimit(connection, fill_table, tmp_path, table_name, all_data):
        actual_filepath = tmp_path / "actual.csv"
        params = {"delimit": "AUTO"}

        connection.export_to_file(
            dst=actual_filepath, query_or_table=table_name, export_params=params
        )

        assert actual_filepath.read_text() == all_data.csv_str()

    @staticmethod
    def test_without_column_names(
        connection, fill_table, tmp_path, table_name, all_data
    ):
        actual_filepath = tmp_path / "actual.csv"

        connection.export_to_file(dst=actual_filepath, query_or_table=table_name)

        assert actual_filepath.read_text() == all_data.csv_str()

    @staticmethod
    def test_with_column_names(connection, fill_table, tmp_path, table_name, all_data):
        actual_filepath = tmp_path / "actual.csv"
        params = {"with_column_names": True}

        connection.export_to_file(
            dst=actual_filepath, query_or_table=table_name, export_params=params
        )

        expected_header = ",".join(all_data.columns) + "\n"
        assert actual_filepath.read_text() == expected_header + all_data.csv_str()


@pytest.mark.etl
class TestExportGeneral:
    @staticmethod
    def test_without_resolving_hostname(
        connection_without_resolving_hostnames,
        fill_table,
        tmp_path,
        table_name,
        all_data,
    ):
        actual_filepath = tmp_path / "actual.csv"

        connection_without_resolving_hostnames.export_to_file(
            dst=actual_filepath, query_or_table=table_name
        )

        assert actual_filepath.read_text() == all_data.csv_str()

    @staticmethod
    def test_custom_export_callback(
        connection, fill_table, tmp_path, table_name, all_data
    ):
        actual_filepath = tmp_path / "actual.csv"

        def export_cb(pipe, dst):
            dst.write_bytes(pipe.read())

        connection.export_to_callback(
            callback=export_cb, dst=actual_filepath, query_or_table=table_name
        )

        assert actual_filepath.read_text() == all_data.csv_str()


@pytest.mark.etl
@pytest.mark.exceptions
class TestExportToCallbackExceptions:
    @staticmethod
    def test_only_export_callback_has_exception(connection, fill_table, table_name):
        error_msg = "Error from callback"

        def export_cb(pipe, dst, **kwargs):
            raise ValueError(error_msg)

        with pytest.raises(ValueError, match=error_msg):
            connection.export_to_callback(
                callback=export_cb, dst=None, query_or_table=table_name
            )

    @staticmethod
    def test_only_sql_has_exception(connection, tmp_path):
        actual_filepath = tmp_path / "actual.csv"

        def export_cb(pipe, dst, **kwargs):
            dst.write_bytes(pipe.read())

        with pytest.raises(ExaQueryError, match="object DOES_NOT_EXIST not found"):
            connection.export_to_callback(
                callback=export_cb, dst=actual_filepath, query_or_table="DOES_NOT_EXIST"
            )

    @staticmethod
    def test_both_export_callback_and_sql_have_exceptions(connection):
        error_msg = "Error from callback"

        def export_cb(pipe, dst, **kwargs):
            raise ValueError(error_msg)

        with pytest.raises(ExaCallbackError) as ex:
            connection.export_to_callback(
                callback=export_cb, dst=None, query_or_table="DOES_NOT_EXIST"
            )

        assert ex.value.callback.__name__ == "export_cb"
        assert repr(ex.value.callback_error) == repr(ValueError(error_msg))
        assert isinstance(ex.value.sql_thread_error, ExaQueryError)
        assert (
            "object DOES_NOT_EXIST not found [line 1, column 8] "
            in ex.value.sql_thread_error.message
        )
