import pytest
from pyexasol.exceptions import ExaQueryError

@pytest.fixture
def connection_without_resolving_hostnames(connection_factory):
    with connection_factory(compression=True, resolve_hostnames=False) as con:
        yield con


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
    def test_only_export_callback_has_exception(connection, fill_table):
        error_msg = "Error from callback"

        def export_cb(pipe, dst, **kwargs):
            raise Exception(error_msg)

        with pytest.raises(Exception, match=error_msg):
            connection.export_to_callback(
                callback=export_cb, dst=None, query_or_table=fill_table
            )

    @staticmethod
    def test_only_sql_has_exception(connection, output_filepath, export_cb):
        with pytest.raises(ExaQueryError, match="object DOES_NOT_EXIST not found"):
            connection.export_to_callback(
                callback=export_cb, dst=output_filepath, query_or_table="DOES_NOT_EXIST"
            )
