import pytest
from integration.import_and_export.helper import select_result

from pyexasol.exceptions import (
    ExaImportError,
    ExaQueryError,
)


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
