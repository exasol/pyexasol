import polars as pl
import pytest
from integration.import_and_export.helper import (
    SELECT_QUERY,
    select_result,
)


@pytest.mark.polars
class TestExportToPolars:
    @staticmethod
    def test_export_from_table(connection, fill_table, table_name, all_data):
        result = connection.export_to_polars(table_name)
        assert result.equals(pl.from_dicts(all_data.list_dict))

    @staticmethod
    def test_export_from_sql_query(connection, fill_table, all_data):
        result = connection.export_to_polars(SELECT_QUERY)
        assert result.equals(pl.from_dicts(all_data.list_dict))


@pytest.mark.polars
class TestImportFromPolars:
    @staticmethod
    def test_from_dataframe(connection, table_name, all_data):
        df = pl.from_records(all_data.list_dict)
        connection.import_from_polars(df, table_name)

        assert select_result(connection) == all_data.list_tuple()

    @staticmethod
    def test_from_lazy_frame(connection, table_name, all_data):
        lf = pl.from_records(all_data.list_dict).lazy()
        connection.import_from_polars(lf, table_name)

        assert select_result(connection) == all_data.list_tuple()
