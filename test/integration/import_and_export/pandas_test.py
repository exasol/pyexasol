import pandas as pd
import pytest
from integration.import_and_export.helper import (
    SELECT_QUERY,
    select_result,
)
from numpy.array_api import int64


@pytest.mark.pandas
class TestExportToPandas:
    @staticmethod
    def test_export_from_table(connection, fill_table, table_name, all_data):
        expected = pd.DataFrame(all_data.list_dict)
        expected.IS_GRADUATING = expected.IS_GRADUATING.astype(int64)

        result = connection.export_to_pandas(table_name)

        assert result.equals(expected)

    @staticmethod
    def test_export_from_sql_query(connection, fill_table, all_data):
        expected = pd.DataFrame(all_data.list_dict)
        expected.IS_GRADUATING = expected.IS_GRADUATING.astype(int64)

        result = connection.export_to_pandas(SELECT_QUERY)

        assert result.equals(expected)


@pytest.mark.pandas
class TestImportFromPandas:
    @staticmethod
    def test_from_dataframe(connection, table_name, all_data):
        df = pd.DataFrame(all_data.list_dict)
        connection.import_from_pandas(df, table_name)

        assert select_result(connection) == all_data.list_tuple()
