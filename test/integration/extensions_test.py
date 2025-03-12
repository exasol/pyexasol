import pytest

import pyexasol


@pytest.mark.extensions
class TestExaExtension:

    @pytest.fixture(scope="class")
    def connection(self, dsn, user, password, schema, websocket_sslopt):
        con = pyexasol.connect(
            dsn=dsn,
            user=user,
            password=password,
            schema=schema,
            websocket_sslopt=websocket_sslopt,
            lower_ident=True,
        )
        yield con
        con.close()

    def test_get_columns(self, connection, expected_user_table_column_info):
        actual = connection.ext.get_columns("users")
        assert expected_user_table_column_info == actual

    def test_columns_sql(self, connection, expected_user_table_column_info):
        actual = connection.ext.get_columns_sql("SELECT * FROM users")
        assert expected_user_table_column_info == actual

    def test_get_sys_columns(self, connection, expected_user_table_column_last_visit_ts):
        expected = [
            {
                "name": "USER_ID",
                "type": "DECIMAL",
                "sql_type": "DECIMAL(18,0)",
                "size": 18,
                "scale": 0,
                "nulls": True,
                "distribution_key": False,
                "default": None,
                "comment": None,
            },
            {
                "name": "USER_NAME",
                "type": "VARCHAR",
                "sql_type": "VARCHAR(255) UTF8",
                "size": 255,
                "scale": None,
                "nulls": True,
                "distribution_key": False,
                "default": None,
                "comment": None,
            },
            {
                "name": "REGISTER_DT",
                "type": "DATE",
                "sql_type": "DATE",
                "size": 10,
                "scale": None,
                "nulls": True,
                "distribution_key": False,
                "default": None,
                "comment": None,
            },
            {
                "name": "LAST_VISIT_TS",
                "type": "TIMESTAMP",
                "sql_type": expected_user_table_column_last_visit_ts.sql_type,
                "size": 29,
                "scale": None,
                "nulls": True,
                "distribution_key": False,
                "default": None,
                "comment": None,
            },
            {
                "name": "IS_FEMALE",
                "type": "BOOLEAN",
                "sql_type": "BOOLEAN",
                "size": 1,
                "scale": None,
                "nulls": True,
                "distribution_key": False,
                "default": None,
                "comment": None,
            },
            {
                "name": "USER_RATING",
                "type": "DECIMAL",
                "sql_type": "DECIMAL(10,5)",
                "size": 10,
                "scale": 5,
                "nulls": True,
                "distribution_key": False,
                "default": None,
                "comment": None,
            },
            {
                "name": "USER_SCORE",
                "type": "DOUBLE PRECISION",
                "sql_type": "DOUBLE",
                "size": 64,
                "scale": None,
                "nulls": True,
                "distribution_key": False,
                "default": None,
                "comment": None,
            },
            {
                "name": "STATUS",
                "type": "VARCHAR",
                "sql_type": "VARCHAR(50) UTF8",
                "size": 50,
                "scale": None,
                "nulls": True,
                "distribution_key": False,
                "default": None,
                "comment": None,
            },
        ]
        actual = connection.ext.get_sys_columns("users")
        assert expected == actual

    def test_get_sys_tables(self, connection):
        expected = [
            {
                "table_name": "payments",
                "table_schema": "pyexasol_test",
                "table_is_virtual": False,
                "table_has_distribution_key": False,
                "table_comment": None,
            },
            {
                "table_name": "users",
                "table_schema": "pyexasol_test",
                "table_is_virtual": False,
                "table_has_distribution_key": False,
                "table_comment": None,
            },
        ]
        actual = connection.ext.get_sys_tables()
        assert expected == actual

    def test_get_sys_views(self, connection):
        expected = []
        actual = connection.ext.get_sys_views()
        assert expected == actual

    def test_get_sys_schemas(self, connection):
        expected = [
            {
                "schema_comment": None,
                "schema_is_virtual": False,
                "schema_name": "pyexasol_test",
                "schema_owner": "sys",
            }
        ]
        actual = connection.ext.get_sys_schemas()
        assert expected == actual

    def test_get_disk_space(self, flush_statistics, connection):
        expected = {
            "free_size",
            "measure_time",
            "occupied_size",
            "occupied_size_percent",
            "total_size",
        }
        actual = set(connection.ext.get_disk_space_usage().keys())
        assert expected == actual

    def test_export_to_pandas_with_dtype(self, connection):
        result = connection.ext.export_to_pandas_with_dtype("users")

        expected = 10_000
        actual = len(result)
        assert actual == expected

        expected = {
            "user_id": "int64",
            "user_name": "category",
            "register_dt": "object",
            "last_visit_ts": "object",
            "is_female": "category",
            "user_rating": "float64",
            "user_score": "float64",
            "status": "category",
        }
        actual = {column: result[column].dtype.name for column in expected}
        assert actual == expected

    def test_get_reserved_words(self, connection, expected_reserved_words):
        expected = expected_reserved_words
        actual = set(connection.ext.get_reserved_words())
        assert expected == actual
