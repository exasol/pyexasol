import pytest

import pyexasol


@pytest.mark.metadata
def test_wss_protocol_version_supports_nosql_metadata(connection):
    assert connection.protocol_version() >= pyexasol.PROTOCOL_V2


@pytest.mark.metadata
def test_schema_exists(connection, schema):
    assert connection.meta.schema_exists(schema)


@pytest.mark.metadata
def test_schema_does_not_exist(connection):
    schema = "ThisSchemaShouldNotExist"
    assert not connection.meta.schema_exists(schema)


@pytest.mark.metadata
def test_table_exists(connection):
    assert connection.meta.table_exists("users")


@pytest.mark.metadata
def test_table_does_not_exist(connection):
    table = "ThisTableShouldNotExist"
    assert not connection.meta.view_exists(table)


@pytest.mark.metadata
def test_view_exists(connection, view):
    assert connection.meta.view_exists(view)


@pytest.mark.metadata
def test_view_does_not_exist(connection):
    view = "ThisViewShouldNotExist"
    assert not connection.meta.view_exists(view)


@pytest.mark.metadata
def test_list_schemas(connection):
    query = connection.meta.execute_meta_nosql("getSchemas")
    schemas = query.fetchall()
    expected = {"EXA_STATISTICS", "PYEXASOL_TEST", "SYS"}
    actual = {schema["NAME"] for schema in schemas}
    assert actual == expected


@pytest.mark.metadata
def test_list_schemas_with_fitler(connection):
    schemas = connection.meta.execute_meta_nosql(
        "getSchemas", {"schema": "PYEXASOL%"}
    ).fetchall()
    expected = {"PYEXASOL_TEST"}
    actual = {schema["NAME"] for schema in schemas}
    assert actual == expected


@pytest.mark.metadata
def test_list_tables_with_filters(connection):
    schemas = connection.meta.execute_meta_nosql(
        "getTables", {"schema": "PYEXASOL_TEST"}
    ).fetchall()
    expected = {"USERS", "PAYMENTS"}
    actual = {schema["NAME"] for schema in schemas}
    assert actual == expected


@pytest.mark.metadata
def test_list_columns_with_filters(connection, expected_user_table_column_last_visit_ts):
    query = connection.meta.execute_meta_nosql(
        "getColumns",
        {
            "schema": "PYEXASOL_TEST",
            "table": "USERS",
        },
    )
    expected = {
        ("USER_ID", "DECIMAL(18,0)"),
        ("USER_NAME", "VARCHAR(255) UTF8"),
        ("REGISTER_DT", "DATE"),
        ("LAST_VISIT_TS", expected_user_table_column_last_visit_ts.sql_type),
        ("IS_FEMALE", "BOOLEAN"),
        ("USER_RATING", "DECIMAL(10,5)"),
        ("USER_SCORE", "DOUBLE"),
        ("STATUS", "VARCHAR(50) UTF8"),
    }
    actual = {(c["NAME"], c["TYPE"]) for c in query.fetchall()}
    assert actual == expected


@pytest.mark.metadata
def test_list_keywords(connection, expected_reserved_words):
    expected = expected_reserved_words
    actual = set(connection.meta.list_sql_keywords())
    assert actual == expected
