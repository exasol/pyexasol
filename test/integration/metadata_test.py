import pytest


@pytest.mark.metadata
def test_get_columns_without_executing_query(
    connection, expected_user_table_column_info
):
    statement = "SELECT a.*, a.user_id + 1 AS next_user_id FROM users a"
    expected = {k.upper(): v for k, v in expected_user_table_column_info.items()}
    expected["NEXT_USER_ID"] = {"type": "DECIMAL", "precision": 19, "scale": 0}
    actual = connection.meta.sql_columns(statement)
    assert actual == expected


@pytest.mark.metadata
def test_schema_exists_returns_true_if_schema_exists(connection, schema):
    assert connection.meta.schema_exists(schema)


@pytest.mark.metadata
def test_schema_exists_returns_false_if_schema_doesnt_exist(connection):
    schema = "THIS_SCHEMA_SHOULD_NOT_EXIST_____"
    assert not connection.meta.schema_exists(schema)


@pytest.mark.metadata
def test_table_exits_returns_true_for_existing_table(connection):
    table = "users"
    assert connection.meta.table_exists(table)


@pytest.mark.metadata
def test_table_exits_returns_false_for_non_existing_table(connection):
    table = "this_talbe_should_not_exist_____"
    assert not connection.meta.table_exists(table)


@pytest.mark.metadata
def test_view_exits_returns_true_for_existing_view(connection, view):
    assert connection.meta.view_exists(view)


@pytest.mark.metadata
def test_view_exits_returns_false_for_non_existing_view(connection):
    view = "this_view_should_not_exist_____"
    assert not connection.meta.view_exists(view)


@pytest.mark.metadata
def test_list_schemas(connection):
    expected = ["PYEXASOL_TEST"]
    actual = [schema["SCHEMA_NAME"] for schema in connection.meta.list_schemas()]
    assert actual == expected


@pytest.mark.metadata
def test_list_schemas_with_filter(connection):
    expected = []
    actual = [
        schema["SCHEMA_NAME"]
        for schema in connection.meta.list_schemas(schema_name_pattern="FOOBAR%")
    ]
    assert actual == expected


@pytest.mark.metadata
def test_list_tables(connection):
    expected = {"USERS", "PAYMENTS"}
    actual = {
        table["TABLE_NAME"]
        for table in connection.meta.list_tables(table_schema_pattern="PYEXASOL%")
    }
    assert actual == expected


@pytest.mark.metadata
def test_list_views(connection, view):
    expected = {view}
    actual = {
        view["VIEW_NAME"]
        for view in connection.meta.list_views(view_schema_pattern="PYEXASOL%")
    }
    assert actual == expected


@pytest.mark.metadata
def test_list_columns(connection):
    expected = {"USER_NAME", "USER_ID", "USER_SCORE", "USER_RATING"}
    actual = {
        columns["COLUMN_NAME"]
        for columns in connection.meta.list_columns(
            column_schema_pattern="PYEXASOL%",
            column_table_pattern="USERS%",
            column_name_pattern="%USER%",
        )
    }
    assert actual == expected


@pytest.mark.metadata
def test_list_objects(connection):
    expected = {"USERS"}
    actual = {
        db_object["OBJECT_NAME"]
        for db_object in connection.meta.list_objects(
            object_name_pattern="%USER%",
        )
    }
    assert actual == expected


@pytest.mark.metadata
def test_list_object_sizes(connection):
    sizes = [
        db_object["MEM_OBJECT_SIZE"]
        for db_object in connection.meta.list_object_sizes(
            object_name_pattern="USERS%", object_type_pattern="TABLE"
        )
    ]
    do_all_objects_have_a_size = all(map(lambda size: size != 0, sizes))
    assert do_all_objects_have_a_size


@pytest.mark.metadata
def test_list_indices(connection):
    expected = [
        {
            "INDEX_OWNER": "SYS",
            "INDEX_SCHEMA": "PYEXASOL_TEST",
            "INDEX_TABLE": "PAYMENTS",
            "INDEX_TYPE": "GLOBAL",
        }
    ]
    actual = [
        {
            "INDEX_OWNER": index["INDEX_OWNER"],
            "INDEX_SCHEMA": index["INDEX_SCHEMA"],
            "INDEX_TABLE": index["INDEX_TABLE"],
            "INDEX_TYPE": index["INDEX_TYPE"],
        }
        for index in connection.meta.list_indices(index_schema_pattern="PYEXASOL%")
    ]
    assert actual == expected


@pytest.mark.metadata
def test_list_keywords(connection, expected_reserved_words):
    expected = expected_reserved_words
    actual = set(connection.meta.list_sql_keywords())
    assert actual == expected
