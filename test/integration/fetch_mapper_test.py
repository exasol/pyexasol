import pytest

from pyexasol.data_types import convert_websocket_to_python


# For the fetch_mapper tests we need to configure the connection accordingly
@pytest.fixture
def fetch_mapper_connection(connection_factory):
    con = connection_factory(fetch_mapper=convert_websocket_to_python)
    yield con
    con.close()


@pytest.mark.fetch_mapper
def test_fetch_mapper_with_convert_websocket_to_python(
    fetch_mapper_connection, data_type_case
):
    result = fetch_mapper_connection.execute(
        data_type_case.sql_template.format(data_type_case.websocket_value)
    )
    actual = result.fetchval()
    assert actual == data_type_case.python_value
    assert type(actual) is type(data_type_case.python_value)


@pytest.mark.fetch_mapper
def test_without_fetch_mapper(connection, data_type_case):
    result = connection.execute(
        data_type_case.sql_template.format(data_type_case.websocket_value)
    )
    actual = result.fetchval()
    assert actual == data_type_case.websocket_value
    assert type(actual) is type(data_type_case.websocket_value)
