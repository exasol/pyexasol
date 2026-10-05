import pytest

from pyexasol.data_types import WebSocketDataType

# The WebSocket protocol returns some values the same as EXA_SQL_TYPES,
# but there are differences, which must be noted here.
ALLOWED_MISSING_TYPE_MAPPINGS = {
    "BIGINT": WebSocketDataType.Decimal,
    "INTEGER": WebSocketDataType.Decimal,
    "SMALLINT": WebSocketDataType.Decimal,
    "TINYINT": WebSocketDataType.Decimal,
    "FLOAT": WebSocketDataType.Double,
    "DOUBLE PRECISION": WebSocketDataType.Double,
    "LONG VARCHAR": WebSocketDataType.String,
}


@pytest.fixture
def database_types(connection):
    """Return the type names currently exposed by the database catalog."""
    result = connection.execute("SELECT TYPE_NAME FROM EXA_SQL_TYPES")
    return {row[0] for row in result.fetchall()}


@pytest.fixture
def websocket_types():
    return {data_type.value for data_type in WebSocketDataType}


class TestWebSocketDataTypes:
    @staticmethod
    def test_all_websocket_types_exist_in_database_catalog(
        database_types, websocket_types
    ):
        """Every supported WebSocket type must be represented by the catalog."""
        missing_database_types = websocket_types - database_types

        missing_database_types -= {
            data_type.value for data_type in ALLOWED_MISSING_TYPE_MAPPINGS.values()
        }
        assert not missing_database_types, (
            "WebSocketDataType entries missing from EXA_SQL_TYPES: "
            f"{sorted(missing_database_types)}"
        )

    @staticmethod
    def test_all_needed_database_types_declared(database_types, websocket_types):
        """Every needed database type must be in WebSocketDataType."""
        missing_websocket_types = database_types - websocket_types
        missing_websocket_types -= ALLOWED_MISSING_TYPE_MAPPINGS.keys()

        assert not missing_websocket_types, (
            "EXA_SQL_TYPES contains types not in WebSocketDataType: "
            f"{sorted(missing_websocket_types)}"
        )

    @staticmethod
    @pytest.mark.parametrize(
        "database_type, expected_websocket_type",
        ALLOWED_MISSING_TYPE_MAPPINGS.items(),
    )
    def test_allowed_database_type_mapping_is_reflected_in_result_metadata(
        connection, database_type, expected_websocket_type
    ):
        """Each allowed catalog difference must map to the expected WebSocket type."""
        statement = connection.execute(
            f"SELECT CAST(NULL AS {database_type}) AS TEST_VALUE"
        )
        column_metadata = statement.columns()["TEST_VALUE"]

        assert WebSocketDataType(column_metadata["type"]) is expected_websocket_type
