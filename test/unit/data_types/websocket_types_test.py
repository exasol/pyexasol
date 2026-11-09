from pyexasol.data_types import WebSocketDataType


def test_maps_all_type_codes(data_type_cases):
    tested_type_names = {
        data_type_case.websocket_data_type for data_type_case in data_type_cases
    }
    assert tested_type_names == set(WebSocketDataType)
