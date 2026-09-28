import datetime

import pytest

from exasol.driver.websocket._types import TypeCode
from pyexasol.mapper import (
    ExaTimeDelta,
    exasol_mapper,
)

MAPPER_CASES = [
    ("ALL_NONE_CASE", None, None),
    ("DECIMAL", "123", 123),
    (
        "DATE",
        "2026-09-11",
        datetime.date(2026, 9, 11),
    ),
    (
        "TIMESTAMP",
        "2026-09-11 12:34:56",
        datetime.datetime(2026, 9, 11, 12, 34, 56),
    ),
    (
        "INTERVAL DAY TO SECOND",
        "+000000003 10:59:59.123000000",
        ExaTimeDelta(days=3, hours=10, minutes=59, seconds=59, microseconds=123000),
    ),
    ("DOUBLE", 1.25, 1.25),
    ("BOOLEAN", True, True),
    ("VARCHAR", "text", "text"),
    ("CHAR", "text", "text"),
    ("HASHTYPE", "hash", "hash"),
    ("GEOMETRY", "POINT (10 20)", "POINT (10 20)"),
    ("INTERVAL YEAR TO MONTH", "+000000001-02", "+000000001-02"),
    (
        "TIMESTAMP WITH LOCAL TIME ZONE",
        "2026-09-11 12:34:56",
        "2026-09-11 12:34:56",
    ),
]


class TestExasolMapper:
    @staticmethod
    @pytest.mark.parametrize(
        "type_name,value,expected",
        MAPPER_CASES,
    )
    def test_maps_types(type_name, value, expected):
        data_type = {"type": type_name, "scale": 0}
        assert exasol_mapper(value, data_type) == expected

    @staticmethod
    def test_maps_all_type_codes():
        tested_type_names = {type_name for type_name, _, _ in MAPPER_CASES}
        expected_type_names = {type_code.value for type_code in TypeCode}

        assert tested_type_names - {"ALL_NONE_CASE"} == expected_type_names
