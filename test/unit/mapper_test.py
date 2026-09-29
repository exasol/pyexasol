import datetime
import decimal

import pytest

from exasol.driver.websocket.dbapi2 import TypeCode
from pyexasol.mapper import (
    ExaTimeDelta,
    exasol_mapper,
)

MAPPER_CASES = [
    ("DECIMAL", "123", 0, 123),
    ("DECIMAL", "123.45", 2, decimal.Decimal("123.45")),
    (
        "DATE",
        "2026-09-11",
        0,
        datetime.date(2026, 9, 11),
    ),
    (
        "TIMESTAMP",
        "2026-09-11 12:34:56",
        0,
        datetime.datetime(2026, 9, 11, 12, 34, 56),
    ),
    (
        "INTERVAL DAY TO SECOND",
        "+000000003 10:59:59.123000000",
        0,
        ExaTimeDelta(days=3, hours=10, minutes=59, seconds=59, microseconds=123000),
    ),
    ("DOUBLE", 1.25, 0, 1.25),
    ("BOOLEAN", True, 0, True),
    ("VARCHAR", "text", 0, "text"),
    ("CHAR", "text", 0, "text"),
    ("HASHTYPE", "hash", 0, "hash"),
    ("GEOMETRY", "POINT (10 20)", 0, "POINT (10 20)"),
    ("INTERVAL YEAR TO MONTH", "+000000001-02", 0, "+000000001-02"),
    (
        "TIMESTAMP WITH LOCAL TIME ZONE",
        "2026-09-11 12:34:56",
        0,
        "2026-09-11 12:34:56",
    ),
]


class TestExasolMapper:
    @staticmethod
    @pytest.mark.parametrize(
        "type_name,value,scale,expected",
        MAPPER_CASES,
    )
    def test_maps_types(type_name, value, scale, expected):
        data_type = {"type": type_name, "scale": scale}
        assert exasol_mapper(value, data_type) == expected

    @staticmethod
    def test_maps_all_type_codes():
        tested_type_names = {type_name for type_name, _, _, _ in MAPPER_CASES}
        expected_type_names = {type_code.value for type_code in TypeCode}

        assert tested_type_names == expected_type_names

    @staticmethod
    def test_maps_none():
        assert exasol_mapper(None, {"type": "DECIMAL", "scale": 0}) is None
