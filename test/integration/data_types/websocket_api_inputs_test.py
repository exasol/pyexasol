"""
Inspect the values the Exasol WebSocket API accepts as input.

These tests bind raw strings through the WebSocket API and check whether the
database accepts them, how it stores them, and which inputs it rejects.
"""

import pytest
from packaging.version import Version

from pyexasol import ExaRequestError


class SingleColumnTable:
    """A one-column table that binds raw values as strings and reads them back."""

    def __init__(self, connection, name):
        self.connection = connection
        self.name = name

    @classmethod
    def create_empty(cls, connection, name, column_type):
        connection.execute(f"CREATE OR REPLACE TABLE {name} (V {column_type});")
        return cls(connection, name)

    def drop(self):
        self.connection.execute(f"DROP TABLE IF EXISTS {self.name};")

    def insert(self, value):
        statement = self.connection.create_prepared_statement(
            f"INSERT INTO {self.name} VALUES (?);"
        )
        statement.execute_prepared([(value,)])

    def read(self):
        result = self.connection.execute(f"SELECT V FROM {self.name};")
        return result.fetchone()[0]


@pytest.fixture
def timestamp_table(connection, db_version):
    # Exasol 8 versions before 8.32.0 only support TIMESTAMP with the default of 3
    # fractional digits and do not accept a declared precision. Other versions do
    # support explicit declaration.
    column_type = "TIMESTAMP(3)" if db_version >= Version("8.32.0") else "TIMESTAMP"
    table = SingleColumnTable.create_empty(
        connection, "WS_TIMESTAMP_INPUT", column_type
    )
    yield table
    table.drop()


@pytest.fixture
def session_timestamp_format(connection):
    result = connection.execute(
        "SELECT SESSION_VALUE FROM EXA_PARAMETERS "
        "WHERE PARAMETER_NAME = 'NLS_TIMESTAMP_FORMAT';"
    )
    original_format = result.fetchval()
    connection.execute(
        "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='DD/MM/YYYY HH24:MI:SS.FF4';"
    )
    yield
    connection.execute(f"ALTER SESSION SET NLS_TIMESTAMP_FORMAT='{original_format}';")


@pytest.fixture
def interval_table(connection):
    table = SingleColumnTable.create_empty(
        connection, "WS_INTERVAL_INPUT", "INTERVAL DAY(9) TO SECOND(9)"
    )
    yield table
    table.drop()


class TestTimestampInputs:
    # These cases rely on the default session NLS_TIMESTAMP_FORMAT,
    # 'YYYY-MM-DD HH24:MI:SS.FF6'. The session format test changes it temporarily
    # through the session_timestamp_format fixture and restores it afterwards.
    @staticmethod
    @pytest.mark.parametrize(
        "value, expected_stored",
        [
            pytest.param(
                "2026-09-11 12:34:56",
                "2026-09-11 12:34:56.000000",
                id="number-is-less-precise",
            ),
            pytest.param(
                "2026-09-11 12:34:56.123",
                "2026-09-11 12:34:56.123000",
                id="same-as-precision",
            ),
            pytest.param(
                "2026-09-11 12:34:56.123456789",
                "2026-09-11 12:34:56.123000",
                id="greater-precision",
            ),
            pytest.param(
                "2026-9-1 1:2:3",
                "2026-09-01 01:02:03.000000",
                id="unpadded-fields",
            ),
        ],
    )
    def test_accepts_string_and_stores_expected_value(
        timestamp_table, value, expected_stored
    ):
        timestamp_table.insert(value)

        assert timestamp_table.read() == expected_stored

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(
                "2026-09-11 12:34:56.1234567890", id="precision-higher-than-db"
            ),
            pytest.param(
                "2026-09-11 12:34:56+00:00", id="string-has-unsupported-format"
            ),
        ],
    )
    def test_rejects_string(timestamp_table, value):
        with pytest.raises(ExaRequestError):
            timestamp_table.insert(value)

    @staticmethod
    def test_accepts_session_nls_timestamp_format(
        timestamp_table, session_timestamp_format
    ):
        timestamp_table.insert("11/09/2026 12:34:56.1234")

        # The column keeps 3 fractional digits; the session format pads them to 4.
        assert timestamp_table.read() == "11/09/2026 12:34:56.1230"


class TestIntervalDayToSecondInputs:
    @staticmethod
    @pytest.mark.parametrize(
        "value, expected_stored",
        [
            # Fields do not need to be zero-padded; the database pads days, hours,
            # minutes and seconds when storing the value.
            pytest.param(
                "+1 2:3:4", "+000000001 02:03:04.000000000", id="positive-sign"
            ),
            # The column supports up to 9 fractional-second digits, but Exasol
            # documents the accuracy as limited to milliseconds, so the rest is dropped.
            pytest.param(
                "1 02:03:04.1234567", "+000000001 02:03:04.123000000", id="no-sign"
            ),
            pytest.param(
                "-1 02:03:04.5", "-000000001 02:03:04.500000000", id="negative-sign"
            ),
            pytest.param("", None, id="empty-string"),
            pytest.param(None, None, id="none"),
        ],
    )
    def test_accepts_string_and_stores_expected_value(
        interval_table, value, expected_stored
    ):
        interval_table.insert(value)

        assert interval_table.read() == expected_stored

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("+0 25:00:00", id="hours-out-of-range"),
            pytest.param("P1DT2H", id="iso-8601-duration"),
            # The total string length must fit the width of the column's declared
            # precision (day digits plus fractional-second digits), or the database
            # raises an error.
            pytest.param(
                "0" * 20 + "1 02:03:04", id="string-longer-than-allowed-total-length"
            ),
        ],
    )
    def test_rejects_string(interval_table, value):
        with pytest.raises(ExaRequestError):
            interval_table.insert(value)
