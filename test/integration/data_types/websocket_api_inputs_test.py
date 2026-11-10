"""
Inspect the values the Exasol WebSocket API accepts as input.

These tests bind raw strings through the WebSocket API and check whether the
database accepts them, how it stores them, and which inputs it rejects.
"""

from contextlib import contextmanager

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


@contextmanager
def override_session_parameter(connection, name, value):
    """Set a session parameter such as NLS_DATE_FORMAT and restore it afterwards."""
    original_value = connection.execute(
        f"SELECT SESSION_VALUE FROM EXA_PARAMETERS WHERE PARAMETER_NAME = '{name}';"
    ).fetchval()
    connection.execute(f"ALTER SESSION SET {name}='{value}';")
    try:
        yield
    finally:
        connection.execute(f"ALTER SESSION SET {name}='{original_value}';")


# Shared by the TIMESTAMP and TIMESTAMP WITH LOCAL TIME ZONE tests. They rely on the
# default session NLS_TIMESTAMP_FORMAT, 'YYYY-MM-DD HH24:MI:SS.FF6'.
TIMESTAMP_ACCEPTED_STRINGS = [
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
]
TIMESTAMP_REJECTED_STRINGS = [
    pytest.param("2026-09-11 12:34:56.1234567890", id="precision-higher-than-db"),
    pytest.param("2026-09-11 12:34:56+00:00", id="string-has-unsupported-format"),
]


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
def timestamp_ltz_table(connection, db_version):
    # Mirrors timestamp_table: before Exasol 8.32.0 the precision cannot be declared.
    column_type = (
        "TIMESTAMP(3) WITH LOCAL TIME ZONE"
        if db_version >= Version("8.32.0")
        else "TIMESTAMP WITH LOCAL TIME ZONE"
    )
    table = SingleColumnTable.create_empty(
        connection, "WS_TIMESTAMP_LTZ_INPUT", column_type
    )
    yield table
    table.drop()


@pytest.fixture
def session_timestamp_format(connection):
    with override_session_parameter(
        connection, "NLS_TIMESTAMP_FORMAT", "DD/MM/YYYY HH24:MI:SS.FF4"
    ):
        yield


@pytest.fixture
def date_table(connection):
    table = SingleColumnTable.create_empty(connection, "WS_DATE_INPUT", "DATE")
    yield table
    table.drop()


@pytest.fixture
def session_date_format(connection):
    with override_session_parameter(connection, "NLS_DATE_FORMAT", "DD/MM/YYYY"):
        yield


@pytest.fixture
def interval_table(connection):
    table = SingleColumnTable.create_empty(
        connection, "WS_INTERVAL_INPUT", "INTERVAL DAY(9) TO SECOND(9)"
    )
    yield table
    table.drop()


@pytest.fixture
def decimal_table(connection):
    table = SingleColumnTable.create_empty(
        connection, "WS_DECIMAL_INPUT", "DECIMAL(10,2)"
    )
    yield table
    table.drop()


@pytest.fixture
def wide_decimal_table(connection):
    table = SingleColumnTable.create_empty(
        connection, "WS_WIDE_DECIMAL_INPUT", "DECIMAL(36,0)"
    )
    yield table
    table.drop()


@pytest.fixture
def double_table(connection):
    table = SingleColumnTable.create_empty(connection, "WS_DOUBLE_INPUT", "DOUBLE")
    yield table
    table.drop()


@pytest.fixture
def boolean_table(connection):
    table = SingleColumnTable.create_empty(connection, "WS_BOOLEAN_INPUT", "BOOLEAN")
    yield table
    table.drop()


@pytest.fixture
def hashtype_table(connection):
    table = SingleColumnTable.create_empty(
        connection, "WS_HASHTYPE_INPUT", "HASHTYPE(16 BYTE)"
    )
    yield table
    table.drop()


@pytest.fixture(params=["VARCHAR(5)", "CHAR(5)"], ids=["varchar", "char"])
def string_column_type(request):
    return request.param


@pytest.fixture
def string_table(connection, string_column_type):
    table = SingleColumnTable.create_empty(
        connection, "WS_STRING_INPUT", string_column_type
    )
    yield table
    table.drop()


@pytest.fixture
def geometry_table(connection):
    table = SingleColumnTable.create_empty(connection, "WS_GEOMETRY_INPUT", "GEOMETRY")
    yield table
    table.drop()


@pytest.fixture
def interval_year_to_month_table(connection):
    table = SingleColumnTable.create_empty(
        connection, "WS_INTERVAL_YM_INPUT", "INTERVAL YEAR(2) TO MONTH"
    )
    yield table
    table.drop()


@pytest.fixture
def session_numeric_characters(connection):
    result = connection.execute(
        "SELECT SESSION_VALUE FROM EXA_PARAMETERS "
        "WHERE PARAMETER_NAME = 'NLS_NUMERIC_CHARACTERS';"
    )
    original_characters = result.fetchval()
    connection.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS=',.';")
    yield
    connection.execute(
        f"ALTER SESSION SET NLS_NUMERIC_CHARACTERS='{original_characters}';"
    )


class TestTimestampInputs:
    # The session format test changes NLS_TIMESTAMP_FORMAT temporarily through the
    # session_timestamp_format fixture and restores it afterwards.
    @staticmethod
    @pytest.mark.parametrize("value, expected_stored", TIMESTAMP_ACCEPTED_STRINGS)
    def test_accepts_string_and_stores_expected_value(
        timestamp_table, value, expected_stored
    ):
        timestamp_table.insert(value)

        assert timestamp_table.read() == expected_stored

    @staticmethod
    @pytest.mark.parametrize("value", TIMESTAMP_REJECTED_STRINGS)
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


class TestTimestampLocalTimeZoneInputs:
    # The same string cases as for TIMESTAMP. The string has no time zone: the
    # database reads it in the session's TIME_ZONE.
    @staticmethod
    @pytest.mark.parametrize("value, expected_stored", TIMESTAMP_ACCEPTED_STRINGS)
    def test_accepts_string_and_stores_expected_value(
        timestamp_ltz_table, value, expected_stored
    ):
        timestamp_ltz_table.insert(value)

        assert timestamp_ltz_table.read() == expected_stored

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            *TIMESTAMP_REJECTED_STRINGS,
            # An offset or zone marker is rejected, as for TIMESTAMP.
            pytest.param("2026-09-11 12:34:56 +02:00", id="string-has-offset"),
            pytest.param("2026-09-11 12:34:56Z", id="string-has-zulu"),
        ],
    )
    def test_rejects_string(timestamp_ltz_table, value):
        with pytest.raises(ExaRequestError):
            timestamp_ltz_table.insert(value)

    @staticmethod
    def test_accepts_session_nls_timestamp_format(
        timestamp_ltz_table, session_timestamp_format
    ):
        timestamp_ltz_table.insert("11/09/2026 12:34:56.1234")

        # The column keeps 3 fractional digits; the session format pads them to 4.
        assert timestamp_ltz_table.read() == "11/09/2026 12:34:56.1230"

    @staticmethod
    def test_interprets_string_in_session_time_zone(connection, timestamp_ltz_table):
        with override_session_parameter(connection, "TIME_ZONE", "UTC"):
            timestamp_ltz_table.insert("2026-09-11 12:34:56")
        with override_session_parameter(connection, "TIME_ZONE", "EUROPE/BERLIN"):
            # Berlin is UTC+2 in September, so the same instant reads 2 hours later.
            assert timestamp_ltz_table.read() == "2026-09-11 14:34:56.000000"

    @staticmethod
    def test_shifts_string_in_skipped_local_time(connection, timestamp_ltz_table):
        # Clocks in Berlin jump from 02:00 to 03:00 on 2026-03-29, so 02:30 does not
        # exist. The database does not reject it, but moves it one hour forward.
        with override_session_parameter(connection, "TIME_ZONE", "EUROPE/BERLIN"):
            timestamp_ltz_table.insert("2026-03-29 02:30:00")

            assert timestamp_ltz_table.read() == "2026-03-29 03:30:00.000000"


class TestDateInputs:
    @staticmethod
    @pytest.mark.parametrize(
        "value, expected_stored",
        [
            pytest.param("2026-09-11", "2026-09-11", id="iso-format"),
            pytest.param("2026-9-1", "2026-09-01", id="unpadded-fields"),
            # A trailing time part is accepted and discarded by the database.
            pytest.param(
                "2026-09-11 12:34:56", "2026-09-11", id="string-has-time-part"
            ),
            pytest.param(
                "2026-09-11 12:34:56.123456",
                "2026-09-11",
                id="string-has-time-part-with-fraction",
            ),
            pytest.param("", None, id="empty-string"),
            pytest.param(None, None, id="none"),
        ],
    )
    def test_accepts_and_stores_expected_value(date_table, value, expected_stored):
        date_table.insert(value)

        assert date_table.read() == expected_stored

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("2026-09-11T12:34:56", id="string-has-T"),
            pytest.param("2026-02-30", id="day-out-of-range"),
            pytest.param("2026-13-01", id="month-out-of-range"),
            pytest.param("2023-02-29", id="leap-day-in-non-leap-year"),
            pytest.param("11/09/2026", id="string-has-unsupported-format"),
        ],
    )
    def test_rejects_string(date_table, value):
        with pytest.raises(ExaRequestError):
            date_table.insert(value)

    @staticmethod
    def test_accepts_session_nls_date_format(date_table, session_date_format):
        date_table.insert("11/09/2026")

        assert date_table.read() == "11/09/2026"


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


class TestDecimalInputs:
    # The column is DECIMAL(10,2). The database returns DECIMAL values as strings
    # without trailing zeros.
    @staticmethod
    @pytest.mark.parametrize(
        "value, expected_stored",
        [
            pytest.param(None, None, id="none"),
            pytest.param(-3, "-3", id="int"),
            pytest.param(1.51, "1.51", id="float"),
            # Scale beyond the column's is truncated, not rounded.
            pytest.param(1.555, "1.55", id="greater-scale-truncated"),
            pytest.param("1", "1", id="string"),
            pytest.param("", None, id="string-empty"),
            pytest.param("-1.5", "-1.5", id="string-negative"),
            pytest.param("+1.5", "1.5", id="string-positive-sign"),
            pytest.param(".5", "0.5", id="string-no-padding"),
            pytest.param("  1.5 ", "1.5", id="string-surrounding-whitespace"),
            pytest.param("1E+3", "1000", id="string-exponent-upper"),
            pytest.param("1e3", "1000", id="string-exponent-lower"),
            # Only the numeric value must fit the column, not the string length.
            pytest.param(
                "0" * 50 + "1.5",
                "1.5",
                id="string-leading-zeros-beyond-precision",
            ),
            pytest.param(
                "1." + "5" * 50,
                "1.55",
                id="string-fraction-digits-beyond-scale",
            ),
        ],
    )
    def test_accepts_and_stores_expected_value(decimal_table, value, expected_stored):
        decimal_table.insert(value)

        assert decimal_table.read() == expected_stored

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("1,5", id="string-comma-separator"),
            pytest.param("abc", id="string-not-a-number"),
            pytest.param("NaN", id="string-nan"),
            pytest.param("Infinity", id="string-infinity"),
            pytest.param("1_000", id="string-underscore"),
            # DECIMAL(10,2) leaves 8 digits for the integer part, so a 9-digit int
            # does not fit. The value is rejected even though it is a valid number.
            pytest.param(123456789, id="int-precision-overflow"),
            pytest.param(True, id="bool-true"),
        ],
    )
    def test_rejects_value(decimal_table, value):
        with pytest.raises(ExaRequestError):
            decimal_table.insert(value)

    @staticmethod
    def test_accepts_session_nls_numeric_characters(
        decimal_table, session_numeric_characters
    ):
        decimal_table.insert("1,5")

        # The value is also returned with the session's decimal separator.
        assert decimal_table.read() == "1,5"

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            # decimal_to_websocket always formats with "." as the decimal separator.
            # Under ',.' it is a group separator, so the database rejects the value.
            pytest.param("1.5", id="converted-decimal-with-fraction"),
            pytest.param("12.25", id="converted-decimal-with-fraction-two-digits"),
        ],
    )
    def test_rejects_converted_fraction_with_session_nls_numeric_characters(
        decimal_table, session_numeric_characters, value
    ):
        with pytest.raises(ExaRequestError):
            decimal_table.insert(value)


class TestWideDecimalInputs:
    @staticmethod
    @pytest.mark.parametrize(
        "value, expected_stored",
        [
            # Still exact as JSON numbers: 2**53 + 1 has 16 digits, and 10**35 is a
            # one followed by zeros. These show the loss is not simply "any int
            # above 2**53", but depends on how many significant digits it has.
            pytest.param(2**53 + 1, str(2**53 + 1), id="int-above-2-53"),
            pytest.param(str(10**36 - 1), str(10**36 - 1), id="string-max-precision"),
            # A large int bound as a JSON number loses precision on its way to
            # the column. This is not a database storage issue, as when sent as a
            # SQL literals and strings with the same value are stored exactly. The
            # loss happens in between, while the server handles the JSON number.
            # This is why a Decimal converter should send a string, not a number.
            pytest.param(
                12345678901234567890123456789012345,
                "12345678901234567000000000000000000",
                id="int-loses-precision-server-side",
            ),
            # The same value sent as a string is stored exactly.
            pytest.param(
                "12345678901234567890123456789012345",
                "12345678901234567890123456789012345",
                id="string-same-value-stays-exact",
            ),
        ],
    )
    def test_accepts_and_stores_expected_value(
        wide_decimal_table, value, expected_stored
    ):
        wide_decimal_table.insert(value)

        assert wide_decimal_table.read() == expected_stored

    @staticmethod
    def test_rejects_string_precision_overflow(wide_decimal_table):
        with pytest.raises(ExaRequestError):
            wide_decimal_table.insert(str(10**36))


class TestDoubleInputs:
    # These cases mirror the DECIMAL ones. The column is DOUBLE, which accepts only
    # a JSON float (or None / empty string). The database returns DOUBLE values as
    # floats.
    @staticmethod
    @pytest.mark.parametrize(
        "value, expected_stored",
        [
            pytest.param(None, None, id="none"),
            pytest.param(1.555, 1.555, id="greater-scale-kept"),
            # Only empty string works. See the later test for rejected numeric strings.
            pytest.param("", None, id="string-empty"),
            # A large int is accepted, but only its first 17 significant digits
            # arrive, as for DECIMAL. Converting it to a float in Python first
            # avoids this (see the case below).
            pytest.param(
                12345678901234567890123456789012345,
                1.2345678901234567e34,
                id="int-35-digits-loses-precision-server-side",
            ),
            # The same value converted to a float in Python is stored exactly.
            pytest.param(
                float(12345678901234567890123456789012345),
                float(12345678901234567890123456789012345),
                id="int-35-digits-as-float-stays-exact",
            ),
            # DOUBLE-specific cases.
            pytest.param(1e308, 1e308, id="large-exponent"),
            pytest.param(1e-320, 1e-320, id="subnormal"),
        ],
    )
    def test_accepts_and_stores_expected_value(double_table, value, expected_stored):
        double_table.insert(value)

        assert double_table.read() == expected_stored

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            # Every non-empty string is rejected, even a plain number.
            pytest.param("1", id="numeric-string"),
            # A small int is rejected as well, even though a large one is accepted.
            pytest.param(-3, id="int"),
            pytest.param(True, id="bool-true"),
            # DOUBLE-specific cases.
            pytest.param(float("nan"), id="float-nan"),
            pytest.param(float("inf"), id="float-inf"),
        ],
    )
    def test_rejects_value(double_table, value):
        with pytest.raises(ExaRequestError):
            double_table.insert(value)

    @staticmethod
    def test_accepts_float_with_session_nls_numeric_characters(
        double_table, session_numeric_characters
    ):
        # Unlike DECIMAL, a float is a JSON number and does not depend on the
        # session's decimal separator.
        double_table.insert(1.5)

        assert double_table.read() == 1.5


class TestBooleanInputs:
    @staticmethod
    @pytest.mark.parametrize(
        "value, expected_stored",
        [
            pytest.param(True, True, id="bool-true"),
            pytest.param(False, False, id="bool-false"),
            pytest.param(None, None, id="none"),
            # Only the empty string works. See the later test for rejected strings.
            pytest.param("", None, id="string-empty"),
        ],
    )
    def test_accepts_and_stores_expected_value(boolean_table, value, expected_stored):
        boolean_table.insert(value)

        assert boolean_table.read() == expected_stored

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            # Every non-empty string is rejected, even a boolean literal the Exasol
            # docs list ('TRUE', 'T', '1', ...).
            pytest.param("TRUE", id="string"),
            # Likewise a number the docs list as a boolean literal (1, 0).
            pytest.param(1, id="int"),
            pytest.param(1.0, id="float"),
        ],
    )
    def test_rejects_value(boolean_table, value):
        with pytest.raises(ExaRequestError):
            boolean_table.insert(value)


class TestHashtypeInputs:
    # The column is HASHTYPE(16 BYTE), the size of a UUID. The database returns the
    # value as lowercase hex digits without dashes.
    UUID_HEX = "12345678123456781234567812345678"
    # The same value as base64, from the Exasol docs.
    BASE64_HEX = "550e8400e29b11d4a716446655440000"
    # Its base64 characters are '+' and '/', which base64url writes as '-' and '_'.
    URL_SAFE_HEX = "fbffbffbffbffbffbffbffbffbffbff9"

    @staticmethod
    @pytest.mark.parametrize(
        "value, expected_stored",
        [
            pytest.param(None, None, id="none"),
            pytest.param("", None, id="string-empty"),
            pytest.param(UUID_HEX, UUID_HEX, id="string-hex"),
            # The dashes are ignored wherever they are.
            pytest.param(
                "123456781234-5678-1234-567812345678",
                UUID_HEX,
                id="string-with-dashes-anywhere",
            ),
            pytest.param(
                "{12345678-1234-5678-1234-567812345678}",
                UUID_HEX,
                id="string-with-braces",
            ),
            # Base64 and base64url strings are accepted too, with or without padding.
            pytest.param("VQ6EAOKbEdSnFkRmVUQAAA", BASE64_HEX, id="string-base64"),
            pytest.param(
                "VQ6EAOKbEdSnFkRmVUQAAA==", BASE64_HEX, id="string-base64-padded"
            ),
            pytest.param("-_-_-_-_-_-_-_-_-_-_-Q", URL_SAFE_HEX, id="string-base64url"),
        ],
    )
    def test_accepts_and_stores_expected_value(hashtype_table, value, expected_stored):
        hashtype_table.insert(value)

        assert hashtype_table.read() == expected_stored

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(UUID_HEX[:-2], id="string-too-short"),
            pytest.param(UUID_HEX + "00", id="string-too-long"),
            pytest.param(UUID_HEX.replace("1", "g"), id="string-not-hex"),
            pytest.param(f" {UUID_HEX} ", id="string-with-whitespace"),
            pytest.param(f"urn:uuid:{UUID_HEX}", id="string-with-urn-prefix"),
            # No non-string is accepted, even an int with the value of the UUID.
            pytest.param(int(UUID_HEX, 16), id="int"),
            pytest.param(1.5, id="float"),
            pytest.param(True, id="bool"),
        ],
    )
    def test_rejects_value(hashtype_table, value):
        with pytest.raises(ExaRequestError):
            hashtype_table.insert(value)


class TestStringInputs:
    # The tests run for VARCHAR(5) and CHAR(5). CHAR pads the value with spaces.
    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("ab", id="shorter-than-column"),
            pytest.param("ab de", id="same-length-as-column"),
            # The length is counted in characters, not bytes.
            pytest.param("äöüèé", id="multi-byte-characters"),
            pytest.param("😀" * 5, id="four-byte-characters"),
            pytest.param("a\x00b", id="embedded-nul"),
            pytest.param("\n", id="newline"),
        ],
    )
    def test_accepts_and_stores_expected_value(string_table, string_column_type, value):
        string_table.insert(value)

        is_char = string_column_type.startswith("CHAR")
        assert string_table.read() == (value.ljust(5) if is_char else value)

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(None, id="none"),
            pytest.param("", id="string-empty"),
        ],
    )
    def test_stores_null(string_table, value):
        string_table.insert(value)

        assert string_table.read() is None

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("abcdef", id="longer-than-column"),
            # No non-string is accepted, even one that is short enough.
            pytest.param(12345, id="int"),
            pytest.param(1.5, id="float"),
            pytest.param(True, id="bool"),
        ],
    )
    def test_rejects_value(string_table, value):
        with pytest.raises(ExaRequestError):
            string_table.insert(value)


class TestGeometryInputs:
    # GEOMETRY takes WKT strings. The database returns them in a normalized form.
    @staticmethod
    @pytest.mark.parametrize(
        "value, expected_stored",
        [
            pytest.param("POINT (1 2)", "POINT (1 2)", id="point"),
            pytest.param("POINT(1 2)", "POINT (1 2)", id="point-without-space"),
            pytest.param("point(1 2)", "POINT (1 2)", id="lower-case"),
            pytest.param("POINT EMPTY", "POINT EMPTY", id="empty"),
            pytest.param(" POINT(1 2) ", "POINT (1 2)", id="surrounding-whitespace"),
            pytest.param(None, None, id="none"),
            pytest.param("", None, id="string-empty"),
        ],
    )
    def test_accepts_string_and_stores_expected_value(
        geometry_table, value, expected_stored
    ):
        geometry_table.insert(value)

        assert geometry_table.read() == expected_stored

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("abc", id="string-not-wkt"),
            pytest.param("POINT(1)", id="string-missing-coordinate"),
            pytest.param("POLYGON((0 0, 1 0, 1 1))", id="string-polygon-not-closed"),
            pytest.param("POINT(1 2 3)", id="string-three-coordinates"),
            pytest.param("SRID=4326;POINT(1 2)", id="string-ewkt-srid"),
            pytest.param(1, id="int"),
            pytest.param(1.5, id="float"),
            pytest.param(True, id="bool"),
        ],
    )
    def test_rejects_value(geometry_table, value):
        with pytest.raises(ExaRequestError):
            geometry_table.insert(value)


class TestIntervalYearToMonthInputs:
    # The column is INTERVAL YEAR(2) TO MONTH. The database returns '[+-]YY-MM'.
    @staticmethod
    @pytest.mark.parametrize(
        "value, expected_stored",
        [
            pytest.param("1-2", "+01-02", id="unpadded-fields"),
            pytest.param("01-02", "+01-02", id="padded-fields"),
            pytest.param("+1-2", "+01-02", id="positive-sign"),
            pytest.param("-1-2", "-01-02", id="negative-sign"),
            pytest.param(" 1-2 ", "+01-02", id="surrounding-whitespace"),
            pytest.param(None, None, id="none"),
            pytest.param("", None, id="empty-string"),
        ],
    )
    def test_accepts_string_and_stores_expected_value(
        interval_year_to_month_table, value, expected_stored
    ):
        interval_year_to_month_table.insert(value)

        assert interval_year_to_month_table.read() == expected_stored

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            # The month part is 0 to 11, and the year part has at most 2 digits.
            pytest.param("1-12", id="string-month-too-large"),
            pytest.param("100-0", id="string-year-too-large"),
            pytest.param("1", id="string-without-month"),
            pytest.param("1 2", id="string-wrong-separator"),
            pytest.param("P1Y2M", id="string-iso-8601"),
            pytest.param("abc", id="string-not-an-interval"),
            # A number of months is rejected as well.
            pytest.param(14, id="int"),
        ],
    )
    def test_rejects_value(interval_year_to_month_table, value):
        with pytest.raises(ExaRequestError):
            interval_year_to_month_table.insert(value)
