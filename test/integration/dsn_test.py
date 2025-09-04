import pytest

from pyexasol import ExaConnectionError


@pytest.mark.configuration
def test_ip_range_with_custom_Port(connection):
    dsn = "127.0.0.10..19:8564"
    expected = {
        ("127.0.0.10", "127.0.0.10", 8564, None),
        ("127.0.0.11", "127.0.0.11", 8564, None),
        ("127.0.0.12", "127.0.0.12", 8564, None),
        ("127.0.0.13", "127.0.0.13", 8564, None),
        ("127.0.0.14", "127.0.0.14", 8564, None),
        ("127.0.0.15", "127.0.0.15", 8564, None),
        ("127.0.0.16", "127.0.0.16", 8564, None),
        ("127.0.0.17", "127.0.0.17", 8564, None),
        ("127.0.0.18", "127.0.0.18", 8564, None),
        ("127.0.0.19", "127.0.0.19", 8564, None),
    }
    actual = set(connection._process_dsn(dsn))
    assert actual == expected


@pytest.mark.configuration
def test_multiple_ranges_with_multiple_ports_and_default_port_at_the_end(connection):
    dsn = "127.0.0.10..19:8564,127.0.0.20,localhost:8565,127.0.0.21..23"
    expected = {
        ("127.0.0.10", "127.0.0.10", 8564, None),
        ("127.0.0.11", "127.0.0.11", 8564, None),
        ("127.0.0.12", "127.0.0.12", 8564, None),
        ("127.0.0.13", "127.0.0.13", 8564, None),
        ("127.0.0.14", "127.0.0.14", 8564, None),
        ("127.0.0.15", "127.0.0.15", 8564, None),
        ("127.0.0.16", "127.0.0.16", 8564, None),
        ("127.0.0.17", "127.0.0.17", 8564, None),
        ("127.0.0.18", "127.0.0.18", 8564, None),
        ("127.0.0.19", "127.0.0.19", 8564, None),
        ("127.0.0.20", "127.0.0.20", 8565, None),
        ("127.0.0.21", "127.0.0.21", 8563, None),
        ("127.0.0.22", "127.0.0.22", 8563, None),
        ("127.0.0.23", "127.0.0.23", 8563, None),
        ("localhost", "127.0.0.1", 8565, None),
    }
    actual = set(connection._process_dsn(dsn))
    assert actual == expected


@pytest.mark.configuration
def test_multiple_ranges_with_fingerprint_and_port(connection):
    dsn = "127.0.0.10..19/ABC,127.0.0.20,localhost/CDE:8564"
    expected = {
        ("127.0.0.11", "127.0.0.11", 8564, "ABC"),
        ("127.0.0.19", "127.0.0.19", 8564, "ABC"),
        ("127.0.0.20", "127.0.0.20", 8564, "CDE"),
        ("127.0.0.17", "127.0.0.17", 8564, "ABC"),
        ("localhost", "127.0.0.1", 8564, "CDE"),
        ("127.0.0.12", "127.0.0.12", 8564, "ABC"),
        ("127.0.0.15", "127.0.0.15", 8564, "ABC"),
        ("127.0.0.14", "127.0.0.14", 8564, "ABC"),
        ("127.0.0.13", "127.0.0.13", 8564, "ABC"),
        ("127.0.0.16", "127.0.0.16", 8564, "ABC"),
        ("127.0.0.10", "127.0.0.10", 8564, "ABC"),
        ("127.0.0.18", "127.0.0.18", 8564, "ABC"),
    }
    actual = set(connection._process_dsn(dsn))
    assert actual == expected


@pytest.mark.configuration
def test_empty_dsn(connection):
    dsn = " "
    with pytest.raises(ExaConnectionError) as excinfo:
        connection._process_dsn(dsn)

    expected = "Connection string is empty"
    actual = excinfo.value.message
    assert actual == expected


@pytest.mark.configuration
def test_invalid_range(connection):
    dsn = "127.0.0.15..10"
    with pytest.raises(ExaConnectionError) as excinfo:
        connection._process_dsn(dsn)

    expected = (
        "Connection string part [127.0.0.15..10] contains an invalid range, "
        "lower bound is higher than upper bound"
    )
    actual = excinfo.value.message
    assert actual == expected


@pytest.mark.configuration
def test_hostname_cannot_be_resolved(connection):
    dsn = "test1..5.zlan"
    with pytest.raises(ExaConnectionError) as excinfo:
        connection._process_dsn(dsn)

    expected = (
        "Could not resolve IP address of hostname "
        "[test1.zlan] derived from connection string"
    )
    actual = excinfo.value.message
    assert actual == expected


@pytest.mark.configuration
def test_hostname_range_with_zero_padding(connection):
    dsn = "test01..20.zlan"
    with pytest.raises(ExaConnectionError) as excinfo:
        connection._process_dsn(dsn)

    expected = (
        "Could not resolve IP address of hostname "
        "[test01.zlan] derived from connection string"
    )
    actual = excinfo.value.message
    assert actual == expected


@pytest.mark.configuration
def test_invalid_fingerprint(connection):
    """
    This test only validates the wrong behavior of the DSN parser of pyexasol.
    See https://github.com/exasol/pyexasol/issues/237
    """
    dsn = "localhost:8563/1234"
    with pytest.raises(ExaConnectionError) as excinfo:
        connection._process_dsn(dsn)

    expected = (
        "Could not resolve IP address of hostname [localhost:8563]"
        " derived from connection string"
    )
    actual = excinfo.value.message
    assert actual == expected
