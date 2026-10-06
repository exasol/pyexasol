from unittest.mock import patch

import pytest

from pyexasol import ExaConnectionDsnError


class TestProcessDsn:
    """
    The DSN grammar separates the optional fingerprint with "/" and the optional
    port with ":". A DSN which puts these in the wrong order must be rejected by
    the parser instead of being folded into the hostname.
    """

    @staticmethod
    @pytest.mark.parametrize(
        "dsn",
        [
            "localhost:8563/1234",
            "localhost:8563/nocertcheck",
            "myexasol1..4.com:8563/abcdef",
        ],
    )
    def test_fingerprint_after_port_is_rejected(mock_exaconnection_factory, dsn):
        connection = mock_exaconnection_factory()

        with pytest.raises(ExaConnectionDsnError) as excinfo:
            connection._process_dsn(dsn)

        expected = f"Could not parse connection string part [{dsn}]"
        assert excinfo.value.message == expected

    @staticmethod
    @pytest.mark.parametrize(
        "dsn,expected",
        [
            ("localhost", [("localhost", 8563, None)]),
            ("localhost:8564", [("localhost", 8564, None)]),
            ("localhost/ABC", [("localhost", 8563, "ABC")]),
            ("localhost/ABC:8564", [("localhost", 8564, "ABC")]),
            (
                "127.0.0.1..2/CDE:8565",
                [("127.0.0.1", 8565, "CDE"), ("127.0.0.2", 8565, "CDE")],
            ),
            (
                "127.0.0.1..2/ABC:8564",
                [("127.0.0.1", 8564, "ABC"), ("127.0.0.2", 8564, "ABC")],
            ),
            ("my-host-1/ABC:8564", [("my-host-1", 8564, "ABC")]),
            ("my-host-1.com/ABC:8564", [("my-host-1.com", 8564, "ABC")]),
        ],
    )
    def test_valid_dsn_is_still_parsed(mock_exaconnection_factory, dsn, expected):
        # Hostname resolution is disabled, so that parsing can be verified in
        # isolation, without depending on DNS being able to resolve the examples.
        connection = mock_exaconnection_factory(resolve_hostnames=False)

        actual = {
            (host.hostname, host.port, host.fingerprint)
            for host in connection._process_dsn(dsn)
        }
        assert actual == set(expected)


class TestIpRanges:
    """Tests validating correct behavior of ExaConnection._process_dsn() using a mocked connection"""

    @staticmethod
    def test_ip_range_with_custom_port(mock_exaconnection_factory):
        connection = mock_exaconnection_factory(resolve_hostnames=False)
        dsn = "127.0.0.10..19:8564"
        expected = {
            ("127.0.0.10", 8564, None),
            ("127.0.0.11", 8564, None),
            ("127.0.0.12", 8564, None),
            ("127.0.0.13", 8564, None),
            ("127.0.0.14", 8564, None),
            ("127.0.0.15", 8564, None),
            ("127.0.0.16", 8564, None),
            ("127.0.0.17", 8564, None),
            ("127.0.0.18", 8564, None),
            ("127.0.0.19", 8564, None),
        }
        actual = {
            (host.hostname, host.port, host.fingerprint)
            for host in connection._process_dsn(dsn)
        }
        assert actual == expected

    @staticmethod
    def test_multiple_ranges_with_multiple_ports_and_default_port_at_the_end(
        mock_exaconnection_factory,
    ):
        connection = mock_exaconnection_factory(resolve_hostnames=False)
        dsn = "127.0.0.10..19:8564,127.0.0.20,127.0.0.100:8565,127.0.0.21..23"
        expected = {
            ("127.0.0.10", 8564, None),
            ("127.0.0.11", 8564, None),
            ("127.0.0.12", 8564, None),
            ("127.0.0.13", 8564, None),
            ("127.0.0.14", 8564, None),
            ("127.0.0.15", 8564, None),
            ("127.0.0.16", 8564, None),
            ("127.0.0.17", 8564, None),
            ("127.0.0.18", 8564, None),
            ("127.0.0.19", 8564, None),
            ("127.0.0.20", 8565, None),
            ("127.0.0.21", 8563, None),
            ("127.0.0.22", 8563, None),
            ("127.0.0.23", 8563, None),
            ("127.0.0.100", 8565, None),
        }
        actual = {
            (host.hostname, host.port, host.fingerprint)
            for host in connection._process_dsn(dsn)
        }
        assert actual == expected

    @staticmethod
    def test_multiple_ranges_with_fingerprint_and_port(mock_exaconnection_factory):
        connection = mock_exaconnection_factory(resolve_hostnames=False)
        dsn = "127.0.0.10..19/ABC,127.0.0.20,127.0.0.100/CDE:8564"
        expected = {
            ("127.0.0.10", 8564, "ABC"),
            ("127.0.0.11", 8564, "ABC"),
            ("127.0.0.12", 8564, "ABC"),
            ("127.0.0.13", 8564, "ABC"),
            ("127.0.0.14", 8564, "ABC"),
            ("127.0.0.15", 8564, "ABC"),
            ("127.0.0.16", 8564, "ABC"),
            ("127.0.0.17", 8564, "ABC"),
            ("127.0.0.18", 8564, "ABC"),
            ("127.0.0.19", 8564, "ABC"),
            ("127.0.0.20", 8564, "CDE"),
            ("127.0.0.100", 8564, "CDE"),
        }
        actual = {
            (host.hostname, host.port, host.fingerprint)
            for host in connection._process_dsn(dsn)
        }
        assert actual == expected

    @staticmethod
    def test_empty_dsn(mock_exaconnection_factory):
        connection = mock_exaconnection_factory()
        with pytest.raises(ExaConnectionDsnError) as excinfo:
            connection._process_dsn(" ")
        assert excinfo.value.message == "Connection string is empty"

    @staticmethod
    def test_invalid_range(mock_exaconnection_factory):
        connection = mock_exaconnection_factory(resolve_hostnames=False)
        dsn = "127.0.0.15..10"
        with pytest.raises(ExaConnectionDsnError) as excinfo:
            connection._process_dsn(dsn)
        expected = (
            "Connection string part [127.0.0.15..10] contains an invalid range, "
            "lower bound is higher than upper bound"
        )
        assert excinfo.value.message == expected

    @staticmethod
    def test_hostname_cannot_be_resolved(mock_exaconnection_factory):
        connection = mock_exaconnection_factory()
        dsn = "test1..5.zlan"
        with patch("socket.gethostbyname_ex", side_effect=OSError("Name not resolved")):
            with pytest.raises(ExaConnectionDsnError) as excinfo:
                connection._process_dsn(dsn)
        expected = (
            "Could not resolve IP address of hostname "
            "[test1.zlan] derived from connection string"
        )
        assert excinfo.value.message == expected

    @staticmethod
    def test_hostname_range_with_zero_padding(mock_exaconnection_factory):
        connection = mock_exaconnection_factory()
        dsn = "test01..20.zlan"
        with patch("socket.gethostbyname_ex", side_effect=OSError("Name not resolved")):
            with pytest.raises(ExaConnectionDsnError) as excinfo:
                connection._process_dsn(dsn)
        expected = (
            "Could not resolve IP address of hostname "
            "[test01.zlan] derived from connection string"
        )
        assert excinfo.value.message == expected
