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
        ],
    )
    def test_valid_dsn_is_still_parsed(mock_exaconnection_factory, dsn, expected):
        connection = mock_exaconnection_factory()

        actual = {
            (host.hostname, host.port, host.fingerprint)
            for host in connection._process_dsn(dsn)
        }
        assert actual == set(expected)
