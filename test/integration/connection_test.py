import pytest

from unittest import mock

from pyexasol.exceptions import ExaConnectionDsnError
from pyexasol.connection import ResolvedHost

# pylint: disable=protected-access/W0212

def test_resolve_hostname(connection):
    with mock.patch("socket.gethostbyname_ex") as get_hostname:
        get_hostname.return_value = ("host", [], ["ip1", "ip2"])
        actual = connection._resolve_hostname("host", 1234, "fingerprint")
        expected = [("host","ip1", 1234, "fingerprint"),("host","ip2", 1234, "fingerprint")]
        assert actual == expected


@pytest.mark.parametrize("empty_dsn", [None, "", " ", "\t"])
def test_process_empty_dsn_fails(connection, empty_dsn):
    with pytest.raises(ExaConnectionDsnError, match="Connection string is empty"):
        connection._process_dsn(empty_dsn)

def test_process_dsn_shuffles_hosts(connection):
    dsn = "host1:1234,host2:4321"
    def resolve_hostname(con):
        with mock.patch("socket.gethostbyname_ex") as get_hostname:
            get_hostname.side_effect = [("host1", [], ["ip11", "ip12"]), ("host2", [], ["ip21", "ip22"]),
                                        ("host1", [], ["ip11", "ip12"]), ("host2", [], ["ip21", "ip22"])]
            return tuple(con._process_dsn(dsn))
    count = 100
    results = {resolve_hostname(connection) for _ in range(0, count)}
    assert len(results) > 1

def test_process_dsn_without_shuffling(connection):
    with mock.patch("socket.gethostbyname_ex") as get_hostname:
        get_hostname.side_effect = [("host1", [], ["ip11", "ip12"]), ("host2", [], ["ip21", "ip22"])]
        actual = connection._process_dsn("host1,host2:1234", shuffle_host_names=False)
    expected = [
                ResolvedHost("host1","ip11", 1234, None),
                ResolvedHost("host1","ip12", 1234, None),
                ResolvedHost("host2","ip21", 1234, None),
                ResolvedHost("host2","ip22", 1234, None)]
    assert actual == expected

def test_process_dsn_without_port(connection):
    with mock.patch("socket.gethostbyname_ex") as get_hostname:
        get_hostname.side_effect = [("host1", [], ["ip1"])]
        actual = connection._process_dsn("host1")
    expected = [ResolvedHost("host1", "ip1", 8563, None)]
    assert actual == expected

def test_process_dsn_with_fingerprint(connection):
    with mock.patch("socket.gethostbyname_ex") as get_hostname:
        get_hostname.side_effect = [("host1", [], ["ip1"])]
        actual = connection._process_dsn("host1/135a1d2dce102de866f58267521f4232153545a075dc85f8f7596f57e588a181:1234")
    expected = [ResolvedHost("host1", "ip1", 1234, "135A1D2DCE102DE866F58267521F4232153545A075DC85F8F7596F57E588A181")]
    assert actual == expected
