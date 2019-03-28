""" Test utils methods."""
import pytest

from . import utils


@pytest.mark.parametrize(
    "case, expected",
    [
        ("127.0.0.1:8888", [("127.0.0.1", 8888)]),
        (
            "127.0.0.1..3:8888",
            [("127.0.0.1", 8888), ("127.0.0.2", 8888), ("127.0.0.3", 8888)],
        ),
        (
            "127.0.0.1..3,127.0.0.5..6:8563",
            [
                ("127.0.0.1", 8563),
                ("127.0.0.2", 8563),
                ("127.0.0.3", 8563),
                ("127.0.0.5", 8563),
                ("127.0.0.6", 8563),
            ],
        ),
    ],
)
def test_get_host_port_list_from_dsn(case, expected):
    """ Expect list of HOST, PORT."""
    result = utils.get_host_port_list_from_dsn(case)
    assert result == expected
