import pytest


@pytest.mark.smoke
def test_static_select(connection):
    result = connection.execute("SELECT 1;")
    expected = [(1,)]
    actual = result.fetchall()
    assert expected == actual
