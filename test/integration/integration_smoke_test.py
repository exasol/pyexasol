def test_smoke(connection):
    result = connection.execute("SELECT 1;")
    expected = (1,)
    actual = result.fetchall()[0]
    assert expected == actual
