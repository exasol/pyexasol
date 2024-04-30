def test_static_select(connection):
    result = connection.execute("SELECT 1;")
    expected = [(1,)]
    actual = result.fetchall()
    assert expected == actual


def test_sorted_select_and_limited_select(connection):
    statement = f"SELECT * FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = [
        (0, 'Amy Marquez', '2018-10-04', '2018-03-06 21:44:36.142000', True, '0.76', 30.11, 'PENDING\r'),
        (1, 'John Lawson', '2018-05-17', '2018-05-28 02:58:29.079000', True, '0.04', 71.72, 'DISABLED\r'),
        (2, 'Jessica Clark', '2018-05-23', '2018-05-22 04:19:51.098000', False, '0.72', 29.13, 'PENDING\r'),
        (3, 'Jennifer Taylor', '2018-05-01', '2018-03-03 08:12:52.685000', True, '0.43', 8.46, 'SUSPENDED\r'),
        (4, 'Tristan Romero', '2018-10-04', '2018-03-31 20:21:50.199000', True, '0.23', 62.980000000000004, 'PENDING\r')
    ]
    actual = result.fetchall()
    assert expected == actual
