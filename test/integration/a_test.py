import pytest


@pytest.mark.a01_examples
def test_sorted_select_and_limited_select(connection):
    statement = "SELECT * FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = [
        (0, 'Jessica Mccoy', '2018-07-12', '2018-04-03 18:36:40.553000', True, '0.7', None, 'ACTIVE'),
        (1, 'Beth James', '2018-05-24', '2018-03-24 08:08:46.251000', False, '0.53', 22.07, 'ACTIVE'),
        (2, 'Mrs. Teresa Ryan', '2018-08-21', '2018-11-07 01:53:08.727000', False, '0.03', 24.88, 'PENDING'),
        (3, 'Tommy Henderson', '2018-04-18', '2018-04-28 21:39:59.300000', True, '0.5', 27.43, 'DISABLED'),
        (4, 'Jessica Christian', '2018-12-18', '2018-11-29 14:11:55.450000', True, '0.1', 62.59, 'SUSPENDED')
    ]
    actual = result.fetchall()
    assert expected == actual


@pytest.mark.a02_examples
def test_fetch_tuples_using_fetchone(connection):
    statement = "SELECT * FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = [
        (0, 'Jessica Mccoy', '2018-07-12', '2018-04-03 18:36:40.553000', True, '0.7', None, 'ACTIVE'),
        (1, 'Beth James', '2018-05-24', '2018-03-24 08:08:46.251000', False, '0.53', 22.07, 'ACTIVE'),
        (2, 'Mrs. Teresa Ryan', '2018-08-21', '2018-11-07 01:53:08.727000', False, '0.03', 24.88, 'PENDING'),
        (3, 'Tommy Henderson', '2018-04-18', '2018-04-28 21:39:59.300000', True, '0.5', 27.43, 'DISABLED'),
        (4, 'Jessica Christian', '2018-12-18', '2018-11-29 14:11:55.450000', True, '0.1', 62.59, 'SUSPENDED')
    ]
    actual = [row for row in iter(result.fetchone, None)]
    assert expected == actual


@pytest.mark.a02_examples
def test_fetch_tuples_using_fetchmany(connection):
    count = 3
    statement = "SELECT * FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)

    expected = [
        (0, 'Jessica Mccoy', '2018-07-12', '2018-04-03 18:36:40.553000', True, '0.7', None, 'ACTIVE'),
        (1, 'Beth James', '2018-05-24', '2018-03-24 08:08:46.251000', False, '0.53', 22.07, 'ACTIVE'),
        (2, 'Mrs. Teresa Ryan', '2018-08-21', '2018-11-07 01:53:08.727000', False, '0.03', 24.88, 'PENDING'),
    ]
    actual = result.fetchmany(count)
    assert expected == actual

    expected = [
        (3, 'Tommy Henderson', '2018-04-18', '2018-04-28 21:39:59.300000', True, '0.5', 27.43, 'DISABLED'),
        (4, 'Jessica Christian', '2018-12-18', '2018-11-29 14:11:55.450000', True, '0.1', 62.59, 'SUSPENDED')
    ]
    actual = result.fetchmany(count)
    assert expected == actual

    
@pytest.mark.a02_examples
def test_fetch_tuples_using_fetchall(connection):
    count = 3
    statement = "SELECT * FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = [
        (0, 'Jessica Mccoy', '2018-07-12', '2018-04-03 18:36:40.553000', True, '0.7', None, 'ACTIVE'),
        (1, 'Beth James', '2018-05-24', '2018-03-24 08:08:46.251000', False, '0.53', 22.07, 'ACTIVE'),
        (2, 'Mrs. Teresa Ryan', '2018-08-21', '2018-11-07 01:53:08.727000', False, '0.03', 24.88, 'PENDING'),
        (3, 'Tommy Henderson', '2018-04-18', '2018-04-28 21:39:59.300000', True, '0.5', 27.43, 'DISABLED'),
        (4, 'Jessica Christian', '2018-12-18', '2018-11-29 14:11:55.450000', True, '0.1', 62.59, 'SUSPENDED')
    ]
    actual = result.fetchall()
    assert expected == actual


@pytest.mark.a02_examples
def test_fetch_one_column_as_list_of_values(connection):
    statement = "SELECT user_name, user_id FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = [
        'Jessica Mccoy',
        'Beth James',
        'Mrs. Teresa Ryan',
        'Tommy Henderson',
        'Jessica Christian'
    ]
    actual = result.fetchcol()
    assert expected == actual


@pytest.mark.a02_examples
def test_fetch_a_single_value(connection):
    count = 3
    statement = "SELECT user_name, user_id FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = 'Jessica Mccoy'
    actual = result.fetchval()
    assert expected == actual

