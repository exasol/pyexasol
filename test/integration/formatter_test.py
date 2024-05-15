import pytest
from inspect import cleandoc


@pytest.mark.format
@pytest.mark.parametrize("value,expected", [
    (None, None),
    (1, "1"),
    (1.0, "1.0"),
])
def test_no_conversion_specified(connection, value, expected):
    params = {"value": value}
    query = "SELECT {value};"
    actual = connection.execute(query, params).fetchval()

    assert expected == actual


@pytest.mark.format
def test_safe_quote_conversion(connection, schema):
    params = {"table": (schema, "USERS")}
    query = cleandoc("""
        SELECT user_name FROM {table!q}
        ORDER BY user_name DESC
        LIMIT 5;
    """)
    expected = [
        'Zoe Odom',
        'Zoe Merritt',
        'Zoe Lawrence',
        'Zoe Fisher',
        'Zachary Wood'
    ]
    actual = connection.execute(query, params).fetchcol()

    assert expected == actual


@pytest.mark.format
@pytest.mark.parametrize("value", [
    True, 'True', "TRUE", 'true'
])
def test_string_conversion(connection, value):
    params = {"is_female": value}
    query = cleandoc("""
        SELECT user_name FROM USERS
        WHERE is_female IS {is_female!r}
        ORDER BY user_name DESC
        LIMIT 5;
    """)
    expected = [
        'Zoe Odom',
        'Zoe Merritt',
        'Zachary Williams',
        'Zachary Torres',
        'Zachary Thompson'
    ]
    actual = connection.execute(query, params).fetchcol()

    assert expected == actual


@pytest.mark.format
def test_safe_identifier_conversion(connection):
    params = {"table": "USERS"}
    query = cleandoc("""
        SELECT user_name FROM {table!i}
        ORDER BY user_name DESC
        LIMIT 5;
    """)
    expected = [
        'Zoe Odom',
        'Zoe Merritt',
        'Zoe Lawrence',
        'Zoe Fisher',
        'Zachary Wood'
    ]
    actual = connection.execute(query, params).fetchcol()

    assert expected == actual


@pytest.mark.format
@pytest.mark.parametrize("value,expected", [
    ("1.0", 1.0),
    (1.0, 1.0),
    (1e1, 10),
])
def test_safe_float_conversion(connection, value, expected):
    params = {"value": value}
    query = "SELECT {value!f};"
    result = connection.execute(query, params)
    actual = result.fetchval()

    assert expected == actual


@pytest.mark.format
@pytest.mark.parametrize("value,expected", [
    ("0.5", "0.5"),
    (10, 10),
])
def test_safe_decimal_conversion(connection, value, expected):
    params = {"value": value}
    query = "SELECT {value!d};"
    result = connection.execute(query, params)
    actual = result.fetchval()

    assert expected == actual


@pytest.mark.format
def test_complex_format_string(connection, schema):
    params = {
        'random_value': 'abc',
        'null_value': None,
        'table_name_1': 'users',
        'table_name_2': (schema, 'PAYMENTS'),
        'user_rating': '0.5',
        'user_score': 1e1,
        'is_female': 'TRUE',
        'user_statuses': ['ACTIVE', 'PASSIVE', 'SUSPENDED'],
        'exclude_user_score': [10, 20],
        'limit': 10
    }
    query = """
        SELECT {random_value} AS random_value, {null_value} AS null_value, u.user_id, sum(gross_amt) AS gross_amt
        FROM {table_name_1!i} u
            JOIN {table_name_2!q} p ON (u.user_id=p.user_id)
        WHERE u.user_rating >= {user_rating!d}
            AND u.user_score > {user_score!f}
            AND u.is_female IS {is_female!r}
            AND u.status IN ({user_statuses})
            AND u.user_rating NOT IN ({exclude_user_score!d})
        GROUP BY 1,2,3
        ORDER BY 4 DESC
        LIMIT {limit!d}
    """
    expected = [
        ('abc', None, 2740, '30'), ('abc', None, 5314, '27.82'),
        ('abc', None, 6012, '26.79'), ('abc', None, 6187, '24.95'),
        ('abc', None, 6525, '24.75'), ('abc', None, 8445, '24.16'),
        ('abc', None, 1632, '24.07'), ('abc', None, 8768, '23.82'),
        ('abc', None, 4830, '23.74'), ('abc', None, 2692, '23.2')
    ]
    actual = connection.execute(query, params).fetchall()

    assert expected == actual
