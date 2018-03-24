"""
Example 5
How to format values and identifiers
"""

import pyexasol as E
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=40)

# Basic connect
C = E.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)

# SQL with formatting
params = {
    'random_value': 'abc',
    'null_value': None,
    'table_name_1': 'users',
    'table_name_2': (config.schema, 'PAYMENTS'),
    'user_rating': '0.5',
    'user_score': 1e1,
    'is_female': 'TRUE',
    'limit': 10
}

query = """
    SELECT {random_value} AS random_value, {null_value} AS null_value, u.user_id, sum(gross_amt) AS gross_amt
    FROM {table_name_1!i} u
        JOIN {table_name_2!q} p ON (u.user_id=p.user_id)
    WHERE u.user_rating >= {user_rating!d}
        AND u.user_score > {user_score!f}
        AND u.is_female IS {is_female!r}
    GROUP BY 1,2,3
    ORDER BY 4 DESC
    LIMIT {limit!d}
"""

stmt = C.execute(query, params)
print(stmt.query)
printer.pprint(stmt.fetchall())
