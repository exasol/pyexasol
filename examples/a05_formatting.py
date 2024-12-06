"""
Format values and identifiers using query_params and pyexasol formatter
"""

import pprint

import _config as config

import pyexasol

printer = pprint.PrettyPrinter(indent=4, width=40)

# Basic connect
C = pyexasol.connect(
    dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
)

# SQL with formatting
params = {
    "random_value": "abc",
    "null_value": None,
    "table_name_1": "users",
    "table_name_2": (config.schema, "PAYMENTS"),
    "user_rating": "0.5",
    "user_score": 1e1,
    "is_female": "TRUE",
    "user_statuses": ["ACTIVE", "PASSIVE", "SUSPENDED"],
    "exclude_user_score": [10, 20],
    "limit": 10,
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

stmt = C.execute(query, params)
print(stmt.query)
printer.pprint(stmt.fetchall())
