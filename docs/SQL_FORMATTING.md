## SQL Formatting

PyEXASOL provides custom Exasol-specific formatter based on standard [Python 3 Formatter](https://docs.python.org/3/library/string.html#string.Formatter).

You're not forced to use this formatter. You can always overload it using `cls_formatter` connection option or format raw SQL yourself.

## Types of placeholders

Formatter supports only [new-style named placeholders](https://www.python.org/dev/peps/pep-3101/) and optional "conversion" to specify placeholder type.
```
foo {a}, {b}, {c!s}
```

If type was not defined, formatter assumes it's a string value by default (`!s`).

| Conversion | Function | Description |
| --- | --- | --- |
| `!s` | [`.quote()`](/docs/REFERENCE.md#quote) | Escapes string value and wraps it with single quotes. It can also be used for dates, timestamps, numbers, etc. |
| `!d` | [`.safe_decimal()`](/docs/REFERENCE.md#safe_decimal) | Validates decimal value and puts it without quotes. Could be useful for `LIMIT`, `OFFSET`, math expressions. |
| `!f` | [`.safe_float()`](/docs/REFERENCE.md#safe_float) | Similar to `!d`, but allows expressions with exponent part commonly used in float values. |
| `!i` | [`.safe_ident()`](/docs/REFERENCE.md#safe_ident) | Validates identifer and puts it without quotes. It allows you to pass lower-cased identifiers to query upper-cased named tables while keeping it "safe". It is the "convenient" version of identifier placeholder. |
| `!q` | [`.quote_ident()`](/docs/REFERENCE.md#quote_ident) | Escapes string identifier and wraps it with double quotes. It allows you to pass lower-cased identifiers to query lower-cased table names. It is the "proper" version of identifier placeholder. |
| `!r` | `str()` | Converts value to string and puts it "as is" without any escaping or checks. Useful as "raw sql" placeholder to build complex queries and to pass query parts like `ASC`, `DESC`, `TRUE`, `FALSE` etc. |

All value-oriented placeholders convert `None` values into `NULL` without quotes. Please remember that Exasol empty string is converted to `NULL` by database itself.

All identifier-oriented placeholder also accept `tuples` for multi-level identifier scenarios. For example:
```python
safe_ident('my_schema', 'my_table')
>>> my_schema.my_table

quote_ident('my_schema', 'my_table', 'my_column')
>>> "my_schema"."my_table"."my_column"
```

For all `list`-typed values each element will be converted independently and joined into final string using `, ` (comma and space). You may use it to pass multiple values to `IN ()` and `NOT IN ()` expressions.

## Complete example

```python
# SQL with formatting
params = {
    'random_value': 'abc',
    'null_value': None,
    'table_name_1': 'users',
    'table_name_2': (config.schema, 'PAYMENTS'),
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

stmt = C.execute(query, params)
print(stmt.query)
```

Result:
```sql
SELECT 'abc' AS random_value, NULL AS null_value, u.user_id, sum(gross_amt) AS gross_amt
FROM users u
    JOIN "PYEXASOL_TEST"."PAYMENTS" p ON (u.user_id=p.user_id)
WHERE u.user_rating >= 0.5
    AND u.user_score > 10.0
    AND u.is_female IS TRUE
    AND u.status IN ('ACTIVE', 'PASSIVE', 'SUSPENDED')
    AND u.user_rating NOT IN (10, 20)
GROUP BY 1,2,3
ORDER BY 4 DESC
LIMIT 10
```
