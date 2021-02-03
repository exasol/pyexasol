"""
Example 4
Using custom mapper to get python objects out of Exasol fetch methods

DECIMAL(p,0)           -> int
DECIMAL(p,s)           -> decimal.Decimal
DOUBLE                 -> float
DATE                   -> datetime.date
TIMESTAMP              -> datetime.datetime
BOOLEAN                -> bool
VARCHAR                -> str
CHAR                   -> str
INTERVAL DAY TO SECOND -> datetime.timedelta
<others>               -> str
"""

import pyexasol
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=180)

# Basic connect (custom mapper
C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema,
                     fetch_mapper=pyexasol.exasol_mapper)

# Fetch objects
stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(stmt.fetchall())

# Test timestamp formats with different amount of decimal places
# Please note: Exasol stores timestamps with millisecond precision (3 decimal places)
# Lack of precision is not a bug, it's the documented feature

for i in range(0, 9):
    C.execute(f"ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF{i}'")
    printer.pprint(C.execute("SELECT TIMESTAMP'2018-01-01 03:04:05.123456'").fetchval())

# Test interval mapping
stmt = C.execute("SELECT id, from_ts, to_ts, expected_timedelta, to_ts - from_ts AS actual_timedelta FROM interval_test")
for row in stmt.fetchall():
    actual = printer.pformat(row[4])
    print(f"---- Interval Test #{row[0]} ----")
    print(f"    FROM: {row[1]}")
    print(f"      TO: {row[2]}")
    print(f"EXPECTED: {row[3]}")
    print(f"  ACTUAL: {actual} " + ("✓" if actual == row[3] else "✗"))
    assert actual == row[3], f"actual timedelta '{actual}' doesn't match expected timedelta '{row[3]}'"