"""
Example 4
Using custom mapper to get python objects out of Exasol fetch methods

DECIMAL(p,0) -> int
DECIMAL(p,s) -> decimal.Decimal
DOUBLE       -> float
DATE         -> datetime.date
TIMESTAMP    -> datetime.datetime
BOOLEAN      -> bool
VARCHAR      -> str
CHAR         -> str
<others>     -> str
"""

import pyexasol as E
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=180)

# Basic connect (custom mapper
C = E.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema,
              fetch_mapper=E.exasol_mapper)

# Fetch objects
stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(stmt.fetchall())

# Test timestamp formats with different amount of decimal places
# Please note: Exasol stores timestamps with millisecond precision (3 decimal places)
# Lack of precision is not a bug, it's the documented feature

for i in range(0, 9):
    C.execute(f"ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF{i}'")
    printer.pprint(C.execute("SELECT TIMESTAMP'2018-01-01 03:04:05.123456'").fetchval())
