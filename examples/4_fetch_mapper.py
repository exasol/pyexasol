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
