"""
Use context manager ("with" statement) for ExaConnection and ExaStatement objects
"""

import pprint

import _config as config

import pyexasol

printer = pprint.PrettyPrinter(indent=4, width=140)

# Basic usage
with pyexasol.connect(
    dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
) as C:
    with C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5") as stmt:
        printer.pprint(stmt.fetchall())

    printer.pprint(stmt.is_closed)

printer.pprint(C.is_closed)

# Exception causes connection and statement to be closed
try:
    with pyexasol.connect(
        dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
    ) as C:
        with C.execute("SELECT * FROM unknown_table LIMIT 5") as stmt:
            printer.pprint(stmt.fetchall())
except pyexasol.ExaQueryError as e:
    print(e)

printer.pprint(stmt.is_closed)
printer.pprint(C.is_closed)
