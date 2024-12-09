"""
Basic compatibility with DB-API 2.0
Suitable for temporary testing only, should not be used in production
"""

import pprint

import _config as config

import pyexasol.db2

printer = pprint.PrettyPrinter(indent=4, width=140)

C = pyexasol.db2.connect(
    dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
)
cur = C.cursor()

# Fetch tuples row-by-row as iterator
cur.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")

while True:
    row = cur.fetchone()

    if row is None:
        break

    printer.pprint(row)

# Fetch many
cur.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(cur.fetchmany(3))
printer.pprint(cur.fetchmany(3))

# Fetch everything in one go
cur.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(cur.fetchall())

printer.pprint(cur.description)
printer.pprint(cur.rowcount)

# Autocommit is False by default
print(C.attr["autocommit"])
C.commit()
