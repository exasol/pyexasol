"""
Open connection, run simple query, close connection
"""

import pprint

import _config as config

import pyexasol

printer = pprint.PrettyPrinter(indent=4, width=140)

# Basic connect
C = pyexasol.connect(
    dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
)

# Basic query
stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(stmt.fetchall())

# Disconnect
C.close()
