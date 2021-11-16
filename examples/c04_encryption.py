"""
Connection with SSL encryption enabled
It works both for WebSocket communication (wss://) and HTTP(S) Transport
"""

import pyexasol
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

# Connect with encryption
C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema, encryption=True)

# Basic query
stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(stmt.fetchall())

# Export to list
users = C.export_to_list("SELECT * FROM users ORDER BY user_id LIMIT 5")
stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

print(users[0])
print(users[1])

# Import from list
C.import_from_iterable(users, 'users_copy')
stmt = C.last_statement()
print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')
