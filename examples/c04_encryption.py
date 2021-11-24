"""
Connection with SSL encryption enabled
It works both for WebSocket communication (wss://) and HTTP(S) Transport
"""

import hashlib
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


# Connect with encryption AND certificate fingerprint check
server_fingerprint = hashlib.sha256(C._ws.sock.getpeercert(True)).hexdigest().upper()
print(f"Server certificate fingerprint: {server_fingerprint}")

if ':' in config.dsn:
    dsn_with_valid_fingerprint = config.dsn.replace(':', f'/{server_fingerprint}:')
    dsn_with_invalid_fingerprint = config.dsn.replace(':', f'/123abc:')
else:
    dsn_with_valid_fingerprint = f'{config.dsn}/{server_fingerprint}'
    dsn_with_invalid_fingerprint = f'{config.dsn}/123abc'

C = pyexasol.connect(dsn=dsn_with_valid_fingerprint, user=config.user, password=config.password, schema=config.schema, encryption=True)
print(f"Encrypted connection with fingerprint validation was established")


# Invalid fingerprint causes exception
try:
    pyexasol.connect(dsn=dsn_with_invalid_fingerprint, user=config.user, password=config.password, schema=config.schema, encryption=True)
except pyexasol.ExaConnectionFailedError as e:
    print(e)

# Valid fingerprint without encryption causes exception
try:
    pyexasol.connect(dsn=dsn_with_valid_fingerprint, user=config.user, password=config.password, schema=config.schema)
except pyexasol.ExaConnectionDsnError as e:
    print(e)
