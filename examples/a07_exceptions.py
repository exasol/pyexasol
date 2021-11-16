"""
Exceptions for basic queries
"""

import pyexasol
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

# Bad dsn
try:
    C = pyexasol.connect(dsn='123' + config.dsn, user=config.user, password=config.password, schema=config.schema)
except pyexasol.ExaConnectionError as e:
    print(e)

# Bad user \ password
try:
    C = pyexasol.connect(dsn=config.dsn, user=config.user, password='123' + config.password, schema=config.schema)
except pyexasol.ExaAuthError as e:
    print(e)

C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema,
                     fetch_size_bytes=1024 * 10)

# Invalid SQL
try:
    stmt = C.execute("""
        SELECT1 *
        FROM users 
        ORDER BY user_id 
        LIMIT 5
    """)
except pyexasol.ExaQueryError as e:
    print(e)


# Valid SQL, but error during execution
try:
    stmt = C.execute("""
        SELECT *
        FROM users 
        WHERE user_name = 10
        ORDER BY user_id
        LIMIT 5
    """)
except pyexasol.ExaQueryError as e:
    print(e)

# Attempt to read from closed cursor
stmt = C.execute("SELECT * FROM payments")
stmt.fetchone()
stmt.close()

try:
    stmt.fetchall()
except pyexasol.ExaRequestError as e:
    print(e)

# Attempt to fetch query without result set
stmt = C.execute("COMMIT")

try:
    stmt.fetchone()
except pyexasol.ExaRuntimeError as e:
    print(e)

# Attempt to run SELECT with duplicate column names
try:
    stmt = C.execute("""
        SELECT 1, 1, 2 AS user_id, 3 AS user_id
        FROM dual
    """)
except pyexasol.ExaRuntimeError as e:
    print(e)

# Attempt to run query on closed connection
C.close()

try:
    C.execute("SELECT 1")
except pyexasol.ExaRuntimeError as e:
    print(e)

# Simulate websocket error during close
C1 = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)
C2 = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)

C2.execute(f'KILL SESSION {C1.session_id()}')

try:
    C1.close()
except pyexasol.ExaError as e:
    print(e)
