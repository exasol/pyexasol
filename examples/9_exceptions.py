"""
Example 9
Catching exceptions
"""

import pyexasol as E
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

# Bad dsn
try:
    C = E.connect(dsn='123' + config.dsn, user=config.user, password=config.password, schema=config.schema)
except E.ExaCommunicationError as e:
    print(e)

# Bad user \ password
try:
    C = E.connect(dsn=config.dsn, user=config.user, password='123' + config.password, schema=config.schema)
except E.ExaRequestError as e:
    print(e)

C = E.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema,
              fetch_size_bytes=1024 * 10)

# Invalid SQL
try:
    stmt = C.execute("""
        SELECT1 *
        FROM users 
        ORDER BY user_id 
        LIMIT 5
    """)
except E.ExaQueryError as e:
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
except E.ExaQueryError as e:
    print(e)

# Attempt to read from closed cursor
stmt = C.execute("SELECT * FROM payments")
stmt.fetchone()
stmt.close()

try:
    stmt.fetchall()
except E.ExaRequestError as e:
    print(e)

# Attempt to fetch query without result set
stmt = C.execute("COMMIT")

try:
    stmt.fetchone()
except E.ExaRuntimeError as e:
    print(e)

# Attempt to run query on closed connection
C.close()

try:
    C.execute("SELECT 1")
except E.ExaRuntimeError as e:
    print(e)
