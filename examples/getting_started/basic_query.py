"""
Getting Started - Run basic queries
"""

import examples._config as config
import pyexasol

# Using a context manager ensures the connection is properly closed
# even if an exception occurs
with pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    websocket_sslopt=config.websocket_sslopt,
) as C:
    with C.execute("SELECT * FROM EXA_ALL_USERS") as stmt:
        print(stmt.fetchone())  # fetch 1 row
        print(stmt.fetchmany(3))  # fetch 3 rows
        print(stmt.fetchall())  # fetch all remaining rows

    print(stmt.is_closed)
print(C.is_closed)

# You can also iterate through rows directly
with pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    websocket_sslopt=config.websocket_sslopt,
) as C:
    with C.execute("SELECT * FROM EXA_ALL_USERS") as stmt:
        for row in stmt:
            print(row)
