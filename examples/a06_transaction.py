"""
Transactions
"""

import examples._config as config
import pyexasol

# Connect with autocommit OFF
C = pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    schema=config.schema,
    autocommit=False,
    websocket_sslopt=config.websocket_sslopt,
)

# Another way to change autocommit after connection
C.set_autocommit(False)

stmt = C.execute("TRUNCATE TABLE users")
print(f"Truncate affected {stmt.rowcount()} rows")

C.rollback()
print("Truncate was rolled back")

stmt = C.execute("SELECT count(*) FROM users")
print(f"Select affected {stmt.fetchval()} rows")

C.commit()
print("Select was committed")
