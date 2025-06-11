"""
HTTP Transport

EXPORT and IMPORT from Exasol to Pandas DataFrames
Make sure to enable compression for Wifi connections to improve performance
"""

import examples._config as config
import pyexasol

# Connect with compression enabled
C = pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    schema=config.schema,
    compression=True,
    websocket_sslopt=config.websocket_sslopt,
)

C.execute("TRUNCATE TABLE users_copy")

# Export from Exasol table into pandas.DataFrame
pd = C.export_to_pandas("users")
pd.info()

stmt = C.last_statement()
print(f"EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s")

# Import from pandas DataFrame into Exasol table
C.import_from_pandas(pd, "users_copy")

stmt = C.last_statement()
print(f"IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s")

# Export from SQL query
pd = C.export_to_pandas("SELECT user_id, user_name FROM users WHERE user_id >= 5000")
pd.info()

stmt = C.last_statement()
print(f"EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s")
