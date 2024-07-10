"""
HTTP Transport

EXPORT and IMPORT from Exasol to Polars DataFrames
Make sure to enable compression for Wifi connections to improve performance
"""

import pyexasol
import _config as config

# Connect with compression enabled
C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema,
                     compression=True)

C.execute('TRUNCATE TABLE users_copy')

# Export from Exasol table into polars.DataFrame
df = C.export_to_polars('users')
df.info()

stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Import from polars DataFrame into Exasol table
C.import_from_polars(df, 'users_copy')

# Alternatively use polars LazyFrame the same way
# lf = df.lazy()
# C.import_from_polars(lf, 'users_copy')

stmt = C.last_statement()
print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Export from SQL query
df = C.export_to_polars('SELECT user_id, user_name FROM users WHERE user_id >= 5000')
df.info()

stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')
