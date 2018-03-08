"""
Example 6
Export and import from Exasol to Pandas DataFrames

Please make sure you enable compression for office wifi!
"""

import pyexasol as E
import _config as config

# Connect with compression enabled
C = E.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema,
              compression=True)

C.execute("TRUNCATE TABLE users_copy")

# Export from query
pd = C.export_to_pandas("SELECT * FROM users")
stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Import back to another table
C.import_from_pandas(pd, 'users_copy')
stmt = C.last_statement()
print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Export from table name
pd = C.export_to_pandas('users')
stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')
