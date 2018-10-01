"""
Example 6
Export and import from Exasol to Pandas DataFrames

Please make sure you enable compression for office wifi!
"""

import pyexasol
import _config as config

# Connect with compression enabled
C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema,
                     compression=True)

C.execute('TRUNCATE TABLE users_copy')

# Export from Exasol table into pandas.DataFrame
pd = C.export_to_pandas('users')
pd.info()

stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Import from pandas DataFrame into Exasol table
C.import_from_pandas(pd, 'users_copy')

stmt = C.last_statement()
print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Export from SQL query
pd = C.export_to_pandas('SELECT user_id, user_name FROM users WHERE user_id >= 5000')
pd.info()

stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')
