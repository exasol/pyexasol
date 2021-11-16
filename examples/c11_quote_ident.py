"""
Major cases when 'quote_ident' connection option takes effect
"""

import pyexasol
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

# Basic connect
C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, quote_ident=True)

# Open schema
C.open_schema(config.schema)

# Export from table name with lower case characters
pd = C.export_to_pandas('camelCaseTable')
pd.info()

stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Export into table name with lower case characters
C.import_from_pandas(pd, 'camelCaseTable')

stmt = C.last_statement()
print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Ext
cols = C.ext.get_columns('camelCaseTable')
printer.pprint(cols)

cols = C.ext.get_columns_sql('SELECT * FROM {table_name!q}', {'table_name': 'camelCaseTable'})
printer.pprint(cols)

cols = C.ext.get_columns((config.schema, 'camelCaseTable'))
printer.pprint(cols)

cols = C.ext.get_sys_columns('camelCaseTable')
printer.pprint(cols)
