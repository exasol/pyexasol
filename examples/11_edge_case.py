"""
Example 11
Try to read and write edge cases
"""

import pyexasol
import _config as config

import decimal

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

# Basic connect
C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)

edge_cases = [
    # Biggest values
    {
        'dec36_0': decimal.Decimal('+' + ('9' * 36)),
        'dec36_36': decimal.Decimal('+0.' + ('9' * 36)),
        'dbl': 1.7e308,
        'bl': True,
        'dt': '9999-12-31',
        'ts': '9999-12-31 23:59:59.999',
        'var100': 'ひ' * 100,
        'var2000000': 'ひ' * 2000000,
    },
    # Smallest values
    {
        'dec36_0': decimal.Decimal('-' + ('9' * 36)),
        'dec36_36': decimal.Decimal('-0.' + ('9' * 36)),
        'dbl': -1.7e308,
        'bl': False,
        'dt': '0001-01-01',
        'ts': '0001-01-01 00:00:00',
        'var100': '',
        'var2000000': 'ひ',
    },
    # All nulls
    {
        'dec36_0': None,
        'dec36_36': None,
        'dbl': None,
        'bl': None,
        'dt': None,
        'ts': None,
        'var100': None,
        'var2000000': None,
    }
]

insert_q = 'INSERT INTO edge_case VALUES ({dec36_0!d}, {dec36_36!d}, {dbl!f}, {bl}, {dt}, {ts}, {var100}, {var2000000})'
select_q = 'SELECT dec36_0, dec36_36, dbl, bl, dt, ts, var100, LENGTH(var2000000) AS len_var FROM edge_case'

C.execute('TRUNCATE TABLE edge_case')

# Insert (test formatting)
C.execute(insert_q, dict(edge_cases[0]))
C.execute(insert_q, dict(edge_cases[1]))
C.execute(insert_q, dict(edge_cases[2]))

# Select and fetch
stmt = C.execute(select_q)
printer.pprint(stmt.fetchall())


# Same actions with "exasol_mapper"
C.fetch_mapper = pyexasol.exasol_mapper
C.execute('TRUNCATE TABLE edge_case')

# Insert (test formatting)
C.execute(insert_q, dict(edge_cases[0]))
C.execute(insert_q, dict(edge_cases[1]))
C.execute(insert_q, dict(edge_cases[2]))

# Select and fetch
stmt = C.execute(select_q)
printer.pprint(stmt.fetchall())

# Import and export
edge_tuples = C.execute("SELECT * FROM edge_case").fetchall()

C.execute('TRUNCATE TABLE edge_case')

C.import_from_iterable(edge_tuples, 'edge_case')
stmt = C.last_statement()
print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

res = C.export_to_list(select_q)
stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')
printer.pprint(res)
