import os

# Set Exasol cluster credentials here
dsn = os.environ.get('EXAHOST', 'exasol-dev.mlan:8563')
user = os.environ.get('EXAUID', 'SYS')
password = os.environ.get('EXAPWD', 'exasol')
schema = os.environ.get('EXASCHEMA', 'PYEXASOL_TEST')

# change this to 'p_high_random' or another table name
table_name = os.environ.get('EXATABLE', 'p_low_random')
number_of_rows = 10_000_000

odbc_connection_options = {
    # Set ODBC driver path here
    'DRIVER': '/local/bi/badoo_git/etl/exasol/libexaodbc-uo2214lv2.so',
    'EXAHOST': dsn,
    'EXAUID': user,
    'EXAPWD': password,
    'EXASCHEMA': schema,
    'AUTOCOMMIT': 'Y',
    'ENCRYPTION': 'N'
}

turbodbc_connection_options = {**odbc_connection_options, **{
    'parameter_sets_to_buffer': 100000,
    'rows_to_buffer': 100000,
    'use_async_io': True,
}}
