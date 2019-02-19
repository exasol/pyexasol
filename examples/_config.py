import os

dsn = os.environ.get('EXAHOST', 'localhost:8563')
user = os.environ.get('EXAUID', 'SYS')
password = os.environ.get('EXAPWD', 'exasol')
schema = os.environ.get('EXASCHEMA', 'PYEXASOL_TEST')
