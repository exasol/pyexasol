import os

dsn = os.environ.get('EXA_HOST') or 'localhost:8563'
user = os.environ.get('EXA_USER') or 'SYS'
password = os.environ.get('EXA_PWD') or 'exasol'
schema = os.environ.get('EXA_SCHEMA') or 'PYEXASOL_TEST'
