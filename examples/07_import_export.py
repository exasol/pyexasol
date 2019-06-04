"""
Example 7
Export and import from Exasol to objects
"""

import pyexasol
import _config as config

import tempfile
import shutil
import os

# Connect with compression enabled
C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema,
                     compression=True)

# Prepare empty tables
C.execute("TRUNCATE TABLE users_copy")
C.execute("TRUNCATE TABLE payments_copy")

# Create temporary file
file = tempfile.TemporaryFile()

# Export to temporary file
C.export_to_file(file, 'users', export_params={'with_column_names': True})

file.seek(0)
print(file.readline())
print(file.readline())
print(file.readline())
file.seek(0)

stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Import from temporary file
C.import_from_file(file, 'users_copy', import_params={'skip': 1})

stmt = C.last_statement()
print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

file.close()

# Export to list
users = C.export_to_list('users')
print(users[0])
print(users[1])

stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Import from list (or any other iterable)
C.import_from_iterable(users, 'users_copy')

stmt = C.last_statement()
print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')


# Export to custom callback
def my_export_callback(pipe, dst):
    lines = list()

    # Save 2 first lines
    lines.append(pipe.readline())
    lines.append(pipe.readline())

    # Dump everything else to /dev/null
    dev_null = open(os.devnull, 'wb')
    shutil.copyfileobj(pipe, dev_null)
    dev_null.close()

    return lines


res = C.export_to_callback(my_export_callback, None, 'users')
print(res)

stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')


# Import from custom callback
def my_import_callback(pipe, src):
    for line in src:
        pipe.write(line)


C.import_from_callback(my_import_callback, res, 'users_copy')
stmt = C.last_statement()
print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Export as gzipped file
file = tempfile.TemporaryFile()

C.export_to_file(file, 'users', export_params={'with_column_names': True, 'format': 'gz'})

file.seek(0)
print(file.read(30))
file.seek(0)

stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Import gzipped file
C.import_from_file(file, 'users_copy', import_params={'skip': 1, 'format': 'gz'})

stmt = C.last_statement()
print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

file.close()


# Custom encoding for IMPORT and EXPORT
file = tempfile.TemporaryFile()

C.export_to_file(file, 'users', export_params={'encoding': 'WINDOWS-1251'})

file.seek(0)
print(file.read(30))
file.seek(0)

# Import file with custom encoding
C.import_from_file(file, 'users_copy', import_params={'encoding': 'WINDOWS-1251'})

stmt = C.last_statement()
print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

file.close()
