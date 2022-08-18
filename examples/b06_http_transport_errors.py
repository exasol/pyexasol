"""
Parallel HTTP transport

Edge cases, killing & failing various components at different times
"""

import os
import shutil
import time
import traceback
import threading

import pyexasol
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

dev_null = open(os.devnull, 'wb')


C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema, debug=False)


###
# Normal execution
###


def observer_callback(pipe, dst, **kwargs):
    print(f'Threads running: {threading.active_count()}')
    shutil.copyfileobj(pipe, dev_null)

    return


C.export_to_callback(observer_callback, None, 'SELECT * FROM users LIMIT 1000')
print('--- Finished Observer callback (normal execution) ---\n')


###
# SQL error
###


try:
    C.export_to_callback(observer_callback, None, 'SELECT * FROM usersaaa LIMIT 1000')
except Exception as e:
    traceback.print_exc()

print('--- Finished Observer callback (SQL error) ---\n')


###
# Abort SQL query
###


def abort_query_callback(pipe, dst, **kwargs):
    C.abort_query()
    time.sleep(2)

    shutil.copyfileobj(pipe, dev_null)

    return


try:
    C.export_to_callback(abort_query_callback, None, 'SELECT * FROM users LIMIT 1000')
except pyexasol.ExaError as e:
    traceback.print_exc()

print('--- Finished Abort Query ---\n')


###
# Error from callback EXPORT
###


def runtime_error_callback(pipe, dst, **kwargs):
    pipe.read(10)
    time.sleep(1)

    raise RuntimeError('Test error!')


try:
    C.export_to_callback(runtime_error_callback, None, 'SELECT * FROM users LIMIT 1000')
except Exception as e:
    traceback.print_exc()

print('--- Finished Runtime Error EXPORT Callback ---\n')


###
# Error from callback IMPORT
###


def runtime_error_callback(pipe, src, **kwargs):
    pipe.write(b"a,b,c,d")
    time.sleep(1)

    raise RuntimeError('Test error!')


try:
    C.import_from_callback(runtime_error_callback, None, 'users_copy')
except Exception as e:
    traceback.print_exc()

print('--- Finished Runtime Error IMPORT Callback ---\n')


###
# Close WS connection
###


def close_connection_callback(pipe, dst, **kwargs):
    C.close(disconnect=False)
    time.sleep(1)

    shutil.copyfileobj(pipe, dev_null)

    return


try:
    C.export_to_callback(close_connection_callback, None, 'SELECT * FROM users LIMIT 1000')
except Exception as e:
    traceback.print_exc()

print('--- Finished Close Connection ---\n')
