"""
Example 34
Edge cases for HTTP transport, killing & failing various components at different times
"""

import os
import psutil
import shutil
import threading

import pyexasol
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

main_process = psutil.Process()
dev_null = open(os.devnull, 'wb')


def print_stat():
    print(f'Child processes running: {len(main_process.children())}, threads running: {threading.active_count()}')


C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)


###
# Normal execution
###


def observer_callback(pipe, dst, **kwargs):
    print_stat()

    shutil.copyfileobj(pipe, dev_null)

    return


C.export_to_callback(observer_callback, None, 'SELECT * FROM users LIMIT 1000')

print_stat()
print('--- Finished Observer callback (normal execution) ---\n')

###
# Kill HTTP transport
###


def http_terminate_callback(pipe, dst, **kwargs):
    main_process.children()[0].terminate()
    shutil.copyfileobj(pipe, dev_null)

    return


try:
    C.export_to_callback(http_terminate_callback, None, 'SELECT * FROM users LIMIT 1000')
except pyexasol.ExaError as e:
    print(e)

print_stat()
print('--- Finished Kill HTTP transport ---\n')


###
# Abort SQL query
###


def abort_query_callback(pipe, dst, **kwargs):
    C.abort_query()
    shutil.copyfileobj(pipe, dev_null)

    return


try:
    C.export_to_callback(abort_query_callback, None, 'SELECT * FROM users LIMIT 1000')
except pyexasol.ExaError as e:
    print(e)

print_stat()
print('--- Finished Abort Query ---\n')


###
# Error from callback
###


def runtime_error_callback(pipe, dst, **kwargs):
    pipe.read(10)
    raise RuntimeError('Test error!')


try:
    C.export_to_callback(runtime_error_callback, None, 'SELECT * FROM users LIMIT 1000')
except Exception as e:
    print(e)

print_stat()
print('--- Finished Runtime Error Callback ---\n')


###
# Close WS connection
###


def close_connection_callback(pipe, dst, **kwargs):
    C.close()
    shutil.copyfileobj(pipe, dev_null)

    return


try:
    C.export_to_callback(close_connection_callback, None, 'SELECT * FROM users LIMIT 1000')
except Exception as e:
    print(e)

print_stat()
print('--- Finished Close Connection ---\n')
