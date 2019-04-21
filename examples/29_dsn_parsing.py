"""
Example 29
Demonstration of DSN (Connection string) expansion and related exceptions
"""

import pyexasol
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)


C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
                     , verbose_error=False)

print('IP range with custom port: ')
result = C._process_dsn('127.0.0.10..19:8564')
printer.pprint(sorted(result))

print('Multiple ranges with multiple ports and with default port at the end: ')
result = C._process_dsn('127.0.0.10..19:8564,127.0.0.20,localhost:8565,127.0.0.21..23')
printer.pprint(sorted(result))


# Empty DSN
try:
    result = C._process_dsn(' ')
    printer.pprint(result)
except pyexasol.ExaConnectionDsnError as e:
    print(e)


# Invalid range
try:
    result = C._process_dsn('127.0.0.15..10')
    printer.pprint(result)
except pyexasol.ExaConnectionDsnError as e:
    print(e)

# Cannot resolve hostname
try:
    result = C._process_dsn('testl1..5.zlan')
    printer.pprint(result)
except pyexasol.ExaConnectionDsnError as e:
    print(e)
