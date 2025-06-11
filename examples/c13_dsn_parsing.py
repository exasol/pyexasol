"""
Demonstration of DSN (Connection string) expansion and related exceptions
"""

import pprint

import examples._config as config
import pyexasol

printer = pprint.PrettyPrinter(indent=4, width=140)


C = pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    schema=config.schema,
    verbose_error=False,
    encryption=True,
    websocket_sslopt=config.websocket_sslopt,
)

print("IP range with custom port: ")
result = C._process_dsn("127.0.0.10..19:8564")
printer.pprint(sorted(result))

print("Multiple ranges with multiple ports and with default port at the end: ")
result = C._process_dsn("127.0.0.10..19:8564,127.0.0.20,localhost:8565,127.0.0.21..23")
printer.pprint(sorted(result))

print("Multiple ranges with fingerprint and port: ")
result = C._process_dsn("127.0.0.10..19/ABC,127.0.0.20,localhost/CDE:8564")
printer.pprint(sorted(result))

# Empty DSN
try:
    result = C._process_dsn(" ")
    printer.pprint(result)
except pyexasol.ExaConnectionDsnError as e:
    print(e)


# Invalid range
try:
    result = C._process_dsn("127.0.0.15..10")
    printer.pprint(result)
except pyexasol.ExaConnectionDsnError as e:
    print(e)

# Cannot resolve hostname
try:
    result = C._process_dsn("test1..5.zlan")
    printer.pprint(result)
except pyexasol.ExaConnectionDsnError as e:
    print(e)

# Hostname range with zero-padding
try:
    result = C._process_dsn("test01..20.zlan")
    printer.pprint(result)
except pyexasol.ExaConnectionDsnError as e:
    print(e)
