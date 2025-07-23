"""
Connection redundancy, attempts to connect to all hosts from DSN in random order
"""

import pprint

import examples._config as config
import pyexasol

printer = pprint.PrettyPrinter(indent=4, width=140)

# Basic connect
C = pyexasol.connect(
    dsn="0.42.42.40..49," + config.dsn,
    user=config.user,
    password=config.password,
    schema=config.schema,
    connection_timeout=2,
    debug=True,
    websocket_sslopt=config.websocket_sslopt,
)
