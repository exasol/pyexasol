"""
Example 10
Connection redundancy, attempts to connect to all hosts from DSN in random order
"""

import pyexasol
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

# Basic connect
C = pyexasol.connect(dsn='192.0.2.100..110,' + config.dsn, user=config.user, password=config.password, schema=config.schema,
                     socket_timeout=0.5, debug=True)
