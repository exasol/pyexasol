"""
Example 24
Script output server

Exasol should be able to open connection to the host where current script is running
"""

import pyexasol as E
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

C = E.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
              , query_timeout=10)

stmt, log_files = C.execute_udf_output("""
    SELECT echo_java(user_id)
    FROM users
    GROUP BY CEIL(RANDOM() * 4)
""")

printer.pprint(stmt.fetchall())
printer.pprint(log_files)

print(log_files[0].read_text())
