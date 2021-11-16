"""
Profiling with and without details
"""

import pyexasol
import _config as config

import json
import sys

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)

C.execute("ALTER SESSION SET QUERY_CACHE = 'OFF'")
C.execute("ALTER SESSION SET PROFILE = 'ON'")

# Normal profiling
stmt = C.execute("""
    SELECT u.user_id, sum(p.gross_amt) AS total_gross_amt
    FROM users u
        LEFT JOIN payments p ON (u.user_id=p.user_id)
    GROUP BY 1
    ORDER BY 2 DESC NULLS LAST
    LIMIT 10
""")

printer.pprint(stmt.fetchall())
json.dump(C.ext.explain_last(), sys.stdout, indent=4)

# Profiling with extra details per node (IPROC column)
C.execute("""
    SELECT u.user_id, sum(p.gross_amt) AS total_gross_amt
    FROM users u
        LEFT JOIN payments p ON (u.user_id=p.user_id)
    GROUP BY 1
    ORDER BY 2 DESC NULLS LAST
    LIMIT 10
""")
json.dump(C.ext.explain_last(details=True), sys.stdout, indent=4)
