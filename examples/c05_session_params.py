"""
Custom client name, client version, other session parameters
Useful for logging
"""

import pprint

import _config as config

import pyexasol

printer = pprint.PrettyPrinter(indent=4, width=140)

# Normal session
C = pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    schema=config.schema,
    fetch_dict=True,
)

st = C.execute("SELECT * FROM EXA_DBA_SESSIONS WHERE session_id=CURRENT_SESSION")
printer.pprint(st.fetchall())

C.close()

# Modified session
C = pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    schema=config.schema,
    fetch_dict=True,
    client_name="MyCustomClient",
    client_version="1.2.3",
    client_os_username="small_cat",
)

st = C.execute("SELECT * FROM EXA_DBA_SESSIONS WHERE session_id=CURRENT_SESSION")
printer.pprint(st.fetchall())

C.close()
