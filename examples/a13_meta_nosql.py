"""
No SQL lock-free meta data requests introduced in Exasol 7.0, WebSocket protocol v2
"""

import pprint
import sys

import _config as config

import pyexasol

printer = pprint.PrettyPrinter(indent=4, width=140)


C = pyexasol.connect(
    dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
)

if C.protocol_version() < pyexasol.PROTOCOL_V2:
    print("Actual protocol version is less than 2, skipping meta_nosql checks")
    sys.exit(0)


# Schema exists
val = C.meta.schema_exists(C.current_schema())
print(f"Schema exists: {val}")

# Schema does not exist
val = C.meta.schema_exists("abcabcabc")
print(f"Schema exists: {val}")


# Table exists
val = C.meta.table_exists("users")
print(f"Table exists: {val}")

# Table exists (with schema name)
val = C.meta.table_exists((C.current_schema(), "users"))
print(f"Table exists: {val}")

# Table does not exist
val = C.meta.table_exists("abcabcabc")
print(f"Table exists: {val}")


# View exists
val = C.meta.view_exists("users_view")
print(f"View exists: {val}")

# View exists (with schema name)
val = C.meta.view_exists((C.current_schema(), "users_view"))
print(f"View exists: {val}")

# View does not exist
val = C.meta.view_exists("abcabcabc")
print(f"View exists: {val}")


# List schemas
st = C.meta.execute_meta_nosql("getSchemas")
printer.pprint(st.fetchall())

# List schemas with filters
st = C.meta.execute_meta_nosql("getSchemas", {"schema": "PYEXASOL%"})
printer.pprint(st.fetchall())


# List tables with filters
st = C.meta.execute_meta_nosql(
    "getTables",
    {
        "schema": "PYEXASOL%",
        "table": "USERS%",
        "tableTypes": ["TABLE"],
    },
)
printer.pprint(st.fetchall())


# List views with filters
st = C.meta.execute_meta_nosql(
    "getTables",
    {
        "schema": "PYEXASOL%",
        "table": "USERS%",
        "tableTypes": ["VIEW"],
    },
)
printer.pprint(st.fetchall())


# List columns with filters
st = C.meta.execute_meta_nosql(
    "getColumns",
    {
        "schema": "PYEXASOL%",
        "table": "USERS%",
        "column": "%_ID%",
    },
)
printer.pprint(st.fetchall())


# List keywords
val = C.meta.list_sql_keywords()
printer.pprint(val[0:10])


# Exception handling
try:
    st = C.meta.execute_meta_nosql(
        "getColumns",
        {
            "schema": "PYEXASOL%",
            "table": "USERS%",
            "column": ["%_ID%"],
        },
    )
except pyexasol.ExaRequestError as e:
    print(e)
