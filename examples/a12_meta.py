"""
Lock-free meta data requests
"""

import pprint

import _config as config

import pyexasol

printer = pprint.PrettyPrinter(indent=4, width=140)


C = pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    schema=config.schema,
    websocket_sslopt=config.websocket_sslopt,
)

# Get columns without executing SQL query
columns = C.meta.sql_columns("SELECT a.*, a.user_id + 1 AS next_user_id FROM users a")
printer.pprint(columns)


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
val = C.meta.list_schemas()
printer.pprint(val)

# List schemas with filters
val = C.meta.list_schemas(schema_name_pattern="PYEXASOL%")
printer.pprint(val)


# List tables with filters
val = C.meta.list_tables(table_schema_pattern="PYEXASOL%", table_name_pattern="USERS%")
printer.pprint(val)


# List views with filters
val = C.meta.list_views(view_schema_pattern="PYEXASOL%", view_name_pattern="USERS%")
printer.pprint(val)


# List columns with filters
val = C.meta.list_columns(
    column_schema_pattern="PYEXASOL%",
    column_table_pattern="USERS%",
    column_object_type_pattern="TABLE",
    column_name_pattern="%_ID%",
)
printer.pprint(val)


# List objects with filters
val = C.meta.list_objects(object_name_pattern="USERS%", object_type_pattern="TABLE")
printer.pprint(val)


# List object sizes with filters
val = C.meta.list_object_sizes(
    object_name_pattern="USERS%", object_type_pattern="TABLE"
)
printer.pprint(val)


# List indices
val = C.meta.list_indices(index_schema_pattern="PYEXASOL%")
printer.pprint(val)


# List keywords
val = C.meta.list_sql_keywords()
printer.pprint(val)
