"""
HTTP Transport

EXPORT and IMPORT from Exasol to Parquet File(s)
Make sure to enable compression for Wi-Fi connections to improve performance
"""

import tempfile

import _config as config
from pyarrow import parquet as pq

import pyexasol

# Connect with compression enabled
C = pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    schema=config.schema,
    compression=True,
    websocket_sslopt=config.websocket_sslopt,
)

with tempfile.TemporaryDirectory() as directory_path:

    # Export from Exasol table into the temporary directory into a local parquet file
    C.export_to_parquet(dst=directory_path, query_or_table="users")
    parquet_table = pq.read_table(directory_path)
    print(parquet_table.schema)

    stmt = C.last_statement()
    print(f"EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s")

    # Prepare empty table for import
    C.execute("TRUNCATE TABLE users_copy")

    # import_from_parquet allows for users to specify parquet files in multiple ways;
    # this example usage using a string with a glob pattern is just one of them.
    filepath_glob = f"{directory_path}/*.parquet"
    # Import from temporary directory with a local parquet file into an Exasol table
    C.import_from_parquet(source=filepath_glob, table="users_copy")

    stmt = C.last_statement()
    print(f"IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s")

    basename_template = "second-export_{i}.parquet"
    # Export from SQL query
    C.export_to_parquet(
        dst=directory_path,
        query_or_table="SELECT user_id, user_name FROM users WHERE user_id >= 5000",
        callback_params={
            # for this example, it is ok if we use a directory where data is already stored
            "existing_data_behavior": "overwrite_or_ignore",
            # for this example, we'd want to change the basename_template so that first
            # exported file is retained
            "basename_template": basename_template,
        },
    )

    parquet_table = pq.read_table(f"{directory_path}/{basename_template.format(i=0)}")
    print(parquet_table.schema)

    stmt = C.last_statement()
    print(f"EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s")
