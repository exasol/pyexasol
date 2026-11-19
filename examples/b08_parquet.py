"""
HTTP Transport

EXPORT and IMPORT from Exasol to Parquet File(s)
Make sure to enable compression for Wi-Fi connections to improve performance
"""

import tempfile

from pyarrow import parquet as pq

import examples._config as config
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

    # Export from Exasol table into the temporary directory into a local parquet file.
    # The data is transferred as CSV, so BOOLEAN columns are exported as 1/0 and read as
    # int64 by default. The native Parquet import (Exasol 2026.1.0 and newer) rejects
    # int64 for BOOLEAN columns, so we export booleans as true/false to get a bool column.
    C.export_to_parquet(
        dst=directory_path,
        query_or_table="users",
        export_params={"boolean": "true/false"},
    )
    parquet_table = pq.read_table(directory_path)
    print(parquet_table.schema)

    stmt = C.last_statement()
    print(f"EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s")

    # Prepare empty table for import. The native Parquet import (Exasol 2026.1.0 and
    # newer) does not convert between types, e.g. a Parquet double cannot be imported
    # into a DECIMAL column. As the CSV-based export yields doubles for DECIMAL(10,5),
    # the target table declares user_rating as DOUBLE.
    C.execute("""
        CREATE OR REPLACE TABLE users_copy
        (
            user_id         DECIMAL(18,0),
            user_name       VARCHAR(255),
            register_dt     DATE,
            last_visit_ts   TIMESTAMP,
            is_female       BOOLEAN,
            user_rating     DOUBLE,
            user_score      DOUBLE,
            status          VARCHAR(50)
        )
    """)

    # import_from_parquet allows for users to specify parquet files in multiple ways;
    # this example usage using a string with a glob pattern is just one of them.
    filepath_glob = f"{directory_path}/*.parquet"
    # Import from temporary directory with a local parquet file into an Exasol table
    C.import_from_parquet(source=filepath_glob, table="users_copy")

    imported_rows = C.execute("SELECT COUNT(*) FROM users_copy").fetchval()
    print(f"IMPORTED {imported_rows} rows")

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
