"""
Getting Started - Export data to parquet file(s)
"""

import tempfile
from pathlib import Path

import examples._config as config
import pyexasol

# pip install pyexasol[pyarrow]
C = pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    compression=True,
    websocket_sslopt=config.websocket_sslopt,
)

# export_to_parquet requires a destination directory, not a query alone
with tempfile.TemporaryDirectory() as dst:
    C.export_to_parquet(dst=dst, query_or_table="SELECT * FROM EXA_ALL_USERS")
    files = list(Path(dst).glob("*.parquet"))
    print(f"Exported {len(files)} parquet file(s) to {dst}")

C.close()
