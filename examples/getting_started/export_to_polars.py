"""
Getting Started - Export data to polars DataFrame
"""

# pip install pyexasol[polars]
import examples._config as config
import pyexasol

with pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    compression=True,
    websocket_sslopt=config.websocket_sslopt,
) as C:
    df = C.export_to_polars("SELECT * FROM EXA_ALL_USERS")
    print(df.head())
