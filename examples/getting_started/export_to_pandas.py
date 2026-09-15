"""
Getting Started - Export data to pandas DataFrame
"""

import examples._config as config
import pyexasol

# pip install pyexasol[pandas]
C = pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    compression=True,
    websocket_sslopt=config.websocket_sslopt,
)

df = C.export_to_pandas("SELECT * FROM EXA_ALL_USERS")
print(df.head())

C.close()
