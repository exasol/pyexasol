import _config as config
import pyodbc

C = pyodbc.connect(**config.odbc_connection_options)
cur = C.cursor()

cur.execute("ALTER SESSION SET QUERY_CACHE = 'OFF'")

cur.execute(f"SELECT * FROM {config.table_name}")
cur.fetchall()
