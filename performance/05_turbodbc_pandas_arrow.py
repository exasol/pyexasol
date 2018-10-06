import turbodbc
import _config as config

C = turbodbc.connect(**config.turbodbc_connection_options)
cur = C.cursor()

cur.execute("ALTER SESSION SET QUERY_CACHE = 'OFF'")
cur.execute(f"SELECT * FROM {config.table_name}")

df = cur.fetchallarrow().to_pandas()
df.info()
