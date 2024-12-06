import _config as config
import pandas
import turbodbc

C = turbodbc.connect(**config.turbodbc_connection_options)
cur = C.cursor()

cur.execute("ALTER SESSION SET QUERY_CACHE = 'OFF'")
cur.execute(f"SELECT * FROM {config.table_name}")

df = pandas.DataFrame(data=cur.fetchallnumpy())
df.info()
