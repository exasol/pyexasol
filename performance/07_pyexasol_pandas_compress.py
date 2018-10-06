import pyexasol
import _config as config

C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
                     , compression=True)
C.execute("ALTER SESSION SET QUERY_CACHE = 'OFF'")

df = C.export_to_pandas(config.table_name)
df.info()
