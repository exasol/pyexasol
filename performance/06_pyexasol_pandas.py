import performance._config as config
import pyexasol

C = pyexasol.connect(
    dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
)
C.execute("ALTER SESSION SET QUERY_CACHE = 'OFF'")

df = C.export_to_pandas(config.table_name)
df.info()
