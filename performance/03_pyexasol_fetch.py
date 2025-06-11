import performance._config as config
import pyexasol

C = pyexasol.connect(
    dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
)
C.execute("ALTER SESSION SET QUERY_CACHE = 'OFF'")

st = C.execute("SELECT * FROM {table_name!i}", {"table_name": config.table_name})
st.fetchall()
