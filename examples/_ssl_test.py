import pprint
import ssl

import pyexasol

printer = pprint.PrettyPrinter(indent=4, width=140)


with pyexasol.connect(
    dsn="exasol-test-database:8888",
    user="sys",
    password="exasol",
    encryption=True,
    websocket_sslopt={"cert_reqs": ssl.CERT_REQUIRED, "ca_certs": "rootCA.crt"},
) as C:

    # Basic select
    stmt = C.execute("SELECT * FROM exa_all_users LIMIT 5")
    printer.pprint(stmt.fetchall())

    # Basic HTTP transport
    pd = C.export_to_pandas("exa_all_users")
    pd.info()

    # Dump certificate info
    printer.pprint(C._ws.sock.getpeercert())
