import pyexasol

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)


with pyexasol.connect(dsn='exasol-test-database:8888', user='sys', password='exasol', encryption=True) as C:

    # Basic select
    stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
    printer.pprint(stmt.fetchall())

    # Basic HTTP transport
    pd = C.export_to_pandas('users')
    pd.info()

    # Dump certificate info
    printer.pprint(C._ws.sock.getpeercert())
