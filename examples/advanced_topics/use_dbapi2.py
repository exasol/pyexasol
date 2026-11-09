"""Basic usage of the maintained DB-API 2.0 facade."""

import pprint
from contextlib import closing

import examples._config as config
from exasol.driver.websocket import dbapi2

printer = pprint.PrettyPrinter(indent=4, width=140)

with closing(
    dbapi2.connect(
        dsn=config.dsn,
        username=config.user,
        password=config.password,
        schema=config.schema,
        autocommit=False,
        certificate_validation=False,
    )
) as connection:
    # Query and result operations require an active cursor.
    with connection.cursor() as cursor:
        # Fetch tuples row-by-row as iterator
        cursor.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")

        while True:
            row = cursor.fetchone()

            if row is None:
                break

            printer.pprint(row)

        # Fetch many
        cursor.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
        printer.pprint(cursor.fetchmany(3))
        printer.pprint(cursor.fetchmany(3))

        # Fetch everything in one go
        cursor.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
        printer.pprint(cursor.fetchall())

        printer.pprint(cursor.description)
        printer.pprint(cursor.rowcount)

    # The connection remains available after the cursor is closed.
    connection.commit()
