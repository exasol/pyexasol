"""
Use prepared statements for INSERT and SELECT queries
"""

import examples._config as config
import pyexasol

table_name = "PREPARED_STMT_EXAMPLE"

C = pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    schema=config.schema,
    websocket_sslopt=config.websocket_sslopt,
    autocommit=False,
)

try:
    C.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name}
        (
            ID      DECIMAL(18,0),
            NAME    VARCHAR(16)
        )
        """
    )
    # Create an insert prepared statement
    insert_stmt = C.create_prepared_statement(
        f"INSERT INTO {table_name} (ID, NAME) VALUES (?, ?)"
    )
    # Execute twice the same prepared statement for different parameters data
    insert_stmt.execute_prepared([(0, "A"), (1, "B"), (2, "C")])
    print(f"INSERTED {insert_stmt.rowcount()} rows")

    insert_stmt.execute_prepared([(3, "D"), (4, "E")])
    print(f"INSERTED {insert_stmt.rowcount()} rows")

    # Close the prepared statement
    insert_stmt.close()
    print("INSERT prepared statement was closed")

    # Create and execute a prepared statement without placeholders.
    select_all_stmt = C.create_prepared_statement(
        f"SELECT ID, NAME FROM {table_name} ORDER BY ID"
    )
    select_all_stmt.execute_prepared()
    print(select_all_stmt.fetchall())

    # Create a select prepared statement
    select_stmt = C.create_prepared_statement(
        f"SELECT ID, NAME FROM {table_name} WHERE ID >= ? ORDER BY ID"
    )
    # Execute twice the same select prepared statement fetching the results in between
    select_stmt.execute_prepared([(1,)])
    print(select_stmt.fetchall())

    select_stmt.execute_prepared([(1,)])
    print(select_stmt.fetchall())

    # Execute previously created statement without placeholders.
    select_all_stmt.execute_prepared()
    print(select_all_stmt.fetchall())


finally:
    C.execute(f"DROP TABLE IF EXISTS {table_name}")
    C.close()
