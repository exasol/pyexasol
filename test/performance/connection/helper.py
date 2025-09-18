from inspect import cleandoc

from pyexasol import ExaConnection


def create_empty_table(connection: ExaConnection, table_name: str):
    ddl = cleandoc(
        f"""
        CREATE OR REPLACE TABLE {table_name} (
            SALES_ID                DECIMAL(18,0) IDENTITY NOT NULL PRIMARY KEY,
            SALES_TIMESTAMP         TIMESTAMP,
            PRICE                   DECIMAL(9,2),
            CUSTOMER_NAME           VARCHAR(200)
          );
        """
    )
    connection.execute(ddl)
    connection.commit()
