from inspect import cleandoc

import pytest


@pytest.mark.transaction
class TestTransaction:
    @pytest.fixture(scope="class")
    def connection(self, connection_factory):
        # We need to configure the connection accordingly (autocommit=False)
        con = connection_factory(autocommit=False)
        yield con
        con.close()

    @pytest.fixture(scope="class")
    def table(self, connection):
        table_name = "cities"
        ddl = cleandoc(
            f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ID DECIMAL(18,0),
            NAME VARCHAR(255),
            POSTCODE DECIMAL(18,0)
        );
        """
        )
        connection.execute(ddl)
        connection.commit()

        yield table_name

        delete_stmt = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
        connection.execute(delete_stmt)
        connection.commit()

    @pytest.fixture
    def filled_table(self, connection, table, faker):
        items = [(i, faker.city(), faker.postcode()) for i in range(0, 1)]
        connection.ext.insert_multi(table, items)
        connection.commit()

    def test_rollback(self, connection, table, filled_table):
        count = f"SELECT COUNT(*) FROM {table};"
        delete_rows = f"TRUNCATE TABLE {table};"
        initial_rowcount = connection.execute(count).fetchval()

        connection.execute(delete_rows)
        expected = 0
        actual = connection.execute(count).fetchval()
        assert expected == actual

        connection.rollback()

        actual = connection.execute(count).fetchval()
        expected = initial_rowcount

        assert expected == actual

    def test_commit(self, connection, table, filled_table):
        count = f"SELECT COUNT(*) FROM {table};"
        delete_rows = f"TRUNCATE TABLE {table};"
        initial_rowcount = connection.execute(count).fetchval()

        assert initial_rowcount != 0

        connection.execute(delete_rows)
        connection.commit()
        connection.rollback()

        actual = connection.execute(count).fetchval()
        expected = 0

        # prior to assert, undo modification for remaining tests

        assert expected == actual
