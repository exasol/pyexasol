from inspect import cleandoc

import pytest

import pyexasol
from pyexasol import ExaRuntimeError


@pytest.mark.extensions
class TestInsertMultiExtension:

    @pytest.fixture(scope="class")
    def connection(self, connection_factory):
        con = connection_factory(lower_ident=True)
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

    def test_insert_multi(self, connection, table, faker):
        items = [(i, faker.city(), faker.postcode()) for i in range(0, 40)]
        connection.ext.insert_multi(table, items)
        connection.commit()

        result = connection.execute(f"SELECT * FROM {table} ORDER BY ID;")
        actual = result.fetchall()
        expected = [
            (0, "Changchester", 5807),
            (1, "Tammyfort", 53576),
            (2, "Tammystad", 76966),
            (3, "New Donald", 18817),
            (4, "New Laurenside", 92929),
            (5, "West Corey", 10166),
            (6, "Thomasville", 46873),
            (7, "Stewartborough", 72921),
            (8, "Ramoshaven", 34644),
            (9, "Port Samanthatown", 12726),
            (10, "Pagetown", 80703),
            (11, "Claytonmouth", 32470),
            (12, "Janeland", 25544),
            (13, "East Linda", 19178),
            (14, "Davismouth", 67077),
            (15, "Rodriguezside", 38654),
            (16, "New Marvinside", 27135),
            (17, "Seanfurt", 42056),
            (18, "West Ivan", 24976),
            (19, "New Tinaview", 62960),
            (20, "North Erikaberg", 20102),
            (21, "Port Christopherside", 90088),
            (22, "Masseyhaven", 36628),
            (23, "Port Jason", 89561),
            (24, "Rileymouth", 65074),
            (25, "Christopherville", 64260),
            (26, "Meganbury", 2625),
            (27, "Davebury", 49267),
            (28, "West Ronald", 8652),
            (29, "Port Dustinbury", 6429),
            (30, "North Davidborough", 79974),
            (31, "South Christopherville", 5270),
            (32, "East Susanburgh", 94625),
            (33, "South Jasonton", 89544),
            (34, "New Ronaldville", 34569),
            (35, "East Charlesport", 46414),
            (36, "Whitehaven", 61728),
            (37, "New Craigfurt", 26630),
            (38, "Michaelfort", 75181),
            (39, "Lake Emily", 8109),
        ]

        assert actual == expected

    def test_insert_multi_without_data(self, connection, table):
        items = []
        with pytest.raises(ExaRuntimeError):
            connection.ext.insert_multi(table, items)
