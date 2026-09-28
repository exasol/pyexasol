import datetime

from pyexasol.mapper import exasol_mapper


class TestExasolMapper:
    def test_maps_date(self):
        expected_date = datetime.date(2026, 9, 11)
        value = expected_date.strftime("%Y-%m-%d")

        assert exasol_mapper(value, {"type": "DATE"}) == expected_date

    def test_maps_timestamp(self):
        expected_timestamp = datetime.datetime(2026, 9, 11, 12, 34, 56)
        value = expected_timestamp.strftime("%Y-%m-%d %H:%M:%S")

        assert exasol_mapper(value, {"type": "TIMESTAMP"}) == expected_timestamp
