from pyexasol.data_types.converters import ExaTimeDelta


class TestExaTimeDelta:
    @staticmethod
    def test_str_returns_interval_representation():
        value = ExaTimeDelta(days=1, seconds=2)
        assert str(value) == "+000000001 00:00:02.000000000"
