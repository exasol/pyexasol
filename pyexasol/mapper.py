from pyexasol.data_types.converters import ExaTimeDelta
from pyexasol.data_types.websocket_to_python import (
    convert_date,
    convert_decimal,
    convert_timestamp,
)


def exasol_mapper(val, data_type):
    """
    Convert into Python data types according to the Exasol manual

    DECIMAL(p,0)           -> int
    DECIMAL(p,s)           -> decimal.Decimal
    DOUBLE                 -> float
    DATE                   -> datetime.date
    TIMESTAMP              -> datetime.datetime
    BOOLEAN                -> bool
    VARCHAR                -> str
    CHAR                   -> str
    INTERVAL DAY TO SECOND -> datetime.timedelta
    <others>               -> str
    """

    if val is None:
        return None
    elif data_type["type"] == "DECIMAL":
        return convert_decimal(val, data_type["scale"])
    elif data_type["type"] == "DATE":
        return convert_date(val)
    elif data_type["type"] == "TIMESTAMP":
        return convert_timestamp(val)
    elif data_type["type"] == "INTERVAL DAY TO SECOND":
        return ExaTimeDelta.from_interval(val)
    return val
