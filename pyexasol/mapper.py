import decimal
import datetime


def exasol_mapper(val, data_type):
    """
    Convert into Python 3 data types according to Exasol manual

    strptime() function is slow, so we use direct string slicing for performance sake
    More details about this problem: http://ze.phyr.us/faster-strptime/

    DECIMAL(p,0) -> int
    DECIMAL(p,s) -> decimal.Decimal
    DOUBLE       -> float
    DATE         -> datetime.date
    TIMESTAMP    -> datetime.datetime
    BOOLEAN      -> bool
    VARCHAR      -> str
    CHAR         -> str
    <others>     -> str
    """

    if val is None:
        return None
    elif data_type['type'] == 'DECIMAL':
        if data_type['scale'] == 0:
            return int(val)
        else:
            return decimal.Decimal(val)
    elif data_type['type'] == 'DATE':
        return datetime.date(int(val[0:4]), int(val[5:7]), int(val[8:10]))
    elif data_type['type'] == 'TIMESTAMP':
        return datetime.datetime(int(val[0:4]), int(val[5:7]), int(val[8:10]),           # year, month, day
                                 int(val[11:13]), int(val[14:16]), int(val[17:19]),      # hour, minute, second
                                 int(val[20:26].ljust(6, '0')) if len(val) > 20 else 0)  # microseconds (if available)
    else:
        return val
