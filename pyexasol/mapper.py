import decimal
import datetime


class ExaTimeDelta(datetime.timedelta):
    def reverse_seconds(self):
        if self.microseconds > 0:
            seconds = 86399 - self.seconds
            microseconds = 1000000 - self.microseconds
        elif self.seconds > 0:
            seconds = 86400 - self.seconds
            microseconds = 0
        else:
            seconds = 0
            microseconds = 0
        return seconds, microseconds

    @classmethod
    def from_interval(cls, val):
        td = cls(
            days=int(val[0:10]),
            hours=int(val[11:13]),
            minutes=int(val[14:16]),
            seconds=int(val[17:19]),
            microseconds=int(round(float(val[20:29].ljust(9, '0')) / 1000)) if len(val) > 20 else 0,
        )
        if val[0] == '-':
            # normalize according to Python timedelta rules (days are negative; remaining parts apply back "up" towards 0)
            # - eg. -6 days, 1:00:00.000000 would represent 5 days, 23 hours ago (6 days back, 1 hour forward)
            (seconds, microseconds) = td.reverse_seconds()
            if seconds or microseconds:
                td = cls(days=td.days - 1, seconds=seconds, microseconds=microseconds)
            else:
                td = cls(days=td.days, seconds=seconds, microseconds=microseconds)
        return td

    @classmethod
    def from_timedelta(cls, td):
        return cls(days=td.days, seconds=td.seconds, microseconds=td.microseconds)

    def to_interval(self):
        if self.days < 0:
            (seconds, microseconds) = self.reverse_seconds()
            if seconds or microseconds:
                s = "-%09d " % (abs(self.days) - 1,)
            else:
                s = "-%09d " % (abs(self.days) - 1,)
        else:
            seconds = self.seconds
            microseconds = self.microseconds
            s = "+%09d " % (self.days,)

        mm, ss = divmod(seconds, 60)
        hh, mm = divmod(mm, 60)
        s = s + "%02d:%02d:%02d.%09d" % (hh, mm, ss, microseconds * 1000)
        return s

    def __str__(self):
        return self.to_interval()


def exasol_mapper(val, data_type):
    """
    Convert into Python 3 data types according to Exasol manual

    strptime() function is slow, so we use direct string slicing for performance sake
    More details about this problem: http://ze.phyr.us/faster-strptime/

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
    elif data_type['type'] == 'INTERVAL DAY TO SECOND':
        return ExaTimeDelta.from_interval(val)
    else:
        return val
