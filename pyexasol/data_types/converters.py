"""
Helpers for converting values between Exasol's database format and Python
types.

PyExasol communicates with Exasol over the WebSocket protocol. These helpers
perform both directions of conversion: they translate values returned by
Exasol into appropriate Python representations and serialize Python values
back into the formats expected by Exasol.
"""

from __future__ import annotations

import datetime


class ExaTimeDelta(datetime.timedelta):
    def reverse_seconds(self) -> tuple[int, int]:
        """Return the complemented seconds and microseconds of a negative duration."""
        if self.microseconds > 0:
            seconds = 86399 - self.seconds
            microseconds = 1000000 - self.microseconds
            return seconds, microseconds
        elif self.seconds > 0:
            seconds = 86400 - self.seconds
            return seconds, 0
        return 0, 0

    @classmethod
    def from_interval(cls, val: str) -> ExaTimeDelta:
        """
        Convert an Exasol ``INTERVAL DAY TO SECOND`` string to an ExaTimeDelta.

        The expected format is ``[+-]DDDDDDDDD HH:MM:SS.NNNNNNNNN``.
        Nanoseconds are rounded to Python's microsecond precision.
        """
        # Exasol supports nanosecond precision; round to Python's microseconds.
        microseconds = 0
        if len(val) > 20:
            fractional_seconds = val[20:29]
            fractional_nanoseconds = float(fractional_seconds.ljust(9, "0"))
            microseconds = int(round(fractional_nanoseconds / 1000))

        td = cls(
            days=int(val[0:10]),
            hours=int(val[11:13]),
            minutes=int(val[14:16]),
            seconds=int(val[17:19]),
            microseconds=microseconds,
        )
        if val[0] != "-":
            return td

        # Negative numbers are normalized according to Python's timedelta rules
        # (days are negative; remaining parts apply back "up" towards 0)
        #   -6 days, 1:00:00.000000 would represent 5 days, 23 hours ago (6 days back, 1 hour forward)
        seconds, microseconds = td.reverse_seconds()
        if seconds or microseconds:
            return cls(days=td.days - 1, seconds=seconds, microseconds=microseconds)
        return cls(days=td.days, seconds=seconds, microseconds=microseconds)

    @classmethod
    def from_timedelta(cls, td: datetime.timedelta) -> ExaTimeDelta:
        """Create an ExaTimeDelta from a datetime.timedelta."""
        return cls(days=td.days, seconds=td.seconds, microseconds=td.microseconds)

    def to_interval(self) -> str:
        """
        Convert this duration to an Exasol ``INTERVAL DAY TO SECOND`` string.

        The result uses the format ``[+-]DDDDDDDDD HH:MM:SS.NNNNNNNNN``.
        Python microseconds are represented with nanosecond precision by appending
        three zeros.
        """
        if self.days < 0:
            # Python's timedelta normalizes hours and minutes into seconds.
            seconds, microseconds = self.reverse_seconds()
            if seconds or microseconds:
                s = "-%09d " % (abs(self.days) - 1,)
            else:
                s = "-%09d " % abs(self.days)
        else:
            seconds = self.seconds
            microseconds = self.microseconds
            s = "+%09d " % (self.days,)

        mm, ss = divmod(seconds, 60)
        hh, mm = divmod(mm, 60)
        s = s + "%02d:%02d:%02d.%09d" % (hh, mm, ss, microseconds * 1000)
        return s

    def __str__(self) -> str:
        return self.to_interval()
