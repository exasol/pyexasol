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
from warnings import warn

from pyexasol.warnings import PyexasolDeprecationWarning

SECONDS_PER_DAY = 24 * 60 * 60
MICROSECONDS_PER_SECOND = 1_000_000

_to_interval_warning_emitted = False


class ExaTimeDelta(datetime.timedelta):
    """Represent Exasol intervals and convert them in both directions.

    ``from_interval`` converts WebSocket values to Python, while
    ``to_interval_unpadded`` converts Python values back to Exasol's format.
    """

    def reverse_seconds(self) -> tuple[int, int]:
        """Return the seconds and microseconds complement relative to one day."""
        if self.microseconds > 0:
            complemented_seconds = SECONDS_PER_DAY - 1 - self.seconds
            complemented_microseconds = MICROSECONDS_PER_SECOND - self.microseconds
            return complemented_seconds, complemented_microseconds
        elif self.seconds > 0:
            complemented_seconds = SECONDS_PER_DAY - self.seconds
            return complemented_seconds, 0
        return 0, 0

    @classmethod
    def from_interval(cls, val: str) -> ExaTimeDelta:
        """
        Convert an Exasol ``INTERVAL DAY TO SECOND`` string to an ExaTimeDelta.

        The expected format is ``[+-]D...D HH:MM:SS.[N...N]``.
        Fractional seconds are rounded to Python's microsecond precision.
        """
        day_part, time_part = val.split(" ", maxsplit=1)
        hour_part, minute_part, second_part = time_part.split(":")
        second_part, _, fractional_part = second_part.partition(".")

        # Exasol supports nanosecond precision; round to Python's microseconds.
        microseconds = 0
        if fractional_part:
            nanoseconds = int(fractional_part.ljust(9, "0"))
            microseconds = int(round(nanoseconds / 1000))

        td = cls(
            days=int(day_part),
            hours=int(hour_part),
            minutes=int(minute_part),
            seconds=int(second_part),
            microseconds=microseconds,
        )
        if day_part[0] != "-":
            return td

        # Negative numbers are normalized according to Python's timedelta rules
        # (days are negative; remaining parts apply back "up" towards 0)
        #   -6 days, 1:00:00.000000 would represent 5 days, 23 hours ago (6 days back, 1 hour forward)
        seconds, microseconds = td.reverse_seconds()
        if seconds or microseconds:
            return cls(days=td.days - 1, seconds=seconds, microseconds=microseconds)
        return td

    @classmethod
    def from_timedelta(cls, td: datetime.timedelta) -> ExaTimeDelta:
        """Create an ExaTimeDelta from a datetime.timedelta."""
        return cls(days=td.days, seconds=td.seconds, microseconds=td.microseconds)

    def _interval_parts(self) -> tuple[str, int, int, int, int, int]:
        """Return the sign, days, hours, minutes, seconds and microseconds."""
        sign = "+"
        days = self.days
        seconds = self.seconds
        microseconds = self.microseconds
        if self.days < 0:
            # Python's timedelta normalizes hours and minutes into seconds.
            sign = "-"
            seconds, microseconds = self.reverse_seconds()
            days = abs(self.days)
            if seconds or microseconds:
                days -= 1

        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        return sign, days, hours, minutes, seconds, microseconds

    def to_interval(self) -> str:
        """
        Convert this duration to an Exasol ``INTERVAL DAY TO SECOND`` string.

        .. deprecated::
            Use ``to_interval_unpadded`` instead.

        The result uses the format ``[+-]DDDDDDDDD HH:MM:SS.NNNNNNNNN``.
        Python microseconds are represented with nanosecond precision by appending
        three zeros.
        """
        global _to_interval_warning_emitted

        if not _to_interval_warning_emitted:
            warn(
                "ExaTimeDelta.to_interval is deprecated and will be removed in the future. "
                "Use ExaTimeDelta.to_interval_unpadded instead.",
                PyexasolDeprecationWarning,
                stacklevel=2,
            )
            _to_interval_warning_emitted = True

        sign, days, hours, minutes, seconds, microseconds = self._interval_parts()
        day_string = f"{sign}{days:09d}"
        time_string = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        fractional_string = f"{microseconds * 1000:09d}"
        return f"{day_string} {time_string}.{fractional_string}"

    def to_interval_unpadded(self) -> str:
        """
        Convert this duration to an Exasol ``INTERVAL DAY TO SECOND`` string
        without zero-padding.

        The result uses the format ``[+-]D HH:MM:SS[.f..f]``. Unlike ``to_interval``,
        the result is not wider than necessary, so it fits narrower
        ``INTERVAL DAY(p) TO SECOND(f)`` columns.
        """
        sign, days, hours, minutes, seconds, microseconds = self._interval_parts()
        day_string = f"{sign}{days}"
        time_string = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        interval = f"{day_string} {time_string}"
        if microseconds:
            fractional_string = f"{microseconds:06d}".rstrip("0")
            interval += f".{fractional_string}"
        return interval

    def __str__(self) -> str:
        return self.to_interval()
