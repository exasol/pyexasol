# 0.17.0 - 2021-02-05

- Added INTERVAL DAY TO SECOND data type support for standard fetch mapper `exasol_mapper`. Now it returns instances of class `ExaTimeDelta` derived from Python [datetime.timedelta](https://docs.python.org/3/library/datetime.html#datetime.timedelta).

It may potentially cause some issues with existing code. If it does, you may define your own custom `fetch_mapper`. Alternatively, you may call `ExaTimeDelta.to_interval()` or cast the object to string to get back to original values.

