# 0.14.0 - 2020-08-01

## ExaConnection

- Added `protocol_version` connection option to adjust the protocol version requested by client (default: `pyexasol.PROTOCOL_V1`).
- Added `.protocol_version()` function to check the actual protocol version of established connection.

## ExaMetaData

- Added `.meta.execute_meta_nosql()` function to run **no SQL metadata commands** introduced in Exasol v7.0+.
- Function `.meta.execute_snapshot()` is not public. You may use it run complex metadata SQL queries in snapshot isolation mode.

## ExaStatement

- Added ability to execute no SQL metadata commands AND process the response as normal SQL-like result set. It does not change anything in public interface, but it might have an impact if you use custom overloaded `ExaStatement` class.

