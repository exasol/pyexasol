# Changelog

## [0.8.0] - 2019-09-24

### ExaConnection

- DSN hostname ranges with zero-padding are now supported (e.g. `myhost01..16`).
- Context manager ("with" statement) is now supported for connection object.

### ExaStatement

- Context manager ("with" statement) is now supported for statement object.

## [0.7.0] - 2019-08-25

### ExaConnection

- Added read-only [`.options`](/docs/REFERENCE.md#options) property holding original arguments used to create ExaConnection object.
- Added read-only [`.login_info`](/docs/REFERENCE.md#login_info) property holding response data of LOGIN command.
- Added documentation for read-only [`.attr`](/docs/REFERENCE.md#attr) property holding attributes of current connection (autocommit state, etc.).
- Removed undocumented `.meta` property, renamed it to `.login_info`.
- Removed undocumented `.last_stmt` property. Please use [`.last_statement()`](/docs/REFERENCE.md#last_statement) function instead.
- Removed most of exposed properties related to connection options (e.g. `.autocommit`). Please use `.options` or `.attr` instead.

### ExaStatement

- Added documentation for read-only [`.execution_time`](/docs/REFERENCE.md#execution_time) property holding wall-clock execution time of SQL statement.
