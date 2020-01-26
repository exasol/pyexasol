# Changelog

## [0.11.1] - 2020-01-26

### ExaConnection

- Added `.connection_time` property to measure execution time of two login requests required to establish connection.

## [0.11.0] - 2020-01-25

### ExaConnection

- Reworked `close()` method. It is now sending basic `OP_CLOSE` WebSocket frame instead of `disconnect` command.
- Method `close()` is now called implicitly during destruction of `ExaConnection` object to terminate IDLE session and free resources on Exasol server side immediately.
- `ExaFormatter`, `ExaExtension`, `ExaLogger` objects now have [weak reference](https://docs.python.org/3/library/weakref.html) to the main `ExaConnection` object. It helps to prevent circular reference problem which stopped `ExaConnection` object from being processed by Python garbage collector.
- Connection will be closed automatically after receiving `WebSocketException` and raising `ExaCommunicationError`. It should prevent connection from being stuck in invalid state.

## [0.10.0] - 2020-01-01

### PyEXASOL code improvements

- Reworked script output code and moved it into `pyexasol_utils` module. The new way to start script output server in debug mode is: `python -m pyexasol_utils.script_output`. Old call will produce the RuntimeException with directions.
- Removed `.utils` sub-module.
- Renamed `pyexasol_utils.http` into `pyexasol_utils.http_transport` for consistency.

### ExaConnection

- Fixed bug of `.execute_udf_output()` not working with empty `udf_output_bind_address`.
- Added function `_encrypt_password()`, logic was moved from `.utils`.
- Added function `_get_stmt_output_dir()`, logic was moved from `.utils`. It is now possible to overload this function.

## [0.9.2] - 2019-12-08

### ExaExtension

- Metadata functions (starting with `.ext.get_sys_*`) are now using `/*snapshot execution*/` SQL hint described in [IDEA-476](https://www.exasol.com/support/browse/IDEA-476) to prevent locks.

## [0.9.1] - 2019-10-22

### ExaExtension

- Added [`insert_multi`](/docs/REFERENCE.md#insert_multi) function to allow faster INSERT's for small data sets using prepared statement.

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
