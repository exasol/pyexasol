# Changelog

## [Unreleased]

- Switch packaging and project workflows to poetry (internal)

## [0.25.2] - 2023-03-14

- Fix deprecation warning by setting SSLContext protocol.

## [0.25.1] - 2022-11-05

- Add hotfix for `lineterminator` change in Pandas 1.5.* ([details](https://github.com/pandas-dev/pandas/pull/45302))

## [0.25.0] - 2022-08-18

- HTTP transport callbacks are now executed inside a context manager for read or write pipe. It guarantees that pipe will be closed in the main thread regardless of successful execution -OR- exception in callback function.  It should help to prevent certain edge cases with pipes on Windows, when pipe `.close()` can block if called in unexpected order.
- HTTP transport "server" termination was simplified. Now it always closes "write" end of pipe first, followed by "read" end of pipe.
- Attempt to fix GitHub action SSL errors.

## [0.24.0] - 2022-02-12

- [Encryption](/docs/ENCRYPTION.md) is now enabled by default both for WebSocket and HTTP transport. Non-encrypted connections will be disabled in Exasol 8.0.

It may introduce some extra CPU overhead. If it becomes a problem, you may still disable encryption explicitly by setting `encryption=False`.

## [0.23.3] - 2021-12-03

- SSL certificate verification is now enabled when used with `access_token` or `refresh_token` connection options.
- Updated documentation regarding [encryption](/docs/ENCRYPTION.md).

OpenID tokens are used to connect to Exasol SAAS clusters, which are available using public internet address. Unlike Exasol clusters running "on-premises" and secured by corporate VPNs and private networks, SAAS clusters are at risk of MITM attacks. SSL certificates must be verified in such conditions.

Exasol SAAS certificates are properly configured using standard certificate authority, so no extra configuration is required.

## [0.23.2] - 2021-11-24

- Added initial implementation of certificate fingerprint validation, similar to standard JDBC / ODBC clients starting from version 7.1+.
- Replaced most occurrences of ambiguous word `host` in code with `hostname` or `ipaddr`, depending on context.

## [0.23.1] - 2021-11-21

- Improved termination logic for HTTP transport thread while handling an exception. Order of closing pipes now depends on type of callback (EXPORT or IMPORT). It should help to prevent unresponsive infinite loop on Windows.
- Improved parallel HTTP transport examples with better exception handling.
- Removed comment about `if __name__ == '__main__':` being required for Windows OS only. Multiprocessing on macOS uses `spawn` method in the most recent Python versions, so it is no longer unique.
- `pyopenssl` is now a hard dependency, which is required for encrypted HTTP transport to generate an "ad-hoc" certificate. Encryption will be enabled by default for SAAS Exasol in future.

## [0.23.0] - 2021-11-19

- Added [`orjson`](https://github.com/ijl/orjson) as possible option for `jsob_lib` connection parameter.
- Default `indent` for JSON debug output is now 2 (was 4) for more compact representation.
- `ensure_ascii` is now `False` (was `True`) for better readability and smaller payload size.
- Fixed JSON examples, `fetch_mapper` is now set correctly for second data set.

## [0.22.0] - 2021-11-19

**BREAKING (!):** HTTP transport was significantly reworked in this version. Now it uses [threading](https://docs.python.org/3/library/threading.html) instead of [subprocess](https://docs.python.org/3/library/subprocess.html) to handle CSV data streaming.

There are no changes in a common **single-process** HTTP transport.

There are some breaking changes in **parallel** HTTP transport:

- Argument `mode` was removed from [`http_transport()`](/docs/REFERENCE.md#http_transport) function, it is no longer needed.
- Word "proxy" used in context of HTTP transport was replaced with "exa_address" in documentation and code. Word "proxy" now refers to connections routed through an actual HTTP proxy only.
- Function `ExaHTTPTransportWrapper.get_proxy()` was replaced with property `ExaHTTPTransportWrapper.exa_address`. Function `.get_proxy()` is still available for backwards compatibility, but it is deprecated.
- Module `pyexasol_utils.http_transport` no longer exists.
- Constants `HTTP_EXPORT` and `HTTP_IMPORT` are no longer exposed in `pyexasol` module.

Rationale:

- Threading provides much better compatibility with Windows OS and various exotic setups (e.g. uWSGI).
- Orphan "http_transport" processes will no longer be a problem.
- Modern Pandas and Dask can (mostly) release GIL when reading or writing CSV streams.
- HTTP thread is primarily dealing with network I/O and zlib compression, which (mostly) release GIL as well.

Execution time for small data sets might be improved by 1-2s, since another Python interpreter is no longer started from scratch. Execution time for very large data sets might be ~2-5% worse for CPU bound workloads and unchanged for network bound workloads.

Also, [examples](/docs/EXAMPLES.md) were re-arranged in this version, refactored and grouped into multiple categories.

## [0.21.2] - 2021-11-11

- Fixed a bug in `ExaStatement` when no rows were fetched. It could happen when data set has less than 1000 rows, but the amount of data exceeds maximum chunk size.

## [0.21.1] - 2021-09-27

- "HTTP Transport" and "Script Output" subprocess will now restore default handler for SIGTERM signal.

In some cases custom signal handlers can be inherited from parent process, which causes unwanted side effects and prevents correct termination of child process.

## [0.21.0] - 2021-09-27

- Default `protocol_version` is now 3.
- Dropped support for Exasol versions `6.0` and `6.1`.

These versions have reached ["end of life"](https://www.exasol.com/portal/display/DOWNLOAD/Exasol+Life+Cycle) and are no longer supported by vendor. It is still possible to connect to older Exasol versions using PyEXASOL, but you may have to set `protocol_version=1` connection option explicitly.

## [0.20.0] - 2021-06-17

- Send `snapshotTransactionsEnabled` attribute only if set explicitly in connection option, prepare for Exasol 7.1 release.
- Default `snapshot_transaction` connection options is now `None` (database default), previously it was `False`.

## [0.19.0] - 2021-05-31

- Added connection options `access_token` and `refresh_token` to support OpenID Connect in [WebSocket Protocol V3](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV3.md).
- PyEXASOL default protocol version will be upgraded to `3` if connection option `access_token` or `refresh_token` were used.

## [0.18.1] - 2021-03-15

- Fixed orphan process check in HTTP Transport being enabled on Windows instead of POSIX OS.

## [0.18.0] - 2021-02-14

- Enforced TCP keep-alive for HTTP transport connections for Linux, MacOS and Windows. Keep-alive is required to address [Google Cloud firewall rules](https://cloud.google.com/compute/docs/troubleshooting/general-tips#communicatewithinternet) dropping idle connections after 10 minutes.

## [0.17.0] - 2021-02-05

- Added INTERVAL DAY TO SECOND data type support for standard fetch mapper `exasol_mapper`. Now it returns instances of class `ExaTimeDelta` derived from Python [datetime.timedelta](https://docs.python.org/3/library/datetime.html#datetime.timedelta).

It may potentially cause some issues with existing code. If it does, you may define your own custom `fetch_mapper`. Alternatively, you may call `ExaTimeDelta.to_interval()` or cast the object to string to get back to original values.

## [0.16.1] - 2020-12-23

- Added [`comment`](/docs/HTTP_TRANSPORT.md#parameters) parameter for HTTP transport. It allows adding custom SQL comments to EXPORT and IMPORT queries generated by HTTP transport query builders.

## [0.15.1] - 2020-11-20

- Added `websocket_sslopt` connection option, to set custom SSL options for WebSocket connection. See [WebSocket client code](https://github.com/websocket-client/websocket-client/blob/2222f2c49d71afd74fcda486e3dfd14399e647af/websocket/_http.py#L210-L272) for more details.
- Add a basic benchmark to compare performance of individual nodes. Documentation will be added shortly.

## [0.14.2] - 2020-11-02

- Run Travis tests with lowest (3.6) and highest (3.9) supported Python versions only.
- Updated description and classifiers for PyPi.

## [0.14.1] - 2020-08-11

- Fixed the problem with `delimit` HTTP transport parameter expecting `NONE` value instead of `NEVER`.

## [0.14.0] - 2020-08-01

### ExaConnection

- Added [`protocol_version`](/docs/REFERENCE.md#connect) connection option to adjust the protocol version requested by client (default: `pyexasol.PROTOCOL_V1`).
- Added [`.protocol_version()`](/docs/REFERENCE.md#protocol_version) function to check the actual protocol version of established connection.

### ExaMetaData

- Added [`.meta.execute_meta_nosql()`](/docs/REFERENCE.md#execute_meta_nosql) function to run [no SQL metadata commands](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV2.md#metadata-related-commands) introduced in Exasol v7.0+.
- Function [`.meta.execute_snapshot()`](/docs/REFERENCE.md#execute_snapshot) is not public. You may use it run complex metadata SQL queries in snapshot isolation mode.

### ExaStatement

- Added ability to execute no SQL metadata commands AND process the response as normal SQL-like result set. It does not change anything in public interface, but it might have an impact if you use custom overloaded `ExaStatement` class.

## [0.13.1] - 2020-06-02

- Re-throw `BrokenPipeError` (and other sub-classes of `ConnectionError`) as `ExaCommunicationError`. This type of errors might not be handled in WebSocket client library in certain cases.

## [0.13.0] - 2020-05-30

- Added optional [`disconnect`](https://github.com/exasol/websocket-api/blob/master/WebsocketAPI.md#disconnect-closes-a-connection-to-exasol) command executed during `.close()`. It is now enabled by default , but can be disabled with explicit `.close(disconnect=False)` to revert to original behaviour;
- Added `csv_cols` to [HTTP transport parameters](/docs/HTTP_TRANSPORT.md#parameters). It allows to skip some columns in CSV and adjust numeric and date format during IMPORT and EXPORT. It is still recommended to implement your own data transformation layer, since `csv_cols` capabilities are limited;

## [0.12.0] - 2020-03-02

- Added `.meta` sub-set of functions to execute lock-free meta data requests using `/*snapshot execution*/` SQL hint;
- Deprecated some `.ext` functions executing queries similar to `.meta` (code remains in place for compatibility);
- Added connection option `connection_timeout` in addition to existing option `socket_timeout`. `Connection_timeout` is applied during initial connection only and `socket_timeout` is applied for all other requests, including actual login procedure.
- Reworked error handling for HTTP transport to take care of even more complex failure scenarios;
- Reworked internals of SQL builder for IMPORT / EXPORT parameters;
- `ExaStatement` should now properly release result set handle after fetching and object termination;
- Removed `weakref`, it was not related to previous garbage collector problems;
- Renamed previously added `.connection_time` to `.login_time`, which is more accurate name for this timer;
- Query text length in `ExaQueryError` exception is now limited to 20k characters to prevent logs from bloating;
- Fixed `open_schema` function with `quote_ident=True`;
- `.last_statement()` now always returns last `ExaStatement` executed on this connection. Previously it was skipping technical queries from `ExaExtension` (.ext);

## [0.11.2] - 2020-01-27

### ExaConnection

- Added option `client_os_username` to specify custom client OS username. Previously username was always detected automatically with `getpass.getuser()`, but it might not work in some environments, like OpenShift.

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
