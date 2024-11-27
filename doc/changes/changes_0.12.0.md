# 0.12.0 - 2020-03-02

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

