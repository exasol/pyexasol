# Reference

This page contains complete reference of PyEXASOL public API.

- [connect()](#connect)
- [connect_local_config()](#connect_local_config)
- [http_transport()](#http_transport)
- [ExaConnection](#exaconnection)
  - [execute()](#execute)
  - [execute_udf_output()](#execute_udf_output)
  - [commit()](#commit)
  - [rollback()](#rollback)
  - [set_autocommit()](#set_autocommit)
  - [set_query_timeout()](#set_query_timeout)
  - [open_schema()](#open_schema)
  - [current_schema()](#current_schema)
  - [export_to_file()](#export_to_file)
  - [export_to_list()](#export_to_list)
  - [export_to_pandas()](#export_to_pandas)
  - [export_to_callback()](#export_to_callback)
  - [export_parallel()](#export_parallel)
  - [import_from_file()](#import_from_file)
  - [import_from_iterable()](#import_from_iterable)
  - [import_from_pandas()](#import_from_pandas)
  - [import_from_callback()](#import_from_callback)
  - [import_parallel()](#import_parallel)
  - [get_nodes()](#get_nodes)
  - [session_id()](#session_id)
  - [last_statement()](#last_statement)
  - [abort_query()](#abort_query)
  - [close()](#exaconnectionclose)
- [ExaStatement](#exastatement)
  - [\_\_iter\_\_()](#__iter__)
  - [fetchone()](#fetchone)
  - [fetchmany()](#fetchmany)
  - [fetchall()](#fetchall)
  - [fetchcol()](#fetchcol)
  - [fetchval()](#fetchval)
  - [rowcount()](#rowcount)
  - [columns()](#columns)
  - [column_names()](#column_names)
  - [close()](#exastatementclose)
- [ExaFormatter](#exaformatter)
  - [format()](#format)
  - [escape()](#escape)
  - [escape_ident()](#escape_ident)
  - [escape_like()](#escape_like)
  - [quote()](#quote)
  - [quote_ident()](#quote_ident)
  - [safe_ident()](#safe_ident)
  - [safe_float()](#safe_float)
  - [safe_decimal()](#safe_decimal)
- [ExaHTTPTransportWrapper](#exahttptransportwrapper)
  - [get_proxy()](#exahttptransportwrapperget_proxy)
  - [export_to_callback()](#exahttptransportwrapperexport_to_callback)
  - [import_from_callback()](#exahttptransportwrapperimport_from_callback)
- [ExaExtension](#exaextension)
  - [get_columns()](#get_columns)
  - [get_columns_sql()](#get_columns_sql)
  - [get_sys_columns()](#get_sys_columns)
  - [get_sys_tables()](#get_sys_tables)
  - [get_sys_views()](#get_sys_views)
  - [get_sys_schemas()](#get_sys_schemas)
  - [get_reserved_words()](#get_reserved_words)
  - [get_disk_space_usage()](#get_disk_space_usage)
  - [explain_last()](#explain_last)

## connect()
Opens new connection and returns `ExaConnection` object.

| Argument | Example | Description |
| --- | --- | --- |
| `dsn` | `exasolpool1..5.mlan:8563` `10.10.127.1..11:8564` | Connection string, same format as standard JDBC / ODBC drivers |
| `user` | `sys` | Username |
| `password` | `exasol` | Password |
| `schema` | `ingres` | Open schema after connection (Default: `''`, no schema) |
| `autocommit` | `True` | Enable autocommit on connection (Default: `True`) |
| `snapshot_transactions` | `False` | Enable [snapshot transactions](/docs/SNAPSHOT_TRANSACTIONS.md) on connection (Default: `False`) |
| `socket_timeout` | `10` | Socket timeout in seconds passed directly to websocket (Default: `10`) |
| `query_timeout` | `0` | Maximum execution time of queries before automatic abort (Default: `0`, no timeout) |
| `compression` | `True` | Use zlib compression both for WebSocket and HTTP transport (Default: `False`) |
| `encryption` | `True` | Use [SSL encryption](/docs/ENCRYPTION.md) for WebSocket communication and HTTP transport (Default: `False`) |
| `fetch_dict` | `False` | Fetch result rows as dicts instead of tuples (Default: `False`) |
| `fetch_mapper` | `pyexasol.exasol_mapper` | Use custom mapper function to convert Exasol values into Python objects during fetching (Default: `None`) |
| `fetch_size_bytes` | `5 * 1024 * 1024` | Maximum size of data message for single fetch request in bytes (Default: 5Mb) |
| `lower_ident` | `False` | Automatically lowercase identifiers (table names, column names, etc.) returned from relevant functions (Default: `False`) |
| `quote_ident` | `False` | Add double quotes and escape identifiers passed to relevant functions (`export_*`, `import_*`, `ext.*`, etc.) (Default: `False`) |
| `json_lib` | `rapidjson` | Supported values: [`rapidjson`](https://github.com/python-rapidjson/python-rapidjson), [`ujson`](https://github.com/esnme/ultrajson), [`json`](https://docs.python.org/3/library/json.html) (Default: `json`) |
| `verbose_error` | `True` | Display additional information when error occurs (Default: `True`) |
| `debug` | `False` | Output debug information for client-server communication and connection attempts to STDERR |
| `debug_logdir` | `/tmp/` | Store debug information into files in `debug_logdir` instead of outputting it to STDERR |
| `udf_output_host` | `0.0.0.0` | Specific IPv4 address to bind TCP server for script output (Default: `0.0.0.0`) |
| `udf_output_port` | `5555` | Port to bind TCP server for script output (Default: random port) |
| `udf_output_dir` | `/tmp` | Path or path-like object pointing to directory for script output log files (Default: `tempfile.gettempdir()`) |
| `http_proxy` | `http://myproxy.com:3128` | HTTP proxy string in Linux [`http_proxy`](https://www.shellhacks.com/linux-proxy-server-settings-set-proxy-command-line/) format (Default: `None`) |
| `client_name` | `MyClient` | Custom name of client application displayed in Exasol sessions tables (Default: `PyEXASOL`) |
| `client_version` | `1.0.0` | Custom version of client application (Default: `pyexasol.__version__`) |

## connect_local_config()
Opens new connection and returns `ExaConnection` object. It uses local .ini file (usually `~/.pyexasol.ini`) to read credentials and connection parameters. Please read [local config](/docs/LOCAL_CONFIG.md) page for more details.

| Argument | Example | Description |
| --- | --- | --- |
| `config_section` | `my_exasol` | Name of section in config file |
| `config_path` | `/etc/pyexasol.ini` | Path to config file (Default: `~/.pyexasol.ini`) |
| `**kwargs` | - | All other arguments from [`connect`](#connect) method; `**kwargs` override values from config file |

## http_transport()
Opens new HTTP connection and returns `ExaHTTPTransportWrapper` object. This function is part of [parallel HTTP transport API](/docs/HTTP_TRANSPORT_PARALLEL.md).

| Argument | Example | Description |
| --- | --- | --- |
| `host` | `10.17.1.10` | IP address of one of Exasol nodes received from [`get_nodes()`](#get_nodes) |
| `port` | `8563` | Port of one of Exasol nodes received from [`get_nodes()`](#get_nodes) |
| `mode` | `pyexasol.HTTP_EXPORT` | Open connection for `pyexasol.HTTP_EXPORT` or `pyexasol.HTTP_IMPORT` |
| `compression` | `True` | Use zlib compression for HTTP transport, must be the same as `compression` of main connection (Default: `False`) |
| `encryption` | `True` | Use [SSL encryption](/docs/ENCRYPTION.md) for HTTP transport, must be the same as `encryption` of main connection (Default: `False`) |

Please note: this function was changed in PyEXASOL 0.5.1 and is no longer accepts `shard_id` and `dsn` arguments due to potential issues with reserve nodes and partially invalid DSN. You should now call `get_nodes()` in order to get list of real active Exasol nodes and pass this this information to child processes.

## ExaConnection

Object of this class holds connection to Exasol, performs client-server communication and manages fast HTTP transport. All dependent objects have back-reference to parent `ExaConnection` object (`self.connection`).

### execute()
Execute SQL statement with optional formatting.

| Argument | Example | Description |
| --- | --- | --- |
| `query` | `SELECT * FROM {table:i} WHERE col1={col1}` | SQL query text, possibly with placeholders |
| `query_params` | `{'table': 'users', 'col1':'bar'}` | (optional) Values for placeholders |

Returns instance of `ExaStatement`

### execute_udf_output()
Execute SQL statement with optional formatting. Capture [output](/docs/SCRIPT_OUTPUT.md) of UDF scripts.

| Argument | Example | Description |
| --- | --- | --- |
| `query` | `SELECT * FROM {table:i} WHERE col1={col1}` | SQL query text, possibly with placeholders |
| `query_params` | `{'table': 'users', 'col1':'bar'}` | (optional) Values for placeholders |

Returns tuple with two elements: (1) instance of `ExaStatement` and (2) list of `Path` objects for script output log files.

Exasol should be able to open connection to the host where current script is running. It is usually OK in the same data centre, but it is normally not working if you try to run this function on local laptop.

### commit()
Wrapper for query `COMMIT`

### rollback()
Wrapper for query `ROLLBACK`

### set_autocommit()
Set `False` to execute following statements in transaction. Set `True` to get back to automatic COMMIT after each statement.

Autocommit is `True` by default because Exasol has to commit indexes and statistics objects even for pure SELECT statements. Lack of default COMMIT may lead to serious performance degradation.

| Argument | Example | Description |
| --- | --- | --- |
| `val` | `False` | Autocommit mode |

### set_query_timeout()
Set the maximum time in seconds for which a query can run before Exasol kills it automatically. Set value `0` to disable timeout.

It is highly recommended to set timeout for UDF scripts to avoid potential infinite loops and very long transactions.

| Argument | Example | Description |
| --- | --- | --- |
| `val` | `10` | Timeout value in seconds |

### open_schema()
Wrapper for `OPEN SCHEMA`

| Argument | Example | Description |
| --- | --- | --- |
| `schema` | `ingres` | Schema name |

### current_schema()
Returns name of currently opened schema. Returns empty string if no schema was opened.

### export_to_file()
Exports big amount of data from Exasol to file or file-like object using fast HTTP transport.
File must be opened in binary mode.

| Argument | Example | Description |
| --- | --- | --- |
| `dst` | `open(my_file, 'wb')` `/tmp/file.csv` | Path to file or file-like object |
| `query_or_table` | `SELECT * FROM table` `table` `(schema, table)` | SQL query or table for export |
| `query_params` | `{'table': 'users', 'col1':'bar'}` | (optional) Values for SQL query placeholders |
| `export_params` | `{'with_column_names': True}` | (optional) Custom parameters for EXPORT query |

### export_to_list()
Exports big amount of data from Exasol to basic Python `list` using fast HTTP transport. This function may run out of memory.

| Argument | Example | Description |
| --- | --- | --- |
| `query_or_table` | `SELECT * FROM table` `table` `(schema, table)` | SQL query or table for export |
| `query_params` | `{'table': 'users', 'col1':'bar'}` | (optional) Values for SQL query placeholders |
| `export_params` | `{'encoding': 'LATIN1'}` | (optional) Custom parameters for EXPORT query |

Returns `list` of `tuples`

### export_to_pandas()
Exports big amount of data from Exasol to `pandas.DataFrame`. This function may run out of memory.

| Argument | Example | Description |
| --- | --- | --- |
| `query_or_table` | `SELECT * FROM table` `table` `(schema, table)` | SQL query or table for export |
| `query_params` | `{'table': 'users', 'col1':'bar'}` | (optional) Values for SQL query placeholders |
| `export_params` | `{'encoding': 'LATIN1'}` | (optional) Custom parameters for EXPORT query |

Returns instance of `pandas.DataFrame`

### export_to_callback()
Exports big amount of data to user-defined callback function

| Argument | Example | Description |
| --- | --- | --- |
| `callback` | `def my_callback(pipe, dst, **kwargs)` | Callback function |
| `dst` | `anything` | (optional) Export destination for callback function |
| `query_or_table` | `SELECT * FROM table` `table` `(schema, table)` | SQL query or table for export |
| `query_params` | `{'table': 'users', 'col1':'bar'}` | (optional) Values for SQL query placeholders |
| `export_params` | `{'with_column_names': True}` | (optional) Custom parameters for EXPORT query |

Returns result of callback function

### export_parallel()
This function is part of [parallel HTTP transport API](/docs/HTTP_TRANSPORT_PARALLEL.md). It accepts list of proxy `host:port` strings obtained from all child processes and executes parallel export query. Parent process only monitors the execution of query. All actual work is performed in child processes.

| Argument | Example | Description |
| --- | --- | --- |
| `exa_proxy_list` | `['27.0.1.10:5362', '27.0.1.11:7262']` | List of proxy `host:port` strings |
| `query_or_table` | `SELECT * FROM table` `table` `(schema, table)` | SQL query or table for export |
| `query_params` | `{'table': 'users', 'col1':'bar'}` | (optional) Values for SQL query placeholders |
| `export_params` | `{'with_column_names': True}` | (optional) Custom parameters for EXPORT query |

Returns nothing on successful export. You may access `EXPORT` statement results using [`last_statement()`](#last_statement) function.

### import_from_file()
Imports big amount of data from file or file-like object to Exasol. File must be opened in binary mode.

| Argument | Example | Description |
| --- | --- | --- |
| `src` | `open(my_file, 'rb')` `/tmp/my_file.csv` | Source file or file-like object |
| `table` | `my_table` `(my_schema, my_table)` | Destination table for IMPORT |
| `import_params` | `{'column_separator: ','}` | (optional) Custom parameters for IMPORT query |

### import_from_iterable()
Imports big amount of data from `iterable` Python object to Exasol. Iterator must return tuples of values.

| Argument | Example | Description |
| --- | --- | --- |
| `src` | `[(123, 'a')]` | Source object implementing `__iter__` |
| `table` | `my_table` `(my_schema, my_table)` | Destination table for IMPORT |
| `import_params` | `{'column_separator: ','}` | (optional) Custom parameters for IMPORT query |

### import_from_pandas()
Imports big amount of data from `pandas.DataFrame` to Exasol.

| Argument | Example | Description |
| --- | --- | --- |
| `src` | `[(123, 'a')]` | Source `pandas.DataFrame` instance |
| `table` | `my_table` `(my_schema, my_table)` | Destination table for IMPORT |
| `import_params` | `{'column_separator: ','}` | (optional) Custom parameters for IMPORT query |

### import_from_callback()
Imports big amount of data from user-defined callback function to Exasol.

| Argument | Example | Description |
| --- | --- | --- |
| `callback` | `def my_callback(pipe, src, **kwargs)` | Callback function |
| `src` | `anything` | Source for callback function |
| `table` | `my_table` `(my_schema, my_table)` | Destination table for IMPORT |
| `callback_params` | `{'a': 'b'}` | (optional) Dict with additional parameters for callback function |
| `import_params` | `{'column_separator': ','}` | (optional) Custom parameters for IMPORT query |

### import_parallel()
This function is part of [parallel HTTP transport API](/docs/HTTP_TRANSPORT_PARALLEL.md). It accepts list of proxy `host:port` strings obtained from all child processes and executes parallel import query. Parent process only monitors the execution of query. All actual work is performed in child processes.

| Argument | Example | Description |
| --- | --- | --- |
| `exa_proxy_list` | `['27.0.1.10:5362', '27.0.1.11:7262']` | List of proxy `host:port` strings |
| `table` | `table` `(schema, table)` | Destination table for import |
| `import_params` | `{'column_separator': ','}` | (optional) Custom parameters for IMPORT query |

Returns nothing on successful export. You may access `IMPORT` statement results using [`last_statement()`](#last_statement) function.

### get_nodes()

Returns list of currently active Exasol nodes which is normally used for [parallel HTTP transport](/docs/HTTP_TRANSPORT_PARALLEL.md).

| Argument | Example | Description |
| --- | --- | --- |
| `pool_size` | `10` | (optional) Returns list of specific size |

Result format: `[{'host': <ip_address>, 'port': <port>, 'idx': <incremental index of returned node>}]`

If `pool_size` is bigger than number of nodes, list will wrap around and nodes will repeat with different `idx`. If `pool_size` is omitted, returns every active node once.

Exasol shuffles list for every connection.

### session_id()
Returns `SESSION_ID` of current session.

### last_statement()
Get last `ExaStatement` object. May be useful while working with `export_*` and `import_*` functions normally returning result of callback function instead of statement object.

Returns instance of `ExaStatement`.

### abort_query()
Abort running query.

This function should be called from a separate thread and has no response. Response should be checked in the main thread which started execution of query. Please check [27_abort_query](/examples/27_abort_query.py) example.

There are three possible outcomes of calling this function:

1) Query is aborted normally, connection remains active;
2) Query was stuck in a state which cannot be aborted, so Exasol has to terminate connection;
3) Query might be finished successfully before abort call had a chance to take effect;

Please note that you may terminate the whole Python process to close WebSocket connection. It will stop running query automatically.

### ExaConnection.close()
Closes connection to database.


## ExaStatement

Object of this class executes and helps to fetch result set of single Exasol SQL statement. Unlike typical `Cursor` object, `ExaStatement` is not reusable.

`ExaStatement` may fetch result set rows as `tuples` (default) or as `dict` (set `fetch_dict=True` in connection options).

`ExaStatement` may use custom data-type mapper during fetching (set `fetch_mapper=<func>` in connection options). Mapper function accepts two arguments (raw `value` and `dataType` object) and returns custom object or value.

`ExaStatement` fetches big result sets in chunks. The size of chunk may be adjusted (set `fetch_size_bytes=<int>` in connection options).

### \_\_iter\_\_()
The best way to fetch result set of statement is to use iterator:

```python
st = pyexasol.execute('SELECT * FROM table')

for row in st:
    print(row)
```

Iterator yields `tuple` or `dict` depending on `fetch_dict` connection option.

### fetchone()
Fetches one row.

Returns `tuple` or `dict`. Returns `None` if all rows were fetched.

### fetchmany()
Fetches multiple rows.

| Argument | Example | Description |
| --- | --- | --- |
| `size` | `100` | Set the specific amount of rows to fetch (Default: `10000`) |

Returns `list` of `tuples` or `list` of `dict`. Returns empty `list` if all rows were fetched.

### fetchall()
Fetches all remaining rows. This function may run out of memory.

Returns `list` of `tuples` or `list` of `dict`. Returns empty `list` if all rows were fetched.

### fetchcol()
Fetches all values from first column.

Returns `list` of values. Returns empty `list` if all rows were fetched.

### fetchval()
Fetches first column of first row. It may be useful for queries returning single value like `SELECT count(*) FROM table`.

Returns value. Returns `None` if all rows were fetched.

### rowcount()
- Returns total amount of selected rows for statements with result set (`num_rows`).
- Returns total amount of processed rows for DML queries (`row_count`).

### columns()
Returns `dict` with keys as `column names` and values as `dataType` objects defined in Exasol WebSocket protocol.

| Names | Type | Description |
| --- | --- | --- |
| type | string | column data type |
| precision | number | (optional) column precision |
| scale | number | (optional) column scale |
| size | number | (optional) maximum size in bytes of a column value |
| characterSet | string | (optional) character encoding of a text column |
| withLocalTimeZone | true, false | (optional) specifies if a timestamp has a local time zone |
| fraction | number | (optional) fractional part of number |
| srid | number | (optional) spatial reference system identifier |

Since the minimum supported version of Python is 3.6, the order of `dict` preserves the order of columns in result set.

### column_names()

Returns `list` of column names.

### ExaStatement.close()
Closes result set handle if it was opened. You won't be able to fetch next chunk of large dataset after calling this function, but no other side-effects.

## ExaFormatter

`ExaFormatter` inherits standard Python `string.Formatter`. It introduces set of placeholders to prevent SQL injections specifically in Exasol dynamic SQL queries. It also completely disabled `format_spec` section of standard formatting since it has no use in context of SQL queries and may cause more harm than good.

### format()
Formats SQL query using given arguments. Definition is the same as standard `format` function.

### escape()
Accepts raw value. Converts it to `str` and replaces `'` (single-quote) with `''` (two single-quotes). May be useful on its own when escaping small parts of bigger values.

### escape_ident()
Accepts raw identifier. Converts it to `str` and replaces `"` (double-quote) with `""` (two double-quotes). May be useful on its own when escaping small parts of big identifiers.

### escape_like()
Accepts raw value. Converts it to `str` and escapes for LIKE pattern value.

### quote()
Accepts raw value. Converts it to `str`, escapes it using `escape()` and wraps in `'` (single-quote). This is the primary function to pass arbitrary values to Exasol queries.

### quote_ident()
Accepts raw identifier. Coverts it to `str`, escapes it using `escape_ident()` and wraps in `"` (double-quote). This is the primary function to pass arbitrary identifiers to Exasol queries.

Also, accepts tuple of raw identifiers, applies `quote_ident` to all of them and joins with `.` (dot). It may be useful when referencing to `(schema, table)` or `(schema, table, column_name)`.

Please note that identifiers in Exasol are upper-cased by default. If you pass lower-cased identifier into this function, Exasol will try to find object with lower-cased name and may fail. Please consider using `safe_ident()` function if want more convenience.

### safe_ident()
Accepts raw identifier. Converts it to `str` and validates it. Then puts it into SQL query without any quotting. If passed values is not a valid identifier (e.g. contains spaces), throws `ValueError` exception.

Also, accepts tuple of raw identifiers, validates all of them and joins with `.` (dot).

It is the convenient version of `quote_ident` with softer approach to lower-cased identifiers.

### safe_float()
Accepts raw value. Converts it to `str` and validates it as float value for Exasol. For example `+infinity`, `-infinity` are not valid Exasol values. If value is not valid, throws `ValueError` exception.

### safe_decimal()
Accepts raw values. Converts it to `str` and validates it as decimal valie for Exasol. If value is not valid, throws `ValueError` exception.

## ExaHTTPTransportWrapper

Wrapper for [parallel HTTP transport](/docs/HTTP_TRANSPORT_PARALLEL.md) used by child processes.

### ExaHTTPTransportWrapper.get_proxy()

Returns proxy `host:port` string. Those strings should be passed from child processes to parent process and used as argument for [`export_parallel()`](#export_parallel) and [`import_parallel()`](#import_parallel) functions.

### ExaHTTPTransportWrapper.export_to_callback()

Exports chunk of data using callback function. You may use exactly the same callbacks utilized by standard non-parallel [`export_to_callback()`](#export_to_callback) function.

| Argument | Example | Description |
| --- | --- | --- |
| `callback` | `def my_callback(pipe, dst, **kwargs)` | Callback function |
| `dst` | `anything` | (optional) Export destination for callback function |
| `callback_params` | `{'a': 'b'}` | (optional) Dict with additional parameters for callback function |

Returns result of callback function

### ExaHTTPTransportWrapper.import_from_callback()

Import chunk of data using callback function. You may use exactly the same callbacks utilized by standard non-parallel [`import_from_callback()`](#import_from_callback) function.

| Argument | Example | Description |
| --- | --- | --- |
| `callback` | `def my_callback(pipe, dst, **kwargs)` | Callback function |
| `src` | `anything` | (optional) Import source for callback function |
| `callback_params` | `{'a': 'b'}` | (optional) Dict with additional parameters for callback function |

Returns result of callback function

## ExaExtension

This class provides additional capabilities to solve common Exasol-related problems which are normally out of scope of simple SQL driver. You should call `ExaConnection.ext` property in order to use those functions.

For example:
```python
C.ext.get_columns('my_table')
```

### get_columns()

Returns structure of table or view using WebSocket response format. Output is very similar to [ExaStatement.columns()](#columns).

| Argument | Example | Description |
| --- | --- | --- |
| `object_name` | `my_table` `(my_schema, my_table)` | Object name or tuple |

### get_columns_sql()

Returns structure of SQL query result without executing it. Output is very similar to [ExaStatement.columns()](#columns).

| Argument | Example | Description |
| --- | --- | --- |
| `query` | `SELECT * FROM {table:i} WHERE col1={col1}` | SQL query text, possibly with placeholders |
| `query_params` | `{'table': 'users', 'col1':'bar'}` | (optional) Values for placeholders |

### get_sys_columns()

Returns info about columns using Exasol system views. This is different from WebSocket response format.

| Argument | Example | Description |
| --- | --- | --- |
| `object_name` | `my_table` `(my_schema, my_table)` | Object name or tuple |

### get_sys_tables()

Returns info about tables using Exasol system views.

| Argument | Example | Description |
| --- | --- | --- |
| `schema` | `my_schema` | (optional) schema name (Default: current schema) |
| `table_name_prefix` | `DIM_` | (optional) filter tables by name prefix (Default: all tables in schema) |

### get_sys_views()

Returns info about views using Exasol system views.

| Argument | Example | Description |
| --- | --- | --- |
| `schema` | `my_schema` | (optional) schema name (Default: current schema) |
| `view_name_prefix` | `DIM_` | (optional) filter views by name prefix (Default: all views in schema) |

### get_sys_schemas()

Returns info about schemas using Exasol system views.

| Argument | Example | Description |
| --- | --- | --- |
| `schema_name_prefix` | `MUR_` | (optional) filter schemas by name prefix (Default: all schemas) |

### get_reserved_words()

Returns list of Exasol reserved words. Those words cannot be used as identifiers without quotes. It is a bad practice to hard-code such words in SQL driver code. This is list is constantly updated by Exasol during version upgrades, and it's a good idea to always fetch it for best possible results.

### get_disk_space_usage()

There is no easy-to-use Exasol function to get current disk usage. PyEXASOL tries to mitigate it and estimate this value using hidden system view. We take redundancy and free disk space into account.

This function returns dictionary with 4 keys:

| Key | Description |
| --- | --- |
| `occupied_size` | How much space is occupied (in bytes) |
| `free_size` | How much space is available (in bytes) |
| `total_size` | occupied_size + free_size |
| `occupied_size_percent` | Percentage of occupied disk space (0-100%) |

### explain_last()

Profile (EXPLAIN) last executed query. Example: [22_profiling](/examples/22_profiling.py)

| Argument | Example | Description |
| --- | --- | --- |
| `details` | `True` | (optional) return additional details |

- `details=False` returns AVG or MAX values for all Exasol nodes.
- `details=True` returns separate rows for each individual Exasol node (column `iproc`).

Details are useful to detect bad data distribution and imbalanced execution across multiple nodes.

`COMMIT`, `ROLLBACK` and `FLUSH STATISTICS` queries are ignored.

If you want to see real values of CPU, MEM, HDD, NET columns, please enable Exasol profiling first with: `ALTER SESSION SET PROFILE = 'ON';`

Please refer to Exasol User Manuals for explanations about profiling columns.
