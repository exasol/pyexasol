# Reference

- [connect()](#connect)
- [ExaConnection](#exaconnection)
  - [execute()](#execute)
  - [commit()](#commit)
  - [rollback()](#rollback)
  - [set_autocommit()](#set_autocommit)
  - [open_schema()](#open_schema)
  - [current_schema()](#current_schema)
  - [export_to_file()](#export_to_file)
  - [export_to_list()](#export_to_list)
  - [export_to_pandas()](#export_to_pandas)
  - [export_to_callback()](#export_to_callback)
  - [import_from_file()](#import_from_file)
  - [import_from_iterable()](#import_from_iterable)
  - [import_from_pandas()](#import_from_pandas)
  - [import_from_callback()](#import_from_callback)
  - [session_id()](#session_id)
  - [last_statement()](#last_statement)
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

## connect()
Opens new connection and returns `ExaConnection` object.

| Argument | Example | Description |
| --- | --- | --- |
| `dsn` | `exasolpool1..5.mlan:8563` `10.10.127.1..11:8564` | Connection string, same format as standard JDBC / ODBC drivers |
| `user` | `sys` | Username |
| `password` | `password` | Password |
| `schema` | `ingres` | Open schema after connection (Default: `''`, no schema) |
| `autocommit` | `True` | Autocommit mode after connection (Default: `True`) |
| `socket_timeout` | `10` | Socket timeout in seconds passed directly to websocket (Default: `10`) |
| `query_timeout` | `0` | Maximum execution time of queries before automatic abort (Default: `0`, no timeout) |
| `compression` | `True` | Use zlib compression both for WebSocket and HTTP transport (Default: `False`) |
| `fetch_dict` | `False` | Fetch result rows as dicts instead of tuples (Default: `False`) |
| `fetch_mapper` | `pyexasol.exasol_mapper` | Use custom mapper function to convert Exasol values into Python objects during fetching (Default: `None`) |
| `fetch_size_bytes` | `5 * 1024 * 1024` | Maximum size of data message for single fetch request in bytes (Default: 5Mb) |
| `lower_ident` | `False` | Automatically lowercase all identifiers (table names, column names, etc.) returned from relevant functions (Default: `False`) |
| `cls_connection` | `pyexasol.ExaConnection` | Overloaded `ExaConnection` class |
| `cls_statement` | `pyexasol.ExaStatement` | Overloaded `ExaStatement` class |
| `cls_formatter` | `pyexasol.ExaFormatter` | Overloaded `ExaFormatter` class |
| `cls_logger` | `pyexasol.ExaLogger` | Overloaded `ExaLogger` class |
| `cls_extension` | `pyexasol.ExaExtension` | Overloaded `ExaExtension` class |
| `json_lib` | `rapidjson` | Supported values: [`rapidjson`](https://github.com/python-rapidjson/python-rapidjson), [`ujson`](https://github.com/esnme/ultrajson), [`json`](https://docs.python.org/3/library/json.html) (Default: `json`) |
| `verbose_error` | `True` | Display additional information when error occurs (Default: `True`) |
| `debug` | `False` | Output debug information for client-server communication and connection attempts to STDERR |
| `debug_logdir` | `/tmp/` | Store debug information into files in `debug_logdir` instead of outputting it to STDERR |

## ExaConnection

Object of this class holds connection to Exasol, performs client-server communication and manages fast HTTP transport. All dependent objects have back-reference to parent `ExaConnection` object (`self.connection`).

### execute()
Create SQL statement, optionally format and execute it.

| Argument | Example | Description |
| --- | --- | --- |
| `query` | `SELECT * FROM {table:i} WHERE col1={col1}` | SQL query text, possibly with placeholders |
| `query_params` | `{'table': 'users', 'col1':'bar'}` | (optional) Values for placeholders |

Returns instance of `ExaStatement`

### commit()
Wrapper for database `COMMIT`

### rollback()
Wrapper for database `ROLLBACK`

### set_autocommit()
Set `autocommit=False` to start working with transactions and sub-connections. Set `autocommit=True` to get back to default mode. Autocommit is `True` by default because Exasol has to commit indexes and statistics objects even for pure SELECT statements. Unustified lack of commit may lead to serious performance degradation.

| Argument | Example | Description |
| --- | --- | --- |
| `autocommit` | `False` | Autocommit mode |

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

Returns `list` of `tuples`

### export_to_pandas()
Exports big amount of data from Exasol to `pandas.DataFrame`. This function may run out of memory.

| Argument | Example | Description |
| --- | --- | --- |
| `query_or_table` | `SELECT * FROM table` `table` `(schema, table)` | SQL query or table for export |
| `query_params` | `{'table': 'users', 'col1':'bar'}` | (optional) Values for SQL query placeholders |

Returns instance of `pandas.DataFrame`

### export_to_callback()
Exports big amount of data to user-defined callback function

| Argument | Example | Description |
| --- | --- | --- |
| `callback` | `def my_callback(pipe, dst, **kwargs)` | Callback function |
| `dst` | `anything` | (optional) Export destination for callback function |
| `query_or_table` | `SELECT * FROM table` `table` `(schema, table)` | SQL query or table for export |
| `query_params` | `{'table': 'users', 'col1':'bar'}` | (optional) Values for SQL query placeholders |

Returns result of callback function

### import_from_file()
Imports big amount of data from file or file-like object to Exasol. File must be opened in binary mode.

| Argument | Example | Description |
| --- | --- | --- |
| `src` | `open(my_file, 'rb')` `/tmp/my_file.csv` | Source file or file-like object |
| `table` | `my_table` `(my_schema, my_table)` | Destionation table for IMPORT |
| `import_params` | `{'column_separator: ','}` | (optional) Custom parameters for IMPORT query |

### import_from_iterable()
Imports big amount of data from `iterable` Python object to Exasol. Iterator must return tuples of values.

| Argument | Example | Description |
| --- | --- | --- |
| `src` | `[(123, 'a')]` | Source object implementing `__iter__` |
| `table` | `my_table` `(my_schema, my_table)` | Destionation table for IMPORT |

### import_from_pandas()
Imports big amount of data from `pandas.DataFrame` to Exasol.

| Argument | Example | Description |
| --- | --- | --- |
| `src` | `[(123, 'a')]` | Source `pandas.DataFrame` instance |
| `table` | `my_table` `(my_schema, my_table)` | Destionation table for IMPORT |

### import_from_callback()
Imports big amount of data from user-defined callback function to Exasol.

| Argument | Example | Description |
| --- | --- | --- |
| `callback` | `def my_callback(pipe, src, **kwargs)` | Callback function |
| `src` | `anything` | Source for callback function |
| `table` | `my_table` `(my_schema, my_table)` | Destionation table for IMPORT |
| `callback_params` | `{'a': 'b'}` | (optional) Dict with additional parameters for callback function |
| `import_params` | `{'column_separator': ','}` | (optional) Custom parameters for IMPORT query |

### session_id()
Returns `SESSION_ID` of current session.

### last_statement()
Get last `ExaStatement` object. May be useful while working with `export_*` and `import_*` functions normally returning result of callback function instead of statement object.

Returns instance of `ExaStatement`

### ExaConnection.close()
Closes connection to database


## ExaStatement

Object of this class executes and helps to fetch result set of single Exasol SQL statement. Unlike typical `Cursor` object, `ExaStatement` is not reusable.

`ExaStatement` may fetch result set rows as `tuples` (default) or as `dict` (set `fetch_dict=True` in connection options).

`ExaStatement` may use custom data-type mapper during fetching (set `fetch_mapper=<func>` in connection options). Mapper function accepts two arguments (raw `value` and `dataType` object) and returns custom object or value.

`ExaStatement` fetches big result sets in chunks. The size of chunk may be adjusted (set `fetch_size_bytes=<int>` in connection options). Bigger chunks generally benefit from better compression and increase overall performance.

### \_\_iter\_\_()
The best way to fetch result set of statement is to use iterator:

```python
st = pyexasol.execute('SELECT * FROM table')

for row in st:
    print(row)
```

Iterator yields `tuple` or `dict` dependng on `fetch_dict` connection option.

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

`ExaFormatter` inherits standard Python `string.Formatter`. It itroduces set of placeholders to prevent SQL injections specifically in Exasol dynamic SQL queries. It also completely disabled `format_spec` section of standard formatting since it has no use in context of SQL queries and may cause more harm than good.

### format()
Formats SQL query using given arguments. Definition is the same as standard `format` function.

### escape()
Accepts raw value. Converts it to `str` and replaces `'` (single-quote) with `''` (two single-quotes). May be useful on it's own when escaping small parts of bigger values.

### escape_ident()
Accepts raw identifier. Converts it to `str` and replaces `"` (dobule-quote) with `""` (two double-quotes). May be useful on it's own when escaping small parts of big identifiers.

### escape_like()
Accepts raw value. Converts it to `str` and escapes for LIKE pattern value.

### quote()
Accepts raw value. Converts it to `str`, escapes it using `escape()` and wraps in `'` (single-quote). This is the primary function to pass arbitrary values to Exasol queries.

### quote_ident()
Accepts raw identifier. Coverts it to `str`, escapes it using `escape_ident()` and wraps in `"` (double-quote). This is the primary function to pass arbitrary identifiers to Exasol queries.

Also accepts tuple of raw identifiers, applies `quote_ident` to all of them and joins with `.` (dot). It may be useful when referencing to `(schema, table)` or `(schema, table, column_name)`.

Please note that identifiers in Exasol are upper-cased by default. If you pass lower-cased identifier into this function, Exasol will try to find object with lower-cased name and may fail. Please consider using `safe_ident()` function if want more convenience.

### safe_ident()
Accepts raw identifier. Converts it to `str` and validates it. Then puts it into SQL query without any quotting. If passed values is not a valid identifier (e.g. contains spaces), throws `ValueError` exception.

Also accepts tuple of raw identifiers, validates all of them and joins with `.` (dot).

It is the convenient version of `quote_ident` with softer approach to lower-cased identifiers.

### safe_float()
Accepts raw value. Converts it to `str` and validates it as float value for Exasol. For example `+infinity`, `-infinity` are not valid Exasol values. If value is not valid, throws `ValueError` exception.

### safe_decimal()
Accepts raw values. Converts it to `str` and validates it as decimal valie for Exasol. If value is not valid, throws `ValueError` exception.
