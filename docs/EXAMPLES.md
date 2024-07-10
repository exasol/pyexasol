## Preparation
Basic preparation steps are required to see examples in action.

1. Install PyEXASOL with [optional dependencies](/docs/DEPENDENCIES.md).
2. Download [PyEXASOL source code](https://github.com/exasol/pyexasol/archive/master.zip) and unzip it.
3. Make sure Exasol is installed and dedicated schema for testing is created. You may use free [Exasol Community Edition](https://www.exasol.com/portal/display/DOWNLOAD/Free+Trial) for testing purposes.
4. Open `/examples/` directory and edit file `\_config.py`. Input your Exasol credentials.
5. Run script to prepare data set for testing:
```
python examples/a00_prepare.py
```

That's all. Now you may run examples in any order like common python scripts. E.g.:
```
python examples/a01_basic.py
```

### Examples of core functions

- [a01_basic.py](/examples/a01_basic.py) - minimal code to create connection and run query;
- [a02_fetch_tuple.py](/examples/a02_fetch_tuple.py) - all methods of fetching result set returning tuples;
- [a03_fetch_dict.py](/examples/a03_fetch_dict.py) - all methods of fetching result set returning dictionaries;
- [a04_fetch_mapper.py](/examples/a04_fetch_mapper.py) - apply custom data type mapper for fetching;
- [a05_formatting.py](/examples/a05_formatting.py) - SQL text [formatting](/docs/SQL_FORMATTING.md);
- [a06_transaction.py](/examples/a06_transaction.py) - transaction management, autocommit;
- [a07_exceptions.py](/examples/a07_exceptions.py) - error handling for basic SQL queries;
- [a08_ext.py](/examples/a08_ext.py) - extension functions to help with common problems outside of scope of database driver;
- [a09_abort_query.py](/examples/a09_abort_query.py) - abort running query from separate thread;
- [a10_context_manager.py](/examples/a10_context_manager.py) - use WITH clause for `ExaConnection` and `ExaStatement` objects;
- [a11_insert_multi](/examples/a11_insert_multi.py) - INSERT small number of rows using prepared statements instead of HTTP transport;
- [a12_meta](/examples/a12_meta.py) - lock-free meta data requests;
- [a13_meta_nosql](/examples/a13_meta_nosql.py) - no-SQL metadata commands introduces in Exasol v7.0+;

### Examples of HTTP transport

- [b01_pandas.py](/examples/b01_pandas.py) - IMPORT / EXPORT to and from `pandas.DataFrame`;
- [b02_import_export.py](/examples/b02_import_export.py) - other methods of IMPORT / EXPORT;
- [b03_parallel_export.py](/examples/b03_parallel_export.py) - multi-process HTTP transport for EXPORT;
- [b04_parallel_import.py](/examples/b04_parallel_import.py) - multi-process HTTP transport for IMPORT;
- [b05_parallel_export_import.py](/examples/b05_parallel_export_import.py) - multi-process HTTP transport for EXPORT followed by IMPORT;
- [b06_http_transport_errors](/examples/b06_http_transport_errors.py) - various ways to break HTTP transport and handle errors;
- [b07_polars.py](/examples/b07_polars.py) - IMPORT / EXPORT to and from `polars.DataFrame`;


## Examples of misc functions

- [c01_redundancy.py](/examples/c01_redundancy.py) - connection redundancy, handling of missing nodes;
- [c02_edge_case.py](/examples/c02_edge_case.py) - storing and fetching biggest and smallest values for data types available in Exasol;
- [c03_db2_compat.py](/examples/c03_db2_compat.py) - [DB-API 2.0 compatibility wrapper](/docs/DBAPI_COMPAT.md);
- [c04_encryption.py](/examples/c04_encryption.py) - SSL-encrypted WebSocket connection and HTTP transport;
- [c05_session_params.py](/examples/c05_session_params.py) - passing custom session parameters `client_name`, `client_version`, etc.;
- [c06_local_config.py](/examples/c06_local_config.py) - connect using local config file;
- [c07_profiling.py](/examples/c07_profiling.py) - last query profiling;
- [c08_snapshot_transactions.py](/examples/c08_snapshot_transactions.py) - snapshot transactions mode, which may help with metadata locking problems;
- [c09_script_output.py](/examples/c09_script_output.py) - run query with UDF script and capture output (may not work on local laptop);
- [c10_overload.py](/examples/c10_overload.py) - extend core PyEXASOL classes to add custom logic;
- [c11_quote_ident.py](/examples/c11_quote_ident.py) - enable quoted identifiers for `import_*`, `export_*` and other relevant functions;
- [c12_thread_safety.py](/examples/c12_thread_safety.py) - built-in protection from accessing connection object from multiple threads simultaneously;
- [c13_dsn_parsing.py](/examples/c13_dsn_parsing.py) - parsing of complex connection strings and catching relevant exceptions;
- [c14_http_proxy.py](/examples/c14_http_proxy.py) - connection via HTTP proxy;
- [c15_garbage_collection](/examples/c15_garbage_collection.py) - detect potential garbage collection problems due to cross-references;

## Examples of JSON libraries used for fetching

- [j01_rapidjson.py](/examples/j01_rapidjson.py) - JSON library `rapidjson`;
- [j02_ujson.py](/examples/j02_ujson.py) - JSON library `ujson`;
- [j03_orjson.py](/examples/j03_orjson.py) - JSON library `orjson`;
