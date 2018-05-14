## Preparation
Basic preparation steps are required to see examples in action.

1. Install PyEXASOL with [optional dependencies](/docs/DEPENDENCIES.md)
2. Download [PyEXASOL source code](https://github.com/badoo/pyexasol/archive/master.zip) and unzip it.
3. Make sure Exasol is installed and dedicated schema for testing is created. You may use free [Exasol Community Edition](https://www.exasol.com/portal/display/DOWNLOAD/Free+Trial) for testing purposes.
4. Go to "/examples/" directory and edit **\_config.py**. Input your Exasol credentials.
5. Run script to prepare data set for testing:
```
python examples/00_prepare.py
```

That's all. Now you may run examples in any order like common python scripts. E.g.:
```
python examples/01_basic.py
```

## Examples

- [01_basic.py](/examples/01_basic.py) - minimal code to create connection and run query;
- [02_fetch_tuple.py](/examples/02_fetch_tuple.py) - all methods of fetching result set returning tuples;
- [03_fetch_dict.py](/examples/03_fetch_dict.py) - all methods of fetching result set returning dictionaries;
- [04_fetch_mapper.py](/examples/04_fetch_mapper.py) - adding custom data type mapper for fetching;
- [05_formatting.py](/examples/05_formatting.py) - SQL text [formatting](/docs/SQL_FORMATTING.md);
- [06_pandas.py](/examples/06_pandas.py) - IMPORT / EXPORT to and from `pandas.DataFrame`;
- [07_import_export.py](/examples/07_import_export.py) - other methods of IMPORT / EXPORT;
- [08_transaction.py](/examples/08_transaction.py) - transaction management, autocommit;
- [09_exceptions.py](/examples/09_exceptions.py) - error handling, common errors;
- [10_redundancy.py](/examples/10_redundancy.py) - connection redundancy, how driver handles missing nodes;
- [11_edge_case.py](/examples/11_edge_case.py) - storing and fetching biggest and smallest values for data types available in Exasol;
- [12_db2_compat.py](/examples/12_db2_compat.py) - [DB-API 2.0 compatibility wrapper](/docs/DBAPI_COMPAT.md);
- [13_ext.py](/examples/13_ext.py) - extension functions to help with common Exasol-related problems outside of pure database driver scope;
- [14_parallel_export.py](/examples/14_parallel_export.py) - multi-process HTTP transport for export;
- [15_encryption.py](/examples/15_encryption.py) - SSL-encrypted WebSocket connection and HTTP transport;
