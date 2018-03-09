## Preparation
Basic preparation steps are required to see examples in action.

1. [Install PyEXASOL](/README.md#installation)
2. Download [PyEXASOL source code](https://github.com/badoo/pyexasol/archive/master.zip) and unzip it.
3. Make sure Exasol is installed and dedicated schema for testing is created. You may use free [Exasol Community Edition](https://www.exasol.com/portal/display/DOWNLOAD/Free+Trial) for testing purposes.
4. Go to "/examples/" directory and edit **\_config.py**. Input your Exasol credentials.
5. Run script to prepare data set for testing:
```
python examples/0_prepare.py
```

That's all. Now you may run examples in any order like common python scripts. E.g.:
```
python examples/1_basic.py
```

## Examples

- [1_basic.py](/examples/1_basic.py) - minimal code to create connection and run query;
- [2_fetch_tuple.py](/examples/2_fetch_tuple.py) - all methods of fetching result set returing tuples;
- [3_fetch_dict.py](/examples/3_fetch_dict.py) - all methods of fetching result set returing dictionaries;
- [4_fetch_mapper.py](/examples/4_fetch_mapper.py) - adding custom data type mapper for fetching;
- [5_formatting.py](/examples/5_formatting.py) - SQL text [formatting](/docs/SQL_FORMATTING.md);
- [6_pandas.py](/examples/6_pandas.py) - IMPORT / EXPORT to and from `pandas.DataFrame`;
- [7_import_export.py](/examples/7_import_export.py) - other methods of IMPORT / EXPORT;
- [8_transaction.py](/examples/8_transaction.py) - transaction management, autocommit;
- [9_exceptions.py](/examples/9_exceptions.py) - error handling, common errors;
- [10_redundancy.py](/examples/10_redundancy.py) - connection redundancy, how driver handles missing nodes;
- [11_edge_case.py](/examples/11_edge_case.py) - storing and fetching biggest and smallest values for data types available in Exasol;
- [12_db2_compat.py](/examples/12_db2_compat.py) - [DB-API 2.0 compatibility wrapper](/docs/DBAPI_COMPAT.md)