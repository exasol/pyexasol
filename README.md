[![Build Status](https://travis-ci.org/badoo/pyexasol.svg?branch=master)](https://travis-ci.org/badoo/pyexasol)
[![PyPI](https://badge.fury.io/py/pyexasol.svg)](https://badge.fury.io/py/pyexasol)

PyEXASOL is a custom Python driver for [Exasol](https://www.exasol.com) created in [Badoo](https://badoo.com/team/). It helps us to handle massive volumes of data commonly associated with this database.

You may expect significant performance improvement over existing ODBC / JDBC solutions in single process scenario involving pandas. It is also possible to split data set across multiple processes and servers to achieve linear scalability. With PyEXASOL you are not limited by single CPU core.


## Quick links
- [Getting started](#getting-started)
- [Reference](/docs/REFERENCE.md)
- [Examples](/docs/EXAMPLES.md)
- [Best practices](/docs/BEST_PRACTICES.md)
- [Local config (.ini file)](/docs/LOCAL_CONFIG.md)
- [SQL formatting](/docs/SQL_FORMATTING.md)
- [HTTP Transport](/docs/HTTP_TRANSPORT.md)
- [HTTP Transport (multiprocessing)](/docs/HTTP_TRANSPORT_PARALLEL.md)
- [SSL encryption](/docs/ENCRYPTION.md)
- [WebSocket protocol versions](/docs/PROTOCOL_VERSION.md)
- [Performance tests](/docs/PERFORMANCE.md)
- [UDF scripts output](/docs/SCRIPT_OUTPUT.md)
- [DB-API 2.0 compatibility](/docs/DBAPI_COMPAT.md)
- [Optional dependencies](/docs/DEPENDENCIES.md)
- [Changelog](/CHANGELOG.md)


## PyEXASOL main concepts

- Based on [WebSocket protocol](https://github.com/EXASOL/websocket-api/blob/master/WebsocketAPI.md);
- Optimized for minimum overhead;
- Easy integration with pandas via HTTP transport;
- Compression to reduce network bottleneck;


## System requirements

- Exasol >= 6
- Python >= 3.6


## Getting started

Install PyEXASOL:
```
pip install pyexasol[pandas]
```

Run basic query:
```python
import pyexasol

C = pyexasol.connect(dsn='<host:port>', user='sys', password='exasol')

stmt = C.execute("SELECT * FROM EXA_ALL_USERS")

for row in stmt:
    print(row)
```

Load data into `pandas.DataFrame`:
```python
import pyexasol

C = pyexasol.connect(dsn='<host:port>', user='sys', password='exasol', compression=True)

df = C.export_to_pandas("SELECT * FROM EXA_ALL_USERS")
print(df.head())
```

You may set up [local config](/docs/LOCAL_CONFIG.md) to store your personal Exasol credentials and connection options:
```python
import pyexasol

C = pyexasol.connect_local_config('my_config')

stmt = C.execute("SELECT CURRENT_TIMESTAMP")
print(stmt.fetchone())
```


## Created by
Vitaly Markov, 2018

Enjoy!