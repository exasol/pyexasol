[![Examples](https://github.com/exasol/pyexasol/actions/workflows/examples.yml/badge.svg)](https://github.com/exasol/pyexasol/actions/workflows/examples.yml)
[![PyPI](https://badge.fury.io/py/pyexasol.svg)](https://badge.fury.io/py/pyexasol)
[![Anaconda](https://anaconda.org/conda-forge/pyexasol/badges/version.svg)](https://anaconda.org/conda-forge/pyexasol)

PyEXASOL is the officially supported Python connector for [Exasol](https://www.exasol.com). It helps to handle massive volumes of data commonly associated with this DBMS.

You may expect significant performance improvement over ODBC in a single process scenario involving pandas.

PyEXASOL provides API to read & write multiple data streams in parallel using separate processes, which is necessary to fully utilize hardware and achieve linear scalability. With PyEXASOL you are no longer limited to a single CPU core.


## Quick links
- [Getting started](#getting-started)
- [Reference](/docs/REFERENCE.md)
- [Examples](/docs/EXAMPLES.md)
- [Best practices](/docs/BEST_PRACTICES.md)
- [Local config (.ini file)](/docs/LOCAL_CONFIG.md)
- [SQL formatting](/docs/SQL_FORMATTING.md)
- [HTTP Transport](/docs/HTTP_TRANSPORT.md)
- [HTTP Transport (multiprocessing)](/docs/HTTP_TRANSPORT_PARALLEL.md)
- [Parallelism](/docs/PARALLELISM.md)
- [SSL encryption](/docs/ENCRYPTION.md)
- [WebSocket protocol versions](/docs/PROTOCOL_VERSION.md)
- [Performance tests](/docs/PERFORMANCE.md)
- [UDF scripts output](/docs/SCRIPT_OUTPUT.md)
- [DB-API 2.0 compatibility](/docs/DBAPI_COMPAT.md)
- [Optional dependencies](/docs/DEPENDENCIES.md)
- [Changelog](/CHANGELOG.md)


## PyEXASOL main concepts

- Based on [WebSocket protocol](https://github.com/exasol/websocket-api);
- Optimized for minimum overhead;
- Easy integration with pandas via HTTP transport;
- Compression to reduce network bottleneck;


## System requirements

- Exasol >= 6.2
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

Connect to Exasol SAAS using OpenID token for authentication:

```python
import pyexasol

C = pyexasol.connect(dsn='<host:port>', user='sys', refresh_token='<token>')

stmt = C.execute("SELECT * FROM EXA_ALL_USERS")

for row in stmt:
    print(row)
```

## Created by
[Vitaly Markov](https://www.linkedin.com/in/markov-vitaly/), 2018 — 2022

Enjoy!
