<h1 align="center">PyExasol</h1>
<p align="center">
<a href="https://github.com/exasol/pyexasol/actions/workflows/pr-merge.yml">
    <img src="https://github.com/exasol/pyexasol/actions/workflows/pr-merge.yml/badge.svg?branch=master" alt="Continuous Integration (master)">
</a>
<a href="https://anaconda.org/conda-forge/pyexasol">
    <img src="https://anaconda.org/conda-forge/pyexasol/badges/version.svg" alt="Anaconda">
</a>
<a href="https://pypi.org/project/pyexasol/">
    <img src="https://img.shields.io/pypi/v/pyexasol" alt="PyPi Package">
</a>
<a href="https://pypi.org/project/pyexasol/">
    <img src="https://img.shields.io/pypi/dm/pyexasol" alt="Downloads">
</a>
<a href="https://pypi.org/project/pyexasol/">
    <img src="https://img.shields.io/pypi/pyversions/pyexasol" alt="Supported Python Versions">
</a>
</p>

PyExasol is the officially supported Python connector for [Exasol](https://www.exasol.com). It helps to handle massive volumes of data commonly associated with this DBMS.

You may expect significant performance improvement over ODBC in a single process scenario involving pandas or polars.

PyExasol provides API to read & write multiple data streams in parallel using separate processes, which is necessary to fully utilize hardware and achieve linear scalability. With PyExasol you are no longer limited to a single CPU core.

---
* Documentation: [https://exasol.github.io/pyexasol/](https://exasol.github.io/pyexasol/index.html)
* Source Code: [https://github.com/exasol/pyexasol](https://github.com/exasol/pyexasol)
---

## PyExasol main concepts

- Based on [WebSocket protocol](https://github.com/exasol/websocket-api);
- Optimized for minimum overhead;
- Easy integration with pandas and polars via HTTP transport;
- Compression to reduce network bottleneck;


## System requirements

- Exasol >= 7.1
- Python >= 3.9


## Getting started

Install PyExasol:
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

You may set up `local config` to store your personal Exasol credentials and connection options:
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

## Maintained by
[Exasol](https://www.exasol.com) 2023 — Today 
