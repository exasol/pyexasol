PyEXASOL is native Python driver for [Exasol](https://www.exasol.com). It provides special features to handle massive volumes of data commonly associated with this database.

## Quick links
- [Installation](#installation)
- [Reference](/docs/REFERENCE.md)
- [Best practices](/docs/BEST_PRACTICES.md)
- [SQL formatting](/docs/SQL_FORMATTING.md)
- [HTTP Transport](/docs/HTTP_TRANSPORT.md)
- [DB-API 2.0 comaptibility](/docs/DBAPI_COMPAT.md)
- [Examples](/docs/EXAMPLES.md)

## PyEXASOL main concepts

- Based on [WebSocket client-server protocol](https://github.com/EXASOL/websocket-api/blob/master/WebsocketAPI.md);
- Optimized for minimum overhead;
- Easy integration with pandas via HTTP transport;
- Compression to reduce network bottleneck;


## System requirements

- Exasol >= 6
- Python >= 3.6
- websocket-client >= 0.47
- rsa


## Installation

```
pip install pyexasol
```

## Basic usage

```python
import pyexasol

C = pyexasol.connect(dsn='host:port', user='sys', password='exasol')

st = C.execute("SELECT * FROM schema.table LIMIT 10")
for row in st:
    print(row)
```

## Read data into Pandas

```python
import pyexasol

C = E.connect(dsn='host:port', user='sys', password='exasol', compression=True)

data_frame = C.export_to_pandas("SELECT * FROM users")
print(data_frame.head())
```

## Future plans
- Parallel IMPORT / EXPORT in separate processes
- Sub-connections
- UDF scripts

## Created by
Vitaly Markov, 2018

Enjoy!