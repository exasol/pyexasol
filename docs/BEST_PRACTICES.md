# PyEXASOL best practices

This page explains how to use PyEXASOL with maximum efficiency.

## Enable compression for WiFi connections

Wireless network bandwidth is usually the main bottleneck for office laptops. `Compression` flag enables zlib compression both for common fetching and for fast [HTTP transport](/docs/HTTP_TRANSPORT.md). It may improve overall performance by factor 4-8x.

```python
C = pyexasol.connect(... , compression=True)
```

## Use HTTP transport for big volumes of data

It is okay to use common fetching for small data sets up to 1M of records.

For anything bigger than that you should always consider [HTTP transport](/docs/HTTP_TRANSPORT.md) (`export_*` and `import_*` functions). It prevents creation of intermediate Python objects and scales much better.

```python
pd = C.export_to_pandas('SELECT * FROM table')
C.export_to_file('my_file.csv', 'SELECT * FROM table')

C.import_from_pandas(pd, 'table')
C.import_from_file('my_file.csv', 'table')
```

## Prefer iterator syntax to fetch result sets

Iterator syntax is much shorter and easier to use. Also, there is no need to check for `None` or empty list `[]` to detect end of result set.

```python
stmt = C.execute('SELECT * FROM table')

for row in stmt:
    print(row)
```

## Avoid using INSERT prepared statement to import raw values in SQL

PyEXASOL supports INSERT prepared statements since version `0.9.1` via [`.ext.insert_multi()`](/docs/REFERENCE.md#insert_multi) function. It works for small data sets and may provide some performance benefits.

However, it is strongly advised to use more efficient `IMPORT` command and HTTP transport instead. It has some small initial overhead, but large data sets will be transferred and processed much faster. It is also more CPU and memory efficient.

You may use [`import_from_iterable()`](/docs/REFERENCE.md#import_from_iterable) to insert data from list of rows.

```python
data = [
    (1, 'John'),
    (2, 'Gill'),
    (3, 'Ben')
]

C.import_from_iterable(data, 'table')
```

Please note: if you want to INSERT single row only into Exasol, you're probably doing something wrong. It is advised to use row-based databases (MySQL, PostgreSQL, etc) to track status of ETL jobs, etc.

## Always specify full connection string for Exasol cluster

Unlike standard WebSocket Python driver, PyEXASOL supports full connection strings and node redundancy. For example, connection string `myexasol1..5:8563` will be expanded as:

```
myexasol1:8563
myexasol2:8563
myexasol3:8563
myexasol4:8563
myexasol5:8563
```

PyEXASOL tries to connect to random node from this list. If it fails, it tries to connect to another random node. The main benefits of this approach are:

- Multiple connections are evenly distributed across the whole cluster;
- If one or more nodes are down, but the cluster is still operational due to redundancy, users will be able to connect without any problems or random error messages;

## Consider faster JSON-parsing libraries

PyEXASOL defaults to standard [`json`](https://docs.python.org/3/library/json.html) library for best compatibility. It is sufficient for majority of use-cases. However, if you are unhappy with HTTP transport and you wish to load large amounts of data using standard fetching, we highly recommend trying faster JSON libraries.

#### json_lib=[`rapidjson`](https://github.com/python-rapidjson/python-rapidjson)
```
pip install pyexasol[rapidjson]
```
Rapidjson provides significant performance boost and it is well maintained by creators. PyEXASOL defaults to `number_mode=NM_NATIVE`. Exasol server wraps big decimals with quotes and returns as strings, so it should be a safe option.

#### json_lib=[`ujson`](https://github.com/esnme/ultrajson)
```
pip install pyexasol[ujson]
```
Ujson provides best performance in our internal tests, but it is abandoned by creators. Also, float values may lose precision with ujson.

You may try any other json library. All you need to do is to overload `_init_json()` method in `ExaConnection`.

## Use `ext` functions to get structure of tables

It is usually good idea to use [`get_columns`](/docs/REFERENCE.md#get_columns) and [`get_columns_sql`](/docs/REFERENCE.md#get_columns_sql) functions to get structure of tables instead of querying `EXA_ALL_COLUMNS` system view.

```python
cols = C.ext.get_columns('table')
cols = C.ext.get_columns_sql('SELECT a, b, c FROM table')
```

Query to system view might be blocked by transaction locks, but ext function calls are not affected by this problem.
