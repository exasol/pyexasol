## DB-API 2.0 compatibility

PyEXASOL [public interface](/docs/REFERENCE.md) is similar to [PEP-249 DB-API 2.0](https://www.python.org/dev/peps/pep-0249/) specification, but it does not strictly follow it. This page explains the reasons behind this decision and your alternative(s) if you need or want to use a dbabpi2 compatible driver..

### Alternatives

#### Exasol WebSocket Driver
The `pyexasol` package includes a DBAPI2 compatible driver facade, located in the `exasol.driver` package. However, using pyexasol directly will generally yield better performance when utilizing Exasol in an OLAP manner, which is likely the typical use case. 

That said, in specific scenarios, the DBAPI2 API can be advantageous or even necessary. This is particularly true when integrating with "DB-Agnostic" frameworks. In such cases, you can just import and use the DBAPI2 compliant facade as illustrated in the example below.

```python
from exasol.driver.websocket.dbapi2 import connect

connection = connect(dsn='', username='sys', password='exasol', schema='TEST')

with connection.cursor() as cursor:
    cursor.execute("SELECT 1;")
```

#### TurboODBC
[TurboODBC](https://github.com/blue-yonder/turbodbc) offers an alternative ODBC-based, DBAPI2-compatible driver, which supports the Exasol database.

### Rationale

PEP-249 was originally created for general purpose OLTP row store databases running on single server: SQLite, MySQL, PostgreSQL, MSSQL, Oracle, etc.

It does not work very well for OLAP columnar databases (like Exasol) running on multiple servers because it was never designed for this purpose. Despite both OLTP DBMS and OLAP DBMS use SQL for communication, the foundation and usage patterns are completely different.

When people use DB-API 2.0 drivers, they tend to skip manuals and automatically apply OLTP usage patterns without even realizing how much they lose in terms of performance and efficiency.

The good example is [TurbODBC](https://github.com/blue-yonder/turbodbc). Very few know that it is possible to fetch data as [NumPy arrays](https://turbodbc.readthedocs.io/en/latest/pages/advanced_usage.html#numpy-support) and as [Apache Arrow](https://turbodbc.readthedocs.io/en/latest/pages/advanced_usage.html#apache-arrow-support).

Minor intentional incompatibilities with DB-API 2.0 force users to look through manual and to learn about [better ways](/docs/BEST_PRACTICES.md) of getting the job done.

### Exasol specific problems with DB-API 2.0

- Default `autocommit=off` prevents indexes from being stored permanently on disk for `SELECT` statements;
- Default `autocommit=off` may hold transaction for a long time (e.g. opened connection in IPython notebook);
- Python object creation and destruction overhead is very significant when you process large amounts of data;
- Functions `fetchmany()` and `executemany()` has significant additional overhead related to JSON serialisation;
- Exasol WebSocket protocol provides more information about columns than normally available in `.description` property of `cursor`;

We also wanted to discourage:
- "Drop-in" replacements of other Exasol drivers without reading manual;
- Usage of OLTP-oriented ORM (e.g. SQLAlchemy, Django);

Unlike common OLTP databases, each OLAP database is very unique. It is important to understand implementation details and features of specific database and to build application around those features. Generalisation of any kind and "copy-paste" approach may lead to abysmal performance in trivial cases.

## Ideas for migration

Find `cursor()` calls:
```python
cur = C.cursor()
cur.execute('SELECT * FROM table')
data = cur.fetchall()

```
Replace with:
```python
st = C.execute('SELECT * FROM table`)
data = st.fetchall()
```
---

Find `.description`
```python
columns = list(map(str.lower, next(zip(*cur.description))))
```
Replace with:
```python
columns = st.column_names()
```
---

Find all reads into pandas:
```python
cur.execute('SELECT * FROM table')
pandas.DataFrame(cur.fetchall(), columns=columns)
```
Replace with:
```python
C.export_to_pandas('SELECT * FROM table')
```
etc.
