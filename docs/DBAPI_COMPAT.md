## DB-API 2.0 compatibility

PyEXASOL [public interface](/docs/REFERENCE.md) resembles [PEP-249 DB-API 2.0](https://www.python.org/dev/peps/pep-0249/) specification, but it does not strictly follow it. This is done on purpose.

Basic [compatibility wrapper](#db-api-20-wrapper) is available, so you could try PyEXASOL in context of existing application, but it should not be used in production and should not be relied upon.

---

The main reasons behind this decision are following:
- To discourage "drop-in" replacements of other Exasol drivers without switching to faster [HTTP transport](/docs/HTTP_TRANSPORT.md);
- To prevent usage of ORM, SQLAlchemy and other SQL wrappers originally designed for row-based databases;
- To introduce better [SQL formatter](/docs/SQL_FORMATTING.md);

Unlike common OLTP databases, each OLAP database is unique. Understanding of implementation details is mandatory. Generalisation of any kind and "copy-paste" approach may lead to abysmal performance in trivial cases. You should know exactly what you're doing.

## Key differences

- No `Cursor` object.
- `Execute()` method belongs to main `ExaConnection`. `Execute()` returns `ExaStatement` which behaves very similar to `Cursor`, but it is NOT reusable.
- No `executemany()`, no `nextset()`.
- No `.description` attribute in `ExaStatement`. Methods `columns()` and `column_names()` are provided instead. Method `columns()` returns information about result set columns in Exasol-specific format.
- `Autocommit=True` by default to make sure Exasol stores indexes and statistic objects after SELECT statements.
- No `.paramstyle` option. Custom Python 3 new style formatter is provided instead.

## Migration

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

## DB-API 2.0 wrapper

In order to make it easier to start using PyEXASOL, simple DB-API 2.0 wrapper is provided. Please see example:

```python
# Import "wrapped" version of PyEXASOL module
import pyexasol.db2 as E

C = E.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)

# Cursor
cur = C.cursor()
cur.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")

# Standard .description and .rowcount attributes
print(cur.description)
print(cur.rowcount)

# Standard fetching
while True:
    row = cur.fetchone()

    if row is None:
        break

    print(row)

```
