# HTTP transport

The main purpose of HTTP transport is to reduce massive fetching overhead associated with large data sets (1M+ rows). It uses native Exasol commands `EXPORT` and `IMPORT` specifically designed to move large amounts of data. Data is transferred using CSV format with optional zlib compression.

This is a powerful tool which helps to bypass creation of intermediate Python objects altogether and dramatically increases performance.

PyEXASOL offloads HTTP communication and decompression to separate thread using [threading](https://docs.python.org/3/library/threading.html) module. Main thread deals with a simple [pipe](https://docs.python.org/3/library/os.html#os.pipe) opened in binary mode.

You may specify a custom `callback` function to read or write from raw pipe and to apply complex logic. Use `callback_params` to pass additional parameters to `callback` function (e.g. options for pandas or polars).

You may also specify `import_params` or `export_params` to alter `IMPORT` or `EXPORT` query and modify CSV data stream.

# Pre-defined functions

## Export from Exasol to pandas
Export data from Exasol into `pandas.DataFrame`. You may use `callback_param` argument to pass custom options for pandas [`read_csv`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html) function.

```python
# Read from SQL
pd = C.export_to_pandas("SELECT * FROM users")

# Read from table
pd = C.export_to_pandas("users")
```

## Import from pandas to Exasol
Import data from `pandas.DataFrame` into Exasol table. You may use `callback_param` argument to pass custom options for pandas [`to_csv`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html) function.

```python
C.import_from_pandas(pd, "users")
```

## Export from Exasol to polars
Export data from Exasol into `polars.DataFrame`. You may use `callback_param` argument to pass custom options for polars [`read_csv`](https://docs.pola.rs/api/python/stable/reference/api/polars.read_csv.html) function.

```python
# Read from SQL
df = C.export_to_polars("SELECT * FROM users")

# Read from table
df = C.export_to_polars("users")
```

## Import from polars to Exasol
Import data from `polars.DataFrame` or `polars.LazyFrame` into Exasol table. You may use `callback_param` argument to pass custom options for polars [`write_csv`](https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_csv.html) function.

```python
C.import_from_polars(df, "users")
```

## Import from list (a-la INSERT)

```python
my_list = [
    (1, 'Bob', False, '2018-01-01'),
    (2, 'Gill', True, '2018-02-01'),
]

C.import_from_iterable(my_list, "users")
```

## Import from generator
This function is suitable for very big INSERTS as long as generator returns rows 1-by-1 and does not run out of memory.

```python
def my_generator():
    for i in range(5):
        yield (i, 'Bob', True, '2017-01-01')

C.import_from_iterable(my_generator(), "users")
```

## Import from file
Import data from file, path object or file-like object opened in binary mode. You may import from process `STDIN` using `sys.stdin.buffer`.
```python
# Import from file defined with string path
C.import_from_file('/test/my_file.csv', "users")

# Import from path object
C.import_from_file(pathlib.Path('/test/my_file.csv'), "users")

# Import from opened file
file = open('/test/my_file.csv', 'rb')
C.import_from_file(file, "users")
file.close()

# Import from STDIN
C.import_from_file(sys.stdin.buffer, "users")
```

## Export to file
Export data from Exasol into file, path object or file-like object opened in binary mode. You may export to process `STDOUT` using `sys.stdout.buffer`.
```python
# Export from file defined with string path
C.export_to_file('my_file.csv', "users")

# Export into STDOUT
C.export_to_file(sys.stdout.buffer, "users")
```

# Parameters

Please refer to Exasol User Manual to know more about `IMPORT` / `EXPORT` parameters.

### import_params
| Name | Example | Description                                                                                                                                                                                                  |
| --- | --- |--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `column_separator` | `,` | Column separator for CSV                                                                                                                                                                                     |
| `column_delimiter` | `"` | Column delimiter for CSV (quotting)                                                                                                                                                                          |
| `columns` | `['id', 'name']` | List of table columns in data source, useful if column order of data source does not match column order of Exasol table                                                                                      |
| `csv_cols` | `["1..5", "6 FORMAT='999.99'", "8"]` | List of CSV columns with optional [numeric](https://docs.exasol.com/sql_references/formatmodels.htm#NumericFormat) or [date](https://docs.exasol.com/sql_references/formatmodels.htm#DateTimeFormat) formats |
| `row_separator` | `LF` | Row separator for CSV (line-endings)                                                                                                                                                                         |
| `encoding` | `UTF8` | File encoding                                                                                                                                                                                                |
| `with_column_names` | `True` | Add column names as first line, useful for Pandas and Polars                                                                                                                                                 |
| `null` | `\N` | Custom `NULL` value                                                                                                                                                                                          |
| `delimit` | `AUTO` | Delimiter mode: `AUTO`, `ALWAYS`, `NEVER`                                                                                                                                                                    |
| `format` | `gz` | Import file or stream compressed with `gz`, `bzip2`, `zip`                                                                                                                                                   |
| `comment` | `This is a query description` | Add a comment before the beginning of the query                                                                                                                                                              |

### export_params
| Name | Example | Description |
| --- | --- | --- |
| `column_separator` | `,` | Column separator for CSV |
| `column_delimiter` | `"` | Column delimiter for CSV (quotting) |
| `columns` | `['id', 'name']` | List of table columns, useful to reorder table columns during export from table |
| `csv_cols` | `["1..5", "6 FORMAT='999.99'", "8"]` | List of CSV columns with optional [numeric](https://docs.exasol.com/sql_references/formatmodels.htm#NumericFormat) or [date](https://docs.exasol.com/sql_references/formatmodels.htm#DateTimeFormat) formats |
| `row_separator` | `LF` | Row separator for CSV (line-endings) |
| `encoding` | `UTF8` | File encoding |
| `skip` | `1` | How many first rows to skip, useful for skipping header |
| `null` | `\N` | Custom `NULL` value |
| `trim` | `TRIM` | Trim mode: `TRIM`, `RTRIM`, `LTRIM` |
| `format` | `gz` | Export file or stream compressed with `gz`, `bzip2`, `zip` |
| `comment` | `This is a query description` | Add a comment before the beginning of the query |


### The `comment` parameter, for adding comments to queries

For any `export_*` or `import_*` call, you can add a comment that will be inserted before the beginning of the query.

This can be used for profiling and auditing. Example:

```python
C.import_from_file('/test/my_file.csv', 'users', import_params={'comment': '''
This comment will be inserted before the query.
This query is importing user from CSV.
'''})
```

The comment is inserted as a block comment (`/* <comment> */`). Block comment closing sequence (`*/`) is forbidden in the comment.


# How to write custom EXPORT \ IMPORT functions

Full collection of pre-defined callback functions is available in [`callback.py`](/pyexasol/callback.py) module.

Example of callback exporting into basic Python list.

```python
# Define callback function
def export_to_list(pipe, dst, **kwargs):
    wrapped_pipe = io.TextIOWrapper(pipe, newline='\n')
    reader = csv.reader(wrapped_pipe, lineterminator='\n', **kwargs)

    return [row for row in reader]
    
# Run EXPORT using defined callback function
C.export_to_callback(export_to_list, None, 'my_table')
```

Example of callback importing from Pandas into Exasol.

```python
df = <pandas.DataFrame>

def import_from_pandas(pipe, src, **kwargs):
    wrapped_pipe = io.TextIOWrapper(pipe, newline='\n')
    return src.to_csv(wrapped_pipe, header=False, index=False, quoting=csv.QUOTE_NONNUMERIC, **kwargs)

# Run IMPORT using defined callback function
C.export_from_callback(import_from_pandas, df, 'my_table')
```

Example of callback importing from Polars into Exasol.

```python
df = <polars.DataFrame>

def import_from_polars(pipe, src, **kwargs):
    wrapped_pipe = io.TextIOWrapper(pipe, newline='\n')
    return src.write_csv(wrapped_pipe, include_header=False, date_format="%Y-%m-%d", datetime_format="%Y-%m-%d %H:%M:%S%.f", **kwargs)

# Run IMPORT using defined callback function
C.export_from_callback(import_from_polars, df, 'my_table')
```