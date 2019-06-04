# HTTP transport

The main purpose of HTTP transport is to reduce massive fetching overhead associated with large data sets (1M+ rows). It uses native Exasol commands `EXPORT` and `IMPORT` specifically designed to move large amounts of data. Data is transferred using CSV format with optional compression.

This is a powerful tool which helps to bypass creation of Python objects altogether and dramatically increase performance.

PyEXASOL offloads HTTP communication and decompression to separate process using [multiprocessing](https://docs.python.org/3/library/multiprocessing.html) module. Main process only reads or writes to [pipe](https://docs.python.org/3/library/os.html#os.pipe) opened in binary mode.

Specify custom `callback` function to read or write from pipe and to apply complex logic. Use `callback_params` to pass additional parameters to `callback` function (e.g. options for pandas).

You may also specify `import_params` or `export_params` to alter `IMPORT` or `EXPORT` query and modify data stream.

# Pre-defined functions

## Export from Exasol to pandas
Export data from Exasol into `pandas.DataFrame`. You may use `callback_param` argument to pass custom options for pandas [`read_csv`](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html) function.

```python
# Read from SQL
pd = C.export_to_pandas("SELECT * FROM users")

# Read from table
pd = C.export_to_pandas("users")
```

## Import from pandas to Exasol
Import data from `pandas.DataFrame` into Exasol table. You may use `callback_param` argument to pass custom options for pandas [`to_csv`](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_csv.html) function.

```python
C.import_from_pandas(pd, "users")
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

C.import_from_iterable(my_generator, "users")
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
| Name | Example | Description |
| --- | --- | --- |
| `column_separator` | `,` | Column separator for CSV |
| `column_delimiter` | `"` | Column delimiter for CSV (quotting) |
| `row_separator` | `LF` | Row separator for CSV (line-endings) |
| `encoding` | `UTF8` | File encoding |
| `with_column_names` | `True` | Add column names as first line, useful for Pandas |
| `null` | `\N` | Custom `NULL` value |
| `delimit` | `AUTO` | Delimiter mode: `AUTO`, `ALWAYS`, `NONE` |
| `format` | `gz` | Import file or stream compressed with `gz`, `bzip2`, `zip` |

### export_params
| Name | Example | Description |
| --- | --- | --- |
| `column_separator` | `,` | Column separator for CSV |
| `column_delimiter` | `"` | Column delimiter for CSV (quotting) |
| `row_separator` | `LF` | Row separator for CSV (line-endings) |
| `encoding` | `UTF8` | File encoding |
| `skip` | `1` | How many first rows to skip, useful for skipping header |
| `null` | `\N` | Custom `NULL` value |
| `trim` | `TRIM` | Trim mode: `TRIM`, `RTRIM`, `LTRIM` |
| `format` | `gz` | Export file or stream compressed with `gz`, `bzip2`, `zip` |

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
