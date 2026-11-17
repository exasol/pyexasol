# Parquet Import Benchmark: Native Batches vs Callback

Branch: `feature/394-part-3-import-native-parquet`
Database: `exasol/docker-db:2026.1.0`, local docker container (client and database on the same host)
Scripts: `bench_import.py` and `bench_import_wide.py` (repo root)

## Modes compared

All three use `import_from_parquet`:

| Mode | Call | What happens |
|------|------|--------------|
| Native, 1 file per statement | `max_parallel_files=1` | Files one after another. One tunnel and one `IMPORT` per file. |
| Native, 10 files per statement | `max_parallel_files=10` (default) | Up to 10 tunnels (one thread each) in one `IMPORT`. Further files follow in later statements. |
| Callback | `native=False` | One tunnel and one `IMPORT` for all files. PyArrow reads the files and sends CSV. |

The three tables below were measured before the certificate was shared (see
"After sharing the certificate").

Setup: each shape was run 3 times, alternating between the three modes. Times are
wall-clock for the `import_from_parquet` call only (file and table creation excluded).
The row count is asserted after every run. The table has the columns
`ID DECIMAL(18,0)`, `NAME VARCHAR(50)`, `VAL DOUBLE`.

## Native, 1 file per statement

| Files | Rows per file | min | median |
|-------|---------------|-----|--------|
| 10    | 5,000         | 1.36 s | 1.40 s |
| 20    | 5,000         | 2.69 s | 2.77 s |
| 50    | 5,000         | 6.99 s | 7.13 s |
| 20    | 50,000        | 3.32 s | 3.41 s |
| 10    | 200,000       | 2.09 s | 2.12 s |

The time grows linearly with the file count, at about 0.14 s per file. Data volume
matters little, so the cost is mostly per-statement overhead.

## Native, 10 files per statement

| Files | Rows per file | min | median | Speedup vs 1 per statement (median) |
|-------|---------------|-----|--------|------------------------------------|
| 10    | 5,000         | 0.56 s | 0.59 s | 2.4x |
| 20    | 5,000         | 1.16 s | 1.31 s | 2.1x |
| 50    | 5,000         | 3.37 s | 3.42 s | 2.1x |
| 20    | 50,000        | 1.54 s | 1.64 s | 2.1x |
| 10    | 200,000       | 1.27 s | 1.33 s | 1.6x |

Batching is about 2x faster than one file per statement in every shape tested.

## Callback (`native=False`)

| Files | Rows per file | min    | median | Native, 10 per statement is slower by (median) |
|-------|---------------|--------|--------|------------------------------------------------|
| 10    | 5,000         | 0.09 s | 0.09 s | 6.6x                                           |
| 20    | 5,000         | 0.09 s | 0.11 s | 11.9x                                          |
| 50    | 5,000         | 0.16 s | 0.16 s | 21.4x                                          |
| 20    | 50,000        | 0.29 s | 0.31 s | 5.3x                                           |
| 10    | 200,000       | 0.49 s | 0.50 s | 2.7x                                           |

## Where the native time goes

A profile of a native import of 10 files with 5,000 rows each (0.44 s) showed that
0.26 s went into creating the tunnel servers, and 0.21 s of that was RSA key generation
for the TLS certificate (about 21 ms per tunnel, one certificate per tunnel).
The callback import creates one certificate per import, so it does not pay this per file.

To fix this, `import_from_parquet` now generates one certificate per call and shares it
between all tunnels (`ssl_context` argument of `ExaTunnelServer` and
`ExaParquetHttpThread`).

## After sharing the certificate

Same script, same shapes, 3 repeats, medians. "Before" is the table above.

| Files x rows | Native 1 per statement (before / after) | Native 10 per statement (before / after) | Callback | Native 10 is slower than callback by |
|--------------|------------------------------------------|-------------------------------------------|----------|---------------------------------------|
| 10 x 5,000   | 1.40 s / 1.23 s | 0.59 s / 0.27 s | 0.09 s | 3.0x |
| 20 x 5,000   | 2.77 s / 2.62 s | 1.31 s / 0.59 s | 0.14 s | 4.2x |
| 50 x 5,000   | 7.13 s / 6.27 s | 3.42 s / 1.22 s | 0.15 s | 8.1x |
| 20 x 50,000  | 3.41 s / 2.97 s | 1.64 s / 0.86 s | 0.34 s | 2.5x |
| 10 x 200,000 | 2.12 s / 1.85 s | 1.33 s / 0.96 s | 0.52 s | 1.8x |

Minimum times after the change: native 1 per statement 1.14, 2.34, 6.05, 2.97, 1.80 s;
native 10 per statement 0.26, 0.52, 1.21, 0.81, 0.94 s; callback 0.09, 0.13, 0.13, 0.31,
0.52 s.

## Conclusions

- Within the native import, batching 10 files per statement is about 2x faster than
  one file per statement. It does not depend on the certificate: with 1 file per
  statement, sharing the certificate only helped by about 10 percent, so each statement
  has a fixed cost of roughly 0.1 s that batching pays once instead of once per file.
- Sharing one certificate per import made native with 10 files per statement 1.4x to
  2.8x faster. The gain is largest for many small files.
- The callback import is still faster than the native import in every shape
  (1.8x to 8.1x compared with 10 files per statement). The gap is widest for many small
  files and narrows for large files. The remaining native cost is the per-statement
  overhead and, probably, the many small HTTP range requests with which Exasol reads
  each Parquet file through the tunnel. The latter was not profiled.
- These are small files on a local machine. The native import's other properties
  (Exasol reads the Parquet file directly, no CSV conversion of the data) were not
  measured, nor where, if at all, the native import overtakes the callback import for
  larger data.

## Wider data

`bench_import_wide.py`: 10 files with 20,000 rows each, after the certificate sharing.
The columns are a third each `DECIMAL(18,0)` (Parquet `int64`), `DOUBLE` and
`VARCHAR(30)` (values repeat, so they compress well). 3 repeats, medians.

| Columns | Parquet size | Native 1 per statement | Native 10 per statement | Callback | Native 10 vs callback |
|---------|--------------|------------------------|-------------------------|----------|-----------------------|
| 3       | 2 MB         | 1.20 s                 | 0.31 s                  | 0.18 s   | 1.7x slower           |
| 30      | 24 MB        | 1.75 s                 | 0.59 s                  | 0.44 s   | 1.3x slower           |
| 100     | 80 MB        | 3.31 s                 | 1.40 s                  | 1.26 s   | 1.1x slower           |
| 300     | 239 MB       | 9.22 s                 | 3.63 s                  | 3.60 s   | tied                  |

- The gap to the callback import closes as the data per file grows. The native costs
  are mostly fixed per file or per statement, so they matter less for wide files. At 300
  columns native with 10 files per statement and the callback import take the same time.
- Batching is faster at every width (3.8x at 3 columns down to 2.5x at 300 columns),
  but its advantage shrinks as the data grows.
- The native import did not get ahead of the callback import in these runs, although it
  skips the CSV conversion.
- CPU and memory use were not measured, and only this one shape (10 files, 3 repeats)
  was run.

## Not measured

- Other databases or remote clients. Only one local container was used. Network latency
  would probably add to the per-statement and per-tunnel cost, but this was not tested.
- Batch sizes other than 1 and 10.
- Whether Exasol reads the files of one batch in parallel or one after another. The
  speedup from batching could come from either that or the saved per-statement overhead.
- Memory and CPU use.
- Files that compress poorly, and more than 300 columns.

## Existing import tests

The 15 import tests in `parquet_test.py` (`TestImportFromParquet`,
`TestNativeImportFromParquetMultipleFilesAndErrors`) were run 3 times each with the
default batch size forced to 1 and to 10 (`default_batch_plugin.py`, env var
`BENCH_BATCH`):

| `BENCH_BATCH` | Run 1  | Run 2  | Run 3  |
|---------------|--------|--------|--------|
| 1             | 9.12 s | 9.22 s | 9.26 s |
| 10            | 9.34 s | 9.41 s | 9.68 s |

No meaningful difference. The tests use at most 5 files and are dominated by setup.
Forcing the batch size to 1 makes two tests fail (`test_type_mismatch_raises`,
`test_unreadable_file_raises`), which is expected. They assert that 0 rows were imported
after a failure, which only holds when all files share one statement.

## How to rerun

```
poetry run python bench_import.py [n_files=20] [rows_per_file=50000] [repeats=3]

poetry run python bench_import_wide.py 10 20000 100   # files, rows per file, columns

BENCH_BATCH=1 PYTHONPATH=. poetry run pytest -p default_batch_plugin \
  test/integration/import_and_export/parquet_test.py \
  -k "TestImportFromParquet or TestNativeImport" -q
```

`bench_import.py` assumes `localhost:8563`, user `SYS`, password `exasol` and no
certificate validation. Edit the `pyexasol.connect(...)` call at the top for other setups.
