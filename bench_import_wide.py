# usage: bench_import_wide.py n_files rows_per_file n_columns
# Compares native (1 and 10 files per statement) and callback imports for wide tables.
import ssl
import statistics
import sys
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import pyexasol

N_FILES, ROWS, N_COLS = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
REPEATS = 3
MODES = {
    "native, 1 per stmt": {"max_parallel_files": 1},
    "native, 10 per stmt": {"max_parallel_files": 10},
    "callback": {"native": False},
}
# a third each of BIGINT, DOUBLE and VARCHAR columns
kinds = ["BIGINT", "DOUBLE", "VARCHAR"]
cols = [(f"C{i}", kinds[i % 3]) for i in range(N_COLS)]
ddl = ", ".join(
    f"{n} "
    + {"BIGINT": "DECIMAL(18,0)", "DOUBLE": "DOUBLE", "VARCHAR": "VARCHAR(30)"}[k]
    for n, k in cols
)


def column(kind, offset):
    if kind == "BIGINT":
        return pa.array(range(offset, offset + ROWS), type=pa.int64())
    if kind == "DOUBLE":
        return pa.array([j * 0.5 for j in range(ROWS)], type=pa.float64())
    return pa.array([f"value_{j % 1000}" for j in range(ROWS)])


con = pyexasol.connect(
    dsn="localhost:8563",
    user="SYS",
    password="exasol",
    websocket_sslopt={"cert_reqs": ssl.CERT_NONE},
)
con.execute("CREATE SCHEMA IF NOT EXISTS BENCH_PARQUET")
con.execute("OPEN SCHEMA BENCH_PARQUET")
with tempfile.TemporaryDirectory() as d:
    for i in range(N_FILES):
        pq.write_table(
            pa.table({n: column(k, i * ROWS) for n, k in cols}),
            Path(d) / f"{i:03d}.parquet",
        )
    size = sum(f.stat().st_size for f in Path(d).glob("*.parquet")) / 1e6
    times = {m: [] for m in MODES}
    for _ in range(REPEATS):
        for mode, kw in MODES.items():
            con.execute(f"CREATE OR REPLACE TABLE BENCH ({ddl})")
            t = time.perf_counter()
            con.import_from_parquet(Path(d), "BENCH", **kw)
            times[mode].append(time.perf_counter() - t)
            assert (
                con.execute("SELECT COUNT(*) FROM BENCH").fetchval() == N_FILES * ROWS
            )
con.execute("DROP SCHEMA BENCH_PARQUET CASCADE")
print(f"{N_FILES} files x {ROWS} rows x {N_COLS} cols ({size:.0f} MB parquet)")
for m, t in times.items():
    print(f"  {m:<20} median {statistics.median(t):.2f}s  min {min(t):.2f}s")
