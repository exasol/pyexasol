import ssl
import statistics
import sys
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import pyexasol

# usage: bench_import.py [n_files=20] [rows_per_file=50000] [repeats=3]
N_FILES = int(sys.argv[1]) if len(sys.argv) > 1 else 20
ROWS = int(sys.argv[2]) if len(sys.argv) > 2 else 50_000
REPEATS = int(sys.argv[3]) if len(sys.argv) > 3 else 3
# label -> kwargs for import_from_parquet
MODES = {
    "native, 1 file per statement": {"max_parallel_files": 1},
    "native, 10 files per statement": {"max_parallel_files": 10},
    "callback (native=False)": {"native": False},
}

con = pyexasol.connect(
    dsn="localhost:8563",
    user="SYS",
    password="exasol",
    websocket_sslopt={"cert_reqs": ssl.CERT_NONE},
)
print("DB version:", con.exasol_db_version)
con.execute("CREATE SCHEMA IF NOT EXISTS BENCH_PARQUET")
con.execute("OPEN SCHEMA BENCH_PARQUET")

with tempfile.TemporaryDirectory() as d:
    for i in range(N_FILES):
        pq.write_table(
            pa.table(
                {
                    "ID": pa.array(range(i * ROWS, (i + 1) * ROWS), type=pa.int64()),
                    "NAME": pa.array([f"name_{j}" for j in range(ROWS)]),
                    "VAL": pa.array([j * 0.5 for j in range(ROWS)], type=pa.float64()),
                }
            ),
            Path(d) / f"{i:03d}.parquet",
        )
    times = {m: [] for m in MODES}
    for rep in range(REPEATS):
        for mode, kwargs in MODES.items():
            con.execute(
                "CREATE OR REPLACE TABLE BENCH (ID DECIMAL(18,0), NAME VARCHAR(50), VAL DOUBLE)"
            )
            t = time.perf_counter()
            con.import_from_parquet(Path(d), "BENCH", **kwargs)
            times[mode].append(time.perf_counter() - t)
            n = con.execute("SELECT COUNT(*) FROM BENCH").fetchval()
            assert n == N_FILES * ROWS, n
    con.execute("DROP SCHEMA BENCH_PARQUET CASCADE")

print(f"{N_FILES} files x {ROWS} rows, {REPEATS} repeats")
for mode, t in times.items():
    print(f"{mode:<32} min {min(t):.2f}s  median {statistics.median(t):.2f}s")
