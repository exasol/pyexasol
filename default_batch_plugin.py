"""Pytest plugin: force the default max_parallel_files from env BENCH_BATCH."""

import functools
import os

from pyexasol import ExaConnection

_original = ExaConnection.import_from_parquet


@functools.wraps(_original)
def _patched(self, *args, **kwargs):
    if "BENCH_BATCH" in os.environ and "max_parallel_files" not in kwargs:
        kwargs["max_parallel_files"] = int(os.environ["BENCH_BATCH"])
    return _original(self, *args, **kwargs)


setattr(ExaConnection, "import_from_parquet", _patched)
