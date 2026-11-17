"""Serve local Parquet files to Exasol through the HTTP tunnel (native Parquet import)."""

from .thread import ExaParquetHttpThread

__all__ = ["ExaParquetHttpThread"]
