"""Build Parquet import query.

See the Exasol documentation for the
`IMPORT <https://docs.exasol.com/db/latest/loading_data/load_data_parquet.htm>`
"""

from .builders import ImportBuilder

__all__ = ["ImportBuilder"]
