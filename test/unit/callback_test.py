import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyexasol.callback import import_from_parquet


@pytest.mark.parquet
class TestImportFromParquetExceptions:
    @staticmethod
    def test_not_a_valid_path():
        with pytest.raises(ValueError, match="is not a `pathlib.Path`"):
            import_from_parquet(os.pipe(), "not_a_path")

    @staticmethod
    def test_does_not_match_any_files():
        with pytest.raises(ValueError, match="does not match any files"):
            import_from_parquet(os.pipe(), Path("no.parquet"))

    @staticmethod
    def test_hierarchical_data_is_not_supported(tmp_path):
        filepath = tmp_path / "hierarchical.parquet"
        table = pa.Table.from_pydict({"list": [[1]]})
        pq.write_table(table, filepath)

        with pytest.raises(ValueError, match="is hierarchical which is not supported"):
            import_from_parquet(os.pipe(), filepath)
