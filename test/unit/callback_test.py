import os
from pathlib import Path

import pytest

from pyexasol.callback import import_from_parquet


@pytest.mark.parquet
class TestImportFromParquet:
    @staticmethod
    def test_not_a_valid_path():
        with pytest.raises(ValueError, match="is not a `pathlib.Path`"):
            import_from_parquet(os.pipe(), "not_a_path")

    @staticmethod
    def test_does_not_match_any_files():
        with pytest.raises(ValueError, match="does not match any files"):
            import_from_parquet(os.pipe(), Path("no.parquet"))
