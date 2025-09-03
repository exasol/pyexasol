import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyexasol.callback import (
    get_parquet_files,
    import_from_parquet,
)


class TestGetParquetFiles:
    @staticmethod
    def test_str_with_glob_works(tmp_path):
        tmp_file = tmp_path / "empty.parquet"
        tmp_file.touch()

        result = get_parquet_files(str(tmp_path / "*.parquet"))
        assert result == [tmp_file]

    @staticmethod
    def test_path_as_file_works(tmp_path):
        tmp_file = tmp_path / "empty.parquet"
        tmp_file.touch()

        result = get_parquet_files(tmp_file)
        assert result == [tmp_file]

    @staticmethod
    def test_path_as_directory_works(tmp_path):
        tmp_file = tmp_path / "empty.parquet"
        tmp_file.touch()

        result = get_parquet_files(tmp_path)
        assert result == [tmp_file]

    @staticmethod
    def test_list_path_not_valid_files():
        with pytest.raises(ValueError, match="contained entries which were not `Path`"):
            get_parquet_files([Path("not_a_valid_path_file")])

    @staticmethod
    def test_not_a_valid_source_type():
        with pytest.raises(ValueError, match="is not a supported type"):
            get_parquet_files({"not_a_path"})


@pytest.mark.parquet
class TestImportFromParquetExceptions:
    @staticmethod
    def test_matches_no_files():
        with pytest.raises(ValueError, match="does not match any files"):
            import_from_parquet(os.pipe()[0], "not_a_valid_path")

    @staticmethod
    def test_hierarchical_data_is_not_supported(tmp_path):
        filepath = tmp_path / "hierarchical.parquet"
        table = pa.Table.from_pydict({"list": [[1]]})
        pq.write_table(table, filepath)

        with pytest.raises(ValueError, match="is hierarchical which is not supported"):
            import_from_parquet(os.pipe()[0], filepath)
