import os
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import dataset
from pyarrow.lib import ArrowInvalid

from pyexasol.callback import (
    check_export_to_parquet_directory_setting,
    export_to_parquet,
    get_parquet_files,
    import_from_parquet,
)


class TestCheckExportToParquetDirectorySetting:
    @staticmethod
    def test_non_existing_directory_passes(tmp_path):
        check_export_to_parquet_directory_setting(dst=tmp_path / "dummy")

    @staticmethod
    def test_existing_but_not_directory_fails(tmp_path):
        dst = tmp_path / "dummy"
        dst.touch()

        with pytest.raises(ValueError, match="exists and is not a directory"):
            check_export_to_parquet_directory_setting(dst=dst)

    @staticmethod
    def test_existing_and_empty_directory_passes(tmp_path):
        dst = tmp_path / "dummy"
        dst.mkdir()

        check_export_to_parquet_directory_setting(dst=dst)

    @staticmethod
    def test_existing_and_non_empty_directory_fails(tmp_path):
        dst = tmp_path / "dummy"
        dst.mkdir()
        filepath = dst / "file.txt"
        filepath.touch()

        with pytest.raises(ValueError, match="contains existing files"):
            check_export_to_parquet_directory_setting(dst=dst)

    @staticmethod
    @pytest.mark.parametrize(
        "existing_data_behavior", ("overwrite_or_ignore", "delete_matching")
    )
    def test_existing_and_non_empty_directory_passes(tmp_path, existing_data_behavior):
        dst = tmp_path / "dummy"
        dst.mkdir()
        filepath = dst / "file.txt"
        filepath.touch()

        check_export_to_parquet_directory_setting(
            dst=dst, callback_params={"existing_data_behavior": existing_data_behavior}
        )


@pytest.fixture
def pipe():
    data = """name,age,city\nAlice,25,New York\nBob,30,London"""
    return BytesIO(data.encode())


class TestExportToParquet:
    @staticmethod
    def get_row_count(filepath: Path) -> int:
        parquet_dataset = dataset.dataset(filepath)
        return sum(
            row_group.num_rows
            for fragment in parquet_dataset.get_fragments()
            for row_group in fragment.row_groups
        )

    def test_non_existing_directory_passes(self, tmp_path, pipe):
        dst = tmp_path / "dummy"

        export_to_parquet(pipe=pipe, dst=dst)
        assert self.get_row_count(dst) == 2

    def test_existing_but_not_directory_fails(self, tmp_path, pipe):
        filepath = tmp_path / "dummy"
        filepath.touch()

        with pytest.raises(FileExistsError, match="Cannot create directory"):
            export_to_parquet(pipe=pipe, dst=filepath)

    def test_existing_and_empty_directory_passes(self, tmp_path, pipe):
        dst = tmp_path / "dummy"
        dst.mkdir()

        export_to_parquet(pipe=pipe, dst=dst)
        assert self.get_row_count(dst) == 2

    def test_existing_and_non_empty_directory_fails(self, tmp_path, pipe):
        dst = tmp_path / "dummy"
        dst.mkdir()
        filepath = dst / "file.parquet"
        filepath.touch()

        with pytest.raises(ArrowInvalid, match="Could not write to"):
            export_to_parquet(pipe=pipe, dst=dst)

    @pytest.mark.parametrize(
        "existing_data_behavior", ("overwrite_or_ignore", "delete_matching")
    )
    def test_existing_and_non_empty_directory_passes(
        self, tmp_path, pipe, existing_data_behavior
    ):
        dst = tmp_path / "dummy"
        dst.mkdir()
        filepath = dst / "file.txt"
        filepath.touch()

        callback_params = {"existing_data_behavior": existing_data_behavior}
        export_to_parquet(pipe=pipe, dst=dst, **callback_params)

        # This is only needed as files are not deleted & for the test to
        # read from all files present in the directory.
        if existing_data_behavior == "overwrite_or_ignore":
            filepath.unlink()
        assert self.get_row_count(dst) == 2


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
