"""
Collection of basic callbacks for import / export functions
You may write your own callbacks for complex cases

--------------

Export callback arguments:
1) pipe opened in "rb" mode
2) destination for callback (if applicable, can be None)
3) callback_params as dict

--------------

Import callback arguments:
1) pipe opened in "wb" mode
2) source data for callback (e.g. pandas data frame, list, file object)
3) callback_params as dict

"""

import csv
import glob
import io
import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import Union


def export_to_list(pipe, dst, **kwargs):
    """
    Basic example how to export CSV stream into basic list of tuples
    """
    wrapped_pipe = io.TextIOWrapper(pipe, newline="\n", encoding="utf-8")
    reader = csv.reader(wrapped_pipe, lineterminator="\n", **kwargs)

    return [row for row in reader]


def export_to_pandas(pipe, dst, **kwargs):
    """
    Basic example how to export into Pandas DataFrame
    Custom params for "read_csv" may be passed in **kwargs
    """
    import pandas

    return pandas.read_csv(pipe, skip_blank_lines=False, **kwargs)


def export_to_file(pipe, dst):
    """
    Basic example how to export into file or file-like object opened in binary mode
    """
    if not hasattr(dst, "write"):
        dst = open(dst, "wb")

    shutil.copyfileobj(pipe, dst, 65535)


def import_from_iterable(pipe, src: Iterable, **kwargs):
    """
    Basic example how to import from iterable object (list, dict, tuple, iterator, generator, etc.)
    """
    if not hasattr(src, "__iter__"):
        raise ValueError("Data source is not iterable")

    wrapped_pipe = io.TextIOWrapper(pipe, newline="\n", encoding="utf-8")
    writer = csv.writer(
        wrapped_pipe, lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC, **kwargs
    )

    for row in src:
        writer.writerow(row)


def import_from_pandas(pipe, src, **kwargs):
    """
    Basic example how to import from Pandas DataFrame
    Custom params for "to_csv" may be passed in **kwargs
    """
    import pandas

    if not isinstance(src, pandas.DataFrame):
        raise ValueError("Data source is not pandas.DataFrame")

    wrapped_pipe = io.TextIOWrapper(pipe, newline="\n", encoding="utf-8")
    src.to_csv(
        wrapped_pipe,
        header=False,
        index=False,
        lineterminator="\n",
        quoting=csv.QUOTE_NONNUMERIC,
        **kwargs,
    )


def get_parquet_files(source: Union[list[Path], Path, str]) -> list[Path]:
    if isinstance(source, str):
        matches = glob.glob(source)
        return sorted(
            [filepath for i in matches if (filepath := Path(i)) and filepath.is_file()]
        )
    if isinstance(source, Path):
        if source.is_file():
            return [source]
        elif source.is_dir():
            return sorted(source.glob("*.parquet"))
    if isinstance(source, list):
        not_a_valid_path_file = [
            filepath
            for filepath in source
            if not isinstance(filepath, Path) or not filepath.is_file()
        ]
        if len(not_a_valid_path_file) > 0:
            raise ValueError(
                f"source {source} contained entries which were not `Path` or valid `Path` files {not_a_valid_path_file}"
            )
        return source
    raise ValueError(
        f"source {source} is not a supported type (Union[list[Path], Path, str])."
    )


def import_from_parquet(pipe, source: Union[list[Path], Path, str], **kwargs):
    """
    Basic example how to import from pyarrow parquet file(s)

    Args:
        source: Local filepath specification(s) to process. Can be one of:
            - list[pathlib.Path]: list of specific files
            - pathlib.Path: can be either a file or directory. If it is a directory,
            all files matching the following pattern *.parquet will be processed.
            - str: representing a filepath which already contains a glob pattern
            (e.g., "/local_dir/*.parquet")
        **kwargs:
            Custom params for "pyarrow.csv.WriteOptions"

    Please note that nested or hierarchical column types are not supported.
    """
    from pyarrow import (
        csv,
        parquet,
        types,
    )

    if not (parquet_files := get_parquet_files(source)):
        raise ValueError(f"source {source} does not match any files")

    for file in parquet_files:
        parquet_file = parquet.ParquetFile(file)

        nested_fields = [
            field for field in parquet_file.schema_arrow if types.is_nested(field.type)
        ]
        if nested_fields:
            raise ValueError(
                f"Fields {nested_fields} of schema from file {file} is hierarchical which is not supported."
            )

        num_row_groups = parquet_file.num_row_groups
        for i in range(num_row_groups):
            row_group_table = parquet_file.read_row_group(i)

            write_options = csv.WriteOptions(include_header=False, **kwargs)
            csv.write_csv(row_group_table, pipe, write_options=write_options)


def import_from_file(pipe, src):
    """
    Basic example how to import from file or file-like object opened in binary mode
    """
    if not hasattr(src, "read"):
        src = open(src, "rb")

    shutil.copyfileobj(src, pipe, 65536)
