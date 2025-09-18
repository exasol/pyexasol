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
from typing import (
    TYPE_CHECKING,
    Union,
)

if TYPE_CHECKING:
    import pandas
    import polars


def export_to_list(pipe, dst, **kwargs) -> list:
    """
    Basic example of how to export CSV stream into basic list of tuples
    """
    wrapped_pipe = io.TextIOWrapper(pipe, newline="\n", encoding="utf-8")
    reader = csv.reader(wrapped_pipe, lineterminator="\n", **kwargs)

    return list(reader)


def export_to_pandas(pipe, dst, **kwargs) -> "pandas.DataFrame":
    """
    Basic example of how to export into :class:`pandas.DataFrame`
    Custom params for :func:`pandas.DataFrame.read_csv` may be passed in `**kwargs`
    """
    import pandas

    return pandas.read_csv(pipe, skip_blank_lines=False, **kwargs)


def export_to_parquet(pipe, dst: Union[Path, str], **kwargs) -> None:
    """
    Basic example of how to export into local parquet file(s)

    Args:
        dst: Local path to directory for exporting files. Can be either a Path
            or str. The default behavior is that the specified directory should be empty.
            If this is not the case, an exception is thrown.
        **kwargs:
            Custom params for :func:`pyarrow.dataset.write_dataset`. Some important
            defaults to note are:

              existing_data_behavior
                  Set to ``error``, which requires that the specified ``dst`` not
                  contain any files or an exception will be thrown.
              max_rows_per_file
                  Set to ``0``, which means that all rows will be written to 1 file.
                  If ``max_rows_per_file`` is altered, ensure that ``max_rows_per_group``
                  is set to a value less than or equal to the value of ``max_rows_per_file``.
              use_threads
                  Set to ``True`` and ``preserve_order`` is set to ``False``. This means
                  that the writing of multiple files will be done in parallel and that
                  the order is not guaranteed to be preserved.
    """
    from pyarrow import (
        csv,
        dataset,
    )

    reader = csv.open_csv(pipe)
    dataset.write_dataset(reader, base_dir=dst, format="parquet", **kwargs)


def export_to_polars(pipe, dst, **kwargs) -> "polars.DataFrame":
    """
    Basic example of how to export into :class:`polars.DataFrame`
    Custom params for :func:`polars.read_csv` may be passed in `**kwargs`
    """
    import polars

    return polars.read_csv(pipe, **kwargs)


def export_to_file(pipe, dst):
    """
    Basic example of how to export into file or file-like object opened in binary mode
    """
    if not hasattr(dst, "write"):
        dst = open(dst, "wb")

    shutil.copyfileobj(pipe, dst, 65535)


def import_from_iterable(pipe, src: Iterable, **kwargs):
    """
    Basic example of how to import from iterable object (list, dict, tuple, iterator, generator, etc.)
    """
    if not hasattr(src, "__iter__"):
        raise ValueError("Data source is not iterable")

    wrapped_pipe = io.TextIOWrapper(pipe, newline="\n", encoding="utf-8")
    writer = csv.writer(
        wrapped_pipe, lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC, **kwargs
    )

    for row in src:
        writer.writerow(row)


def import_from_pandas(pipe, src: "pandas.DataFrame", **kwargs):
    """
    Basic example of how to import from :class:`pandas.DataFrame`
    Custom params for :fun:`pandas.DataFrame.to_csv` may be passed in **kwargs
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


def import_from_parquet(
    pipe, source: Union[list[Path], Path, str], **kwargs
):  # NOSONAR(S3776)
    """
    Basic example of how to import from :class:`pyarrow.parquet.ParquetFile` via local parquet file(s)

    Args:
        source: Local filepath specification(s) to process. Can be one of:
            - list[pathlib.Path]: list of specific files
            - pathlib.Path: can be either a file or directory. If it is a directory,
            all files matching the following pattern *.parquet will be processed.
            - str: representing a filepath which already contains a glob pattern
            (e.g., "/local_dir/*.parquet")
        **kwargs:
            Custom params for :func:`pyarrow.parquet.ParquetFile.iter_batches`. This can be used
            to specify what columns should be read and their preferred order.

    Please note that nested or hierarchical column types are not supported.
    """
    from pyarrow import (
        csv,
        parquet,
        types,
    )

    def ensure_no_nested_columns(
        schema, requested_columns: Union[list[str], None]
    ) -> None:
        nested_fields = []
        for field in schema:
            if not types.is_nested(field.type):
                continue
            if requested_columns and field.name in requested_columns:
                nested_fields.append(field)
            if not requested_columns:
                nested_fields.append(field)

        if nested_fields:
            raise ValueError(
                f"Fields {nested_fields} of schema from file {file} is hierarchical which is not supported."
            )

    if not (parquet_files := get_parquet_files(source)):
        raise ValueError(f"source {source} does not match any files")

    columns = kwargs.get("columns", None)
    for file in parquet_files:
        parquet_file = parquet.ParquetFile(file, memory_map=True)
        ensure_no_nested_columns(parquet_file.schema_arrow, columns)
        for batch in parquet_file.iter_batches(**kwargs):
            write_options = csv.WriteOptions(include_header=False)
            csv.write_csv(batch, pipe, write_options=write_options)


def import_from_polars(
    pipe, src: Union["polars.LazyFrame", "polars.DataFrame"], **kwargs
):
    """
    Basic example of how to import from :class:`polars.DataFrame` or :class:`polars.LazyFrame`
    Custom params for :func:`polars.DataFrame.write_csv` may be passed in `**kwargs`
    """
    import polars

    if isinstance(src, polars.LazyFrame):
        src = src.collect()
    elif not isinstance(src, polars.DataFrame):
        raise ValueError("Data source is not polars.DataFrame or polars.LazyFrame")

    return src.write_csv(pipe, include_header=False, **kwargs)


def import_from_file(pipe, src):
    """
    Basic example of how to import from file or file-like object opened in binary mode
    """
    if not hasattr(src, "read"):
        src = open(src, "rb")

    shutil.copyfileobj(src, pipe, 65536)
