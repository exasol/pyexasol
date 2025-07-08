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
import io
import shutil
from collections.abc import Iterable
from pathlib import Path


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


def import_from_parquet(pipe, source: Path, **kwargs):
    """
    Basic example how to import from pyarrow parquet file(s)

    Args:
        source:
            Local filepath to a parquet file or set of files matching a glob pattern
        **kwargs:
            Custom params for "pyarrow.csv.WriteOptions"

    Please note that nested or hierarchical types are not supported.
    """
    from pyarrow import (
        csv,
        parquet,
        types,
    )

    if not isinstance(source, Path):
        raise ValueError(f"source {source} is not a `pathlib.Path`")

    matching_files = list(source.parent.glob(source.name))
    if not matching_files:
        raise ValueError(f"source {source} does not match any files")

    for file in sorted(matching_files):
        table = parquet.read_table(file)
        for field in table.schema:
            if types.is_nested(field.type):
                raise ValueError(
                    f"Field {field} of schema from file {file} is hierarchical which is not supported."
                )
        write_options = csv.WriteOptions(include_header=False, **kwargs)
        csv.write_csv(table, pipe, write_options=write_options)


def import_from_file(pipe, src):
    """
    Basic example how to import from file or file-like object opened in binary mode
    """
    if not hasattr(src, "read"):
        src = open(src, "rb")

    shutil.copyfileobj(src, pipe, 65536)
