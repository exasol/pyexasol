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


def import_from_iterable(pipe, src, **kwargs):
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
    import packaging.version
    import pandas

    if not isinstance(src, pandas.DataFrame):
        raise ValueError("Data source is not pandas.DataFrame")

    wrapped_pipe = io.TextIOWrapper(pipe, newline="\n", encoding="utf-8")

    # https://github.com/pandas-dev/pandas/pull/45302
    if packaging.version.parse(pandas.__version__) >= packaging.version.parse("1.5.0"):
        return src.to_csv(
            wrapped_pipe,
            header=False,
            index=False,
            lineterminator="\n",
            quoting=csv.QUOTE_NONNUMERIC,
            **kwargs
        )
    else:
        return src.to_csv(
            wrapped_pipe,
            header=False,
            index=False,
            line_terminator="\n",
            quoting=csv.QUOTE_NONNUMERIC,
            **kwargs
        )


def import_from_file(pipe, src):
    """
    Basic example how to import from file or file-like object opened in binary mode
    """
    if not hasattr(src, "read"):
        src = open(src, "rb")

    shutil.copyfileobj(src, pipe, 65536)
