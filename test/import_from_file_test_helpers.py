import io
import os
from contextlib import nullcontext
from pathlib import Path

import pytest


class CustomPath:
    def __init__(self, filepath: Path):
        self.filepath = filepath

    def __fspath__(self):
        return str(self.filepath)


def source_string_io(filepath: Path):
    return io.StringIO("a,b\n")


def source_bytes_io(filepath: Path):
    return io.BytesIO(b"a,b\n")


def source_text_file(filepath: Path):
    return open(filepath)


def source_binary_file(filepath: Path):
    return open(filepath, "rb")


def source_path(filepath: Path):
    return nullcontext(filepath)


def source_string_path(filepath: Path):
    return nullcontext(str(filepath))


def source_bytes_path(filepath: Path):
    return nullcontext(os.fsencode(filepath))


def source_custom_path(filepath: Path):
    return nullcontext(CustomPath(filepath))


BINARY_SOURCE_FACTORIES = [
    pytest.param(source_bytes_io, id="BytesIO"),
    pytest.param(source_binary_file, id="binary-file"),
    pytest.param(source_path, id="Path"),
    pytest.param(source_string_path, id="string-path"),
    pytest.param(source_bytes_path, id="bytes-path"),
    pytest.param(source_custom_path, id="custom-PathLike"),
]

TEXT_SOURCE_FACTORIES = [
    pytest.param(source_string_io, id="StringIO"),
    pytest.param(source_text_file, id="text-file"),
]

MISSING_PATH_FACTORIES = [
    pytest.param(str, id="string-path"),
    pytest.param(os.fsencode, id="bytes-path"),
    pytest.param(Path, id="Path"),
    pytest.param(CustomPath, id="custom-PathLike"),
]
