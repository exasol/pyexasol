import http.client
import io
import random
import threading
from http.server import ThreadingHTTPServer
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyexasol.transport.parquet.handler import ExaParquetHttpRequestHandler

parse_range = ExaParquetHttpRequestHandler._parse_range

CONTENT = bytes(range(100))


class TestParseRange:
    @staticmethod
    @pytest.mark.parametrize(
        "header, size, expected",
        [
            pytest.param(None, 100, (0, 99, False), id="no_header_whole_file"),
            pytest.param(None, 0, (0, -1, False), id="no_header_empty_file"),
            pytest.param("bytes=0-9", 100, (0, 9, True), id="closed"),
            pytest.param("bytes=10-", 100, (10, 99, True), id="open_ended"),
            pytest.param("bytes=-10", 100, (90, 99, True), id="suffix"),
            pytest.param(
                "bytes=-500", 100, (0, 99, True), id="suffix_larger_than_file"
            ),
            pytest.param("bytes=90-500", 100, (90, 99, True), id="end_clamped_to_file"),
            pytest.param(
                "bytes=0-9,20-29", 100, (0, 99, False), id="multiple_ranges_ignored"
            ),
            pytest.param(
                "items=0-9", 100, (0, 99, False), id="unsupported_unit_ignored"
            ),
            pytest.param("bytes=a-b", 100, (0, 99, False), id="not_numbers_ignored"),
            pytest.param(
                "bytes=20-10", 100, (0, 99, False), id="end_before_start_ignored"
            ),
            pytest.param("bytes=-", 100, (0, 99, False), id="no_digits_ignored"),
            pytest.param("bytes=0-99", 100, (0, 99, True), id="whole_file_is_partial"),
            pytest.param("bytes=0-", 100, (0, 99, True), id="open_ended_whole_file"),
            pytest.param(
                "bytes=0-9", 0, (0, -1, False), id="range_on_empty_file_ignored"
            ),
            pytest.param(" BYTES = 0-9", 100, (0, 9, True), id="unit_case_and_spaces"),
        ],
    )
    def test_satisfiable(header, size, expected):
        assert parse_range(header, size) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "header, size",
        [
            pytest.param("bytes=100-", 100, id="start_at_file_size"),
            pytest.param("bytes=-0", 100, id="zero_length_suffix"),
        ],
    )
    def test_unsatisfiable(header, size):
        assert parse_range(header, size) is None


class FileThatShrank:
    """Reports a larger size than the file really has, as if it was truncated."""

    def __init__(self, real_path, claimed_size):
        self._real_path = real_path
        self._claimed_size = claimed_size

    def stat(self):
        return SimpleNamespace(st_size=self._claimed_size)

    def open(self, mode):
        return self._real_path.open(mode)


@pytest.fixture
def parquet_file(tmp_path):
    path = tmp_path / "data.parquet"
    path.write_bytes(CONTENT)
    return path


@pytest.fixture
def serve(parquet_file):
    """Run the handler on a local HTTP server that stands in for the tunnel server."""
    started = []

    def _serve(file=None):
        server = ThreadingHTTPServer(("127.0.0.1", 0), ExaParquetHttpRequestHandler)
        server.parquet_file = file or parquet_file
        server.total_clients = 0
        server.request_error = None
        thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )
        thread.start()
        started.append((server, thread))
        return server

    yield _serve
    for server, thread in started:
        server.shutdown()
        server.server_close()
        thread.join()


def request(server, method, range_header=None):
    connection = http.client.HTTPConnection(*server.server_address, timeout=5)
    headers = {"Range": range_header} if range_header else {}
    connection.request(method, "/000.parquet", headers=headers)
    return connection, connection.getresponse()


class TestHead:
    @staticmethod
    def test_reports_size_without_body(serve):
        connection, response = request(serve(), "HEAD")

        assert response.status == 200
        assert response.getheader("Content-Length") == "100"
        assert response.getheader("Accept-Ranges") == "bytes"
        assert response.read() == b""
        connection.close()

    @staticmethod
    def test_ignores_range(serve):
        connection, response = request(serve(), "HEAD", "bytes=10-19")

        assert response.status == 200
        assert response.getheader("Content-Length") == "100"
        assert response.getheader("Content-Range") is None
        connection.close()


class TestGet:
    @staticmethod
    def test_with_range_returns_partial_content(serve):
        connection, response = request(serve(), "GET", "bytes=10-19")

        assert response.status == 206
        assert response.getheader("Content-Range") == "bytes 10-19/100"
        assert response.getheader("Content-Length") == "10"
        assert response.read() == CONTENT[10:20]
        connection.close()

    @staticmethod
    @pytest.mark.parametrize(
        "header", ["items=0-9", "bytes=a-b", "bytes=20-10", "bytes=0-9,20-29"]
    )
    def test_ignored_range_returns_whole_file(serve, header):
        connection, response = request(serve(), "GET", header)

        assert response.status == 200
        assert response.getheader("Content-Range") is None
        assert response.read() == CONTENT
        connection.close()

    @staticmethod
    def test_range_covering_whole_file_returns_partial_content(serve):
        connection, response = request(serve(), "GET", "bytes=0-")

        assert response.status == 206
        assert response.getheader("Content-Range") == "bytes 0-99/100"
        assert response.read() == CONTENT
        connection.close()

    @staticmethod
    def test_unsatisfiable_range_returns_416(serve):
        connection, response = request(serve(), "GET", "bytes=500-600")

        assert response.status == 416
        assert response.getheader("Content-Range") == "bytes */100"
        assert response.read() == b""
        connection.close()

    @staticmethod
    def test_serves_several_requests_over_one_keep_alive_connection(serve):
        server = serve()
        connection = http.client.HTTPConnection(*server.server_address, timeout=5)

        bodies = []
        for range_header in ("bytes=0-3", "bytes=-4", "bytes=50-59"):
            connection.request("GET", "/x", headers={"Range": range_header})
            bodies.append(connection.getresponse().read())

        assert bodies == [CONTENT[0:4], CONTENT[-4:], CONTENT[50:60]]
        assert server.total_clients == 1
        connection.close()

    @staticmethod
    def test_body_larger_than_chunk_size_is_complete(serve, monkeypatch):
        monkeypatch.setattr(ExaParquetHttpRequestHandler, "chunk_size", 7)

        connection, response = request(serve(), "GET")

        assert response.read() == CONTENT
        connection.close()


class TestFileChangedDuringImport:
    @staticmethod
    def test_short_file_drops_connection_and_records_error(serve, parquet_file):
        server = serve(FileThatShrank(parquet_file, claimed_size=150))
        connection, response = request(server, "GET")

        with pytest.raises(http.client.IncompleteRead):
            response.read()

        assert isinstance(server.request_error, OSError)
        assert "ended 50 bytes early" in str(server.request_error)
        connection.close()


ROWS_PER_ROW_GROUP = 100
ROW_GROUPS = 3


class RangeReader(io.RawIOBase):
    """
    Seekable file backed by HTTP range requests, as Exasol reads a Parquet file.

    ``HEAD`` gives the size, every ``read`` becomes a ranged ``GET``.
    """

    def __init__(self, server):
        self._connection = http.client.HTTPConnection(*server.server_address, timeout=5)
        self._position = 0
        self.bytes_served = 0

        self._connection.request("HEAD", "/000.parquet")
        response = self._connection.getresponse()
        response.read()
        self.size = int(response.getheader("Content-Length"))

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._position

    def seek(self, offset, whence=io.SEEK_SET):
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self._position, io.SEEK_END: self.size}
        self._position = base[whence] + offset
        return self._position

    def readinto(self, buffer):
        end = min(self._position + len(buffer), self.size) - 1
        if self._position > end:
            return 0
        self._connection.request(
            "GET", "/000.parquet", headers={"Range": f"bytes={self._position}-{end}"}
        )
        data = self._connection.getresponse().read()
        buffer[: len(data)] = data
        self._position += len(data)
        self.bytes_served += len(data)
        return len(data)

    def close(self):
        self._connection.close()
        super().close()


@pytest.fixture
def table():
    rows = ROWS_PER_ROW_GROUP * ROW_GROUPS
    # Random payload does not compress, so skipping that column saves real bytes.
    rng = random.Random(0)
    return pa.table(
        {
            "id": pa.array(range(rows), type=pa.int64()),
            "name": pa.array([f"name-{i}" for i in range(rows)]),
            "payload": pa.array([rng.randbytes(1000).hex() for _ in range(rows)]),
        }
    )


@pytest.fixture
def remote_parquet(serve, tmp_path, table):
    path = tmp_path / "real.parquet"
    pq.write_table(table, path, row_group_size=ROWS_PER_ROW_GROUP)
    reader = RangeReader(serve(path))
    yield reader
    reader.close()


@pytest.mark.parquet
class TestRealParquetFile:
    @staticmethod
    def test_first_and_last_bytes_of_file_are_served(serve, tmp_path, table):
        path = tmp_path / "real.parquet"
        pq.write_table(table, path, row_group_size=ROWS_PER_ROW_GROUP)
        content = path.read_bytes()
        server = serve(path)

        _, first = request(server, "GET", "bytes=0-11")
        first_bytes = first.read()
        _, last = request(server, "GET", "bytes=-12")
        last_bytes = last.read()

        assert first_bytes == content[:12]
        assert last_bytes == content[-12:]
        assert first_bytes != last_bytes
        assert first_bytes.startswith(b"PAR1")
        assert last_bytes.endswith(b"PAR1")

    @staticmethod
    def test_reading_through_ranges_gives_the_original_table(remote_parquet, table):
        parquet = pq.ParquetFile(remote_parquet)

        assert parquet.metadata.num_row_groups == ROW_GROUPS
        assert parquet.read().equals(table)
