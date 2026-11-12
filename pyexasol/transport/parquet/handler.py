from __future__ import annotations

import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler

from .server import ExaParquetTCPServer

# One byte range: ``bytes=<start>-<end>``, ``bytes=<start>-`` or ``bytes=-<suffix>``.
_BYTE_RANGE = re.compile(r"\s*bytes\s*=\s*([0-9]*)-([0-9]*)\s*", re.IGNORECASE)


class ExaParquetHttpRequestHandler(BaseHTTPRequestHandler):
    """
    Serve the Parquet file with HEAD and GET requests, including byte ranges.

    Exasol reads Parquet files like an object store: it requests the file size,
    then the footer and the row groups it needs using ``Range`` headers, over one
    keep-alive connection. Every tunnel serves exactly one file, so the requested
    path (e.g. ``/000.parquet`` for ``FILE '000.parquet'``) is not evaluated.
    """

    protocol_version = "HTTP/1.1"
    chunk_size = 65536

    server: ExaParquetTCPServer

    def setup(self):
        super().setup()
        self.server.total_clients += 1

    def log_message(self, format, *args):
        """
        Suppress the request logging of ``BaseHTTPRequestHandler``.

        By default, the stdlib writes one line to stderr per request, and Exasol issues
        many small range requests per import (the footer, then each row group). The
        logged client address would also be the tunnel endpoint, not a real peer.
        ``log_request`` and ``log_error`` route through this method, so protocol
        errors are silenced as well; the client still receives the HTTP error
        response. Route this to the connection logger if error handling and
        logging will be handled at a higher level.
        """
        pass

    def do_HEAD(self):
        """
        Answer a ``HEAD`` request with the headers of the matching ``GET`` and without a body.

        Exasol sends ``HEAD`` first to learn the file size from ``Content-Length``.
        """
        self._send_headers()

    def do_GET(self):
        """
        Answer a ``GET`` request with the file, or the requested byte range of it.

        Exasol uses ranged ``GET`` requests to read the Parquet footer and then only
        the row groups it needs.
        """
        if byte_range := self._send_headers():
            self._write_range(*byte_range)

    def _send_headers(self) -> tuple[int, int] | None:
        """
        Send the response headers, shared so ``HEAD`` and ``GET`` always match.

        Answers 200 OK for the whole file, 206 Partial Content with ``Content-Range``
        for a byte range and 416 Range Not Satisfiable for an unsatisfiable one.
        Returns the inclusive ``(start, end)`` offsets of the body to send, or
        ``None`` for 416.
        """
        parquet_file = self.server.parquet_file
        file_size = parquet_file.stat().st_size
        range_header = self.headers.get("Range")
        byte_range = self._parse_range(range_header, file_size)
        if byte_range is None:
            self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
            self.send_header("Content-Range", f"bytes */{file_size}")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return None

        start, end = byte_range
        status = HTTPStatus.PARTIAL_CONTENT if range_header else HTTPStatus.OK
        self.send_response(status)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(end - start + 1))
        if range_header:
            self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
        self.end_headers()
        return start, end

    @staticmethod
    def _parse_range(
        range_header: str | None, file_size: int
    ) -> tuple[int, int] | None:
        """
        Return the inclusive ``(start, end)`` byte offsets to send of the Parquet file.

        Offsets are zero-based, so ``bytes=0-99`` is the first 100 bytes. Only one
        range is supported (``a-b``, ``a-`` and ``-n``); further ranges are ignored.
        Examples for a 1000 byte file:

        - ``bytes=100-199``: bytes 100 to 199, ``(100, 199)``.
        - ``bytes=900-``: from byte 900 to the end, ``(900, 999)``.
        - ``bytes=-100``: the last 100 bytes, ``(900, 999)``. This is how a reader
          fetches the Parquet footer without knowing the file size first.
        - ``bytes=900-5000``: an end past the file is clamped, ``(900, 999)``.
        - no ``Range`` header: the whole file, ``(0, 999)``.

        ``None`` (answered with 416 Range Not Satisfiable) is returned for a unit
        other than ``bytes``, a malformed range, or a start at or beyond the end of
        the file.
        """
        last = file_size - 1
        if not range_header:
            return 0, last

        match = _BYTE_RANGE.fullmatch(range_header.split(",")[0])
        if not match:
            return None

        first, second = match.groups()
        if first:
            start = int(first)
            end = min(int(second), last) if second else last
        elif second:
            start = max(file_size - int(second), 0)
            end = last
        else:
            return None

        if start >= file_size or end < start:
            return None
        return start, end

    def _write_range(self, start: int, end: int):
        """
        Stream the bytes ``start`` to ``end`` (inclusive) of the file as the body.

        The file is read in ``chunk_size`` pieces, so large row groups never have to
        fit in memory. This runs after ``_send_headers`` has sent ``Content-Length``, so
        exactly ``end - start + 1`` bytes must follow. If the file turns out shorter
        than announced (for example, it was modified during the import), the
        connection is closed and an ``OSError`` is stored in ``server.request_error``,
        so Exasol sees a failed transfer instead of a silently truncated body.
        """
        parquet_file = self.server.parquet_file
        remaining = end - start + 1
        with parquet_file.open("rb") as source:
            source.seek(start)
            while remaining:
                chunk = source.read(min(remaining, self.chunk_size))
                if not chunk:
                    self.close_connection = True
                    self.server.request_error = OSError(
                        f"{parquet_file} ended {remaining} bytes early; "
                        "was it modified during the import?"
                    )
                    return
                self.wfile.write(chunk)
                remaining -= len(chunk)
