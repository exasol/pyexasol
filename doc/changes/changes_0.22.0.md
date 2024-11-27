# 0.22.0 - 2021-11-19

**BREAKING (!):** HTTP transport was significantly reworked in this version. Now it uses [threading](https://docs.python.org/3/library/threading.html) instead of [subprocess](https://docs.python.org/3/library/subprocess.html) to handle CSV data streaming.

There are no changes in a common **single-process** HTTP transport.

There are some breaking changes in **parallel** HTTP transport:

- Argument `mode` was removed from `http_transport()` function, it is no longer needed.
- Word "proxy" used in context of HTTP transport was replaced with "exa_address" in documentation and code. Word "proxy" now refers to connections routed through an actual HTTP proxy only.
- Function `ExaHTTPTransportWrapper.get_proxy()` was replaced with property `ExaHTTPTransportWrapper.exa_address`. Function `.get_proxy()` is still available for backwards compatibility, but it is deprecated.
- Module `pyexasol_utils.http_transport` no longer exists.
- Constants `HTTP_EXPORT` and `HTTP_IMPORT` are no longer exposed in `pyexasol` module.

Rationale:

- Threading provides much better compatibility with Windows OS and various exotic setups (e.g. uWSGI).
- Orphan "http_transport" processes will no longer be a problem.
- Modern Pandas and Dask can (mostly) release GIL when reading or writing CSV streams.
- HTTP thread is primarily dealing with network I/O and zlib compression, which (mostly) release GIL as well.

Execution time for small data sets might be improved by 1-2s, since another Python interpreter is no longer started from scratch. Execution time for very large data sets might be ~2-5% worse for CPU bound workloads and unchanged for network bound workloads.

Also, **examples** were re-arranged in this version, refactored and grouped into multiple categories.

