# 2.1.0 - 2026-03-10

## Summary

This update improves error reporting for `import_from_callback` and `export_to_callback`
(used internally for the `import_from_*` and `export_to_*`
[variants](https://exasol.github.io/pyexasol/master/user_guide/exploring_features/import_and_export/index.html#variants))
by introducing a custom exception pattern. When data transfer fails, the library now
captures and wraps exceptions from the main execution thread, the HTTP thread,
and SQL thread. This ensures that concurrent failures are no longer obscured by whichever
exception was caught first. Instead, the recorded exceptions are given with their
tracebacks making it easier to identify what went wrong.

## Refactoring

* #303: Updated to `exasol-toolbox` 5.0.0
* #307: Updated to `exasol-toolbox` 5.1.1 and re-locked `poetry.lock`
* #309: Re-locked `poetry.lock` to resolve CVE-2026-26007, which affected `cryptography` versions <= 46.0.4
* #311: Updated  to `exasol-toolbox` 6.0.0
* #317: Added check to `export_to_callback` and `import_from_callback` to ensure that the provided `callback` is `Callable`; if not, an exception is raised.
* #319: Improved error handling in the multithreaded `export_to_callback` so that all recorded exceptions are passed in `ExaExportError`
* #313: Improved error handling in the multithreaded `import_from_callback` so that all recorded exceptions are passed in `ExaImportError`
* #279: Modified documentation for exception changes in `export_to_callback` and `import_from_callback`

## Dependency Updates

### `main`

* Updated dependency `cryptography:46.0.3` to `46.0.5`
* Updated dependency `orjson:3.11.4` to `3.11.5`
* Updated dependency `polars:1.34.0` to `1.37.1`
* Updated dependency `pyarrow:22.0.0` to `23.0.0`
* Added dependency `pytest-benchmark:5.2.3`

### `dev`

* Updated dependency `exasol-toolbox:4.0.0` to `6.0.0`
* Updated dependency `pandas-stubs:2.3.3.251219` to `2.3.3.260113`

### `performance`

* Removed dependency `pytest-benchmark:5.1.0`
