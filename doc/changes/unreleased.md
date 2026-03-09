# Unreleased

## Refactoring

* #303: Updated to `exasol-toolbox` 5.0.0
* #307: Updated to `exasol-toolbox` 5.1.1 and re-locked `poetry.lock`
* #309: Re-locked `poetry.lock` to resolve CVE-2026-26007, which affected `cryptography` versions <= 46.0.4
* #311: Updated  to `exasol-toolbox` 6.0.0
* #317: Added check to `export_to_callback` and `import_from_callback` to ensure that the provided `callback` is `Callable`; if not, an exception is raised.
* #319: Improved error handling in the multithreaded `export_to_callback` so that all recorded exceptions are passed in `ExaExportError`
