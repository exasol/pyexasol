# 0.10.0 - 2020-01-01

## PyExasol code improvements

- Reworked script output code and moved it into `pyexasol_utils` module. The new way to start script output server in debug mode is: `python -m pyexasol_utils.script_output`. Old call will produce the RuntimeException with directions.
- Removed `.utils` sub-module.
- Renamed `pyexasol_utils.http` into `pyexasol_utils.http_transport` for consistency.

## ExaConnection

- Fixed bug of `.execute_udf_output()` not working with empty `udf_output_bind_address`.
- Added function `_encrypt_password()`, logic was moved from `.utils`.
- Added function `_get_stmt_output_dir()`, logic was moved from `.utils`. It is now possible to overload this function.

