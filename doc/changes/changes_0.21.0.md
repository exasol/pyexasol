# 0.21.0 - 2021-09-27

- Default `protocol_version` is now 3.
- Dropped support for Exasol versions `6.0` and `6.1`.

These versions have reached ["end of life"](https://www.exasol.com/portal/display/DOWNLOAD/Exasol+Life+Cycle) and are no longer supported by vendor. It is still possible to connect to older Exasol versions using PyExasol, but you may have to set `protocol_version=1` connection option explicitly.

