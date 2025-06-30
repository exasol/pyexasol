# 0.9.2 - 2019-12-08

## ExaExtension

- Metadata functions (starting with `.ext.get_sys_*`) are now using `/*snapshot execution*/` SQL hint described in [Snapshot Mode](https://docs.exasol.com/db/latest/database_concepts/snapshot_mode.htm) to prevent locks.
