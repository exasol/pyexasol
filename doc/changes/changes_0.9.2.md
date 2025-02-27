# 0.9.2 - 2019-12-08

## ExaExtension

- Metadata functions (starting with `.ext.get_sys_*`) are now using `/*snapshot execution*/` SQL hint described in [IDEA-476](https://www.exasol.com/support/browse/IDEA-476) to prevent locks.

