# 0.26.0 - 2024-07-04

- Added dbapi2 compliant driver interface `exasol.driver.websocket` ontop of pyexasol

    ⚠️ Note:

    This driver facade should only be used if one is certain that using the dbapi2 is the right solution for their scenario, taking all implications into account. For more details on why and who should avoid using dbapi2, please refer to the **DBAPI2 compatibility section** in our documentation.

- Dropped support for python 3.7
- Dropped support for Exasol 6.x
- Dropped support for Exasol 7.0.x
- Relocked dependencies (Internal)
- Switched packaging and project workflow to poetry (internal)


