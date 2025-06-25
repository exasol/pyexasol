# Unreleased

## Summary

### Websocket in connection

From PyExasol version `1.0.0`, the default behavior has been changed to use strict
certificate verification in `ExaConnection` and `pyexasol.connect`. This means that
the default `websocket_sslopt=None` will be mapped to
`{"cert_reqs": ssl.CERT_REQUIRED}`. The prior default behavior was to map such cases
to `{"cert_reqs": ssl.CERT_NONE}`.

* For many users relying on the previous default behavior for encrypted connections,
simply upgrading to version `1.0.0` is likely to lead to breaking changes with error
messages like: `[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed`.

Prior to the upgrade:
1. Please determine which encryption and security measures are appropriate for your
organization by reading through `doc/user_guide/encryption.rst`.
2. If needed, update your usage of `pyexasol.connect(...)` & `ExaConnection` to
reflect your organization's needs.

### IMPORT & EXPORT

In Exasol DB versions prior to version 8.32.0, the behavior of the database was to have TLS
certificate validation deactivated for IMPORT
and EXPORT queries, leaving connections potentially vulnerable to security risks like
man-in-the-middle attacks. Users needed to explicitly enable TLS certificate validation
using custom parameters or SQL syntax. Now, TLS Certificate Validation is activated by
default for IMPORT and EXPORT queries, ensuring secure data transfers by validating
certificates for external file connections like HTTPS and FTPS. For more details, see
`CHANGELOG: TLS Certificate Verification for Loader File Connections <https://exasol.my.site.com/s/article/Changelog-content-16273>`_.

Pyexasol uses a self-signed certificate for the encrypted `http_transport` methods, which means that such queries would
fail the default enabled TLS certificate validation (from Exasol DB version 8.32.0), as the provided certificate is not a globally trusted certificate.
Thus, from PyExasol version `1.0.0`, we have adapted the default behavior of `ExaSQLThread` to include clauses like
`PUBLIC KEY 'sha256//<sha256_base64_encoded_public_key>'` in these statements. Here's an example of an EXPORT query sent to an Exasol DB:

```sql
    EXPORT my_table INTO CSV
    AT '127.18.0.2:8364/YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o='
    PUBLIC KEY 'sha256//YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o=' FILE '000.gz''
    WITH COLUMN HEADERS
```

## ✨Features

* Added support for multi-version documentation
* Added support for all standard nox tasks provided by `exasol-toolbox`
* #179: Modified `ExaConnection` so that default is encryption with strict certification verification
* #190: Added nox task to run examples
* #194: Modified `ExaSQLThread` to include PUBLIC KEY in IMPORT and EXPORT queries for Exasol DB version 8.32.0 and onwards

## ⚒️ Refactorings

* Reformatted entire code base with `black` and `isort`
* #194: Refactored `ExaSQLThread` and its children to reduce duplication and added types

## 🔩 Internal

* Relocked dependencies
  * Due to changes in cryptography's Python support (!=3.9.0 and 3.9.1), we updated our support to Python ^3.9.2.
* Added exasol-toolbox workflows and actions
* Added missing plugin for multi-version documentation
* Added support for publishing documentation to gh pages
* Added `.git-blame-ignore-revs` file to workspace

    Note: please make sure to adjust your git config accordingly (if not done yet)

        ``shell
        git config blame.ignoreRevsFile .git-blame-ignore-revs
        ``
* #181: Modified integration tests to run also with `ssl.CERT_REQUIRED`
* #187: Updated poetry to 2.1.2 & exasol-toolbox to `1.0.1`
* Relocked dependencies to resolve CVE-2025-43859 for transitive dependence `h11`
* Relocked dependencies for transitive dependencies to resolve CVE-2025-47287 `tornado` and CVE-2025-47273 for `setuptools`
* Relocked dependencies for transitive dependency to resolve CVE-2024-47081  `requests`
* #194: Adapted integration tests so that tests with `ssl.CERT_REQUIRED` can be deselected
* #200: Activated Sonar for CI & Relocked dependencies to resolve CVEs for transitive dependence `urllib3`

## 📚 Documentation

* Added sphinx based documentation
* Added example to highlight how sensitive information from exceptions should be handled
* Harmonized spelling of PyExasol across documentation
