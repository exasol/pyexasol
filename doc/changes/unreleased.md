# Unreleased

## Summary

### Websocket in connection
From PyExasol version ``1.0.0``, the default behavior has been changed to use strict 
certificate verification in ``ExaConnection`` and ``pyexasol.connect``. This means that 
the default ``websocket_sslopt=None`` will be mapped to 
``{"cert_reqs": ssl.CERT_REQUIRED}``. The prior default behavior was to map such cases 
to ``{"cert_reqs": ssl.CERT_NONE}``. 

* For many users relying on the previous default behavior for encrypted connections, 
simply upgrading to version ``1.0.0`` is likely to lead to breaking changes with error 
messages like: ``[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed``.

Prior to the upgrade:
1. Please determine which encryption and security measures are appropriate for your 
organization by reading through ``doc/user_guide/encryption.rst``.
2. If needed, update your usage of ``pyexasol.connect(...)`` & ``ExaConnection`` to 
reflect your organization's needs.

### IMPORT & EXPORT
With Exasol DB versions x & x, the default behavior has changed. Previously, the default
behavior of the database was to have TLS certificate validation deactivated for IMPORT 
and EXPORT queries, leaving connections potentially vulnerable to security risks like 
man-in-the-middle attacks. Users needed to explicitly enable TLS certificate validation 
using custom parameters or SQL syntax. Now, TLS Certificate Validation is activated by 
default for IMPORT and EXPORT queries, ensuring secure data transfers by validating 
certificates for external file connections like HTTPS and FTPS.

To deactivate TLS certificate validation manually:
* Use the database parameter -etlCheckCertsDefault=0. This restores the previous behavior of no certificate validation for external connections.
* (recommendation) Use the IGNORE CERTIFICATE / PUBLIC KEY on the SQL query to deactivate certificate validation for that query.

Pyexasol uses a self-signed certificate for IMPORT & EXPORT, which means that it will
fail the default enabled TLS certificate validation, as it is not a globally trusted certificate.
Thus, with `pyexasol.connection().export*` and `pyexasol.connection().import*` methods,
users will need to specify one of the following for the `value` in their `export_params={"certificate":<value>}`
or `import_params={"certificate":<value>}`:
* `IGNORE CERTIFICATE` - to disable certificate verification
* `PUBLIC KEY 'sha256//*******'` - to specify the public key for certificate verification which only works from 8.32+

Users who fail to make this changes will see errors like `SSL certificate problem: unable to get local issuer certificate`.

## ✨Features

* Added support for multi-version documentation
* Added support for all standard nox tasks provided by `exasol-toolbox`
* #179: Modified `ExaConnection` so that default is encryption with strict certification verification
* #194: Added option to specify `certificate` in import and export, which is needed 
for DB versions 8.32+ as pyexasol uses a self-signed certificate which is not a globally trusted certificate. A user can
instead supply `IGNORE CERTIFICATE` or `PUBLIC KEY 'sha256//*******'`

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

        ```shell
        git config blame.ignoreRevsFile .git-blame-ignore-revs
        ```
* #181: Modified integration tests to run also with `ssl.CERT_REQUIRED`
* #187: Updated poetry to 2.1.2 & exasol-toolbox to `1.0.1`
* Relocked dependencies to resolve CVE-2025-43859 for transitive dependency `h11`
* Relocked dependencies for transitive dependencies to resolve CVE-2025-47287 `tornado` and CVE-2025-47273 for `setuptools`
* Relocked dependencies for transitive dependency to resolve CVE-2024-47081  `requests`

## 📚 Documentation

* Added sphinx based documentation
* Added example to highlight how sensitive information from exceptions should be handled
* Harmonized spelling of PyExasol across documentation
