import os
import ssl

dsn = os.environ.get("EXAHOST", "localhost:8563")
user = os.environ.get("EXAUID", "SYS")
password = os.environ.get("EXAPWD", "exasol")
schema = os.environ.get("EXASCHEMA", "TEST")
# For usage with a Docker test setup, we deactivate the requirement to use a strict
# certificate verification. However, users are strongly recommended to use the default
# with `{"cert_reqs": ssl.CERT_REQUIRED}` or other secure variants found
# in the `Security <https://exasol.github.io/pyexasol/master/user_guide/configuration/security.html>`__ page.
websocket_sslopt = {"cert_reqs": ssl.CERT_NONE}
