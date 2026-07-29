"""
Open connection with HTTP proxy
"""

import pprint
import subprocess
import sys
import time

import examples._config as config
import pyexasol

PPROXY_COMPAT_BOOTSTRAP = (
    "import asyncio\n"
    "import sys\n"
    "asyncio.set_event_loop(asyncio.new_event_loop())\n"
    "from pproxy.server import main\n"
    "sys.exit(main())"
)


def _start_pproxy(proxy_url):
    if sys.version_info >= (3, 14):
        # Python 3.14 requires an explicit event loop before pproxy starts.
        command = [
            sys.executable,
            "-c",
            PPROXY_COMPAT_BOOTSTRAP,
            "-l",
            proxy_url,
            "--reuse",
        ]
    else:
        command = ["pproxy", "-l", proxy_url, "--reuse"]

    return subprocess.Popen(command)


printer = pprint.PrettyPrinter(indent=4, width=140)

# Simple HTTP proxy
pproxy = _start_pproxy("http://:8562/")
time.sleep(1)

C = pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    schema=config.schema,
    http_proxy="http://localhost:8562",
    websocket_sslopt=config.websocket_sslopt,
)

stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(stmt.fetchall())

C.close()
pproxy.terminate()


# HTTP proxy with auth
pproxy = _start_pproxy("http://:8562/#my_user:secret_pass")
time.sleep(1)

C = pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    schema=config.schema,
    http_proxy="http://my_user:secret_pass@localhost:8562",
    websocket_sslopt=config.websocket_sslopt,
)

stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(stmt.fetchall())

C.close()
pproxy.terminate()
