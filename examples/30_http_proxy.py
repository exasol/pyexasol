"""
Example 30
Open connection with HTTP proxy
"""

import pyexasol
import subprocess
import time
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

# Simple HTTP proxy
pproxy = subprocess.Popen(['pproxy', '-l', 'http://:8562/', '--reuse'])
time.sleep(1)

C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
                     , http_proxy='http://localhost:8562')

stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(stmt.fetchall())

C.close()
pproxy.terminate()


# HTTP proxy with auth
pproxy = subprocess.Popen(['pproxy', '-l', 'http://:8562/#my_user:secret_pass', '--reuse'])
time.sleep(1)

C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema
                     , http_proxy='http://my_user:secret_pass@localhost:8562')

stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(stmt.fetchall())

C.close()
pproxy.terminate()
