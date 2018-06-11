# Local config

Local config is a very popular feature among data scientists and analysts working on laptops. It allows users to store personal Exasol credentials and connection options in local file and separate it from code.

In order to use local config, please create file `.pyexasol.ini` in your home directory. Here is the example contents of this file:

```
[my_exasol]
dsn = myexasol1..5
user = my_user
password = my_password
schema = my_schema
compression = True
fetch_dict = True

```

You may specify any parameters available in [`connect`](/docs/REFERENCE.md#connect) function, except parameters expecting Python class or function.

You may specify multiple sections. Each section represents separate connection config.

In order to use local config, please use function [`connect_local_config`](/docs/REFERENCE.md#connect_local_config).

```python
import pyexasol

C = pyexasol.connect_local_config('my_exasol')

st = C.execute("SELECT CURRENT_TIMESTAMP")
print(st.fetchone())
```

You may specify different location for local config file using `config_path` argument of `connect_local_config` function.

You may overload class `ExaLocalConfig` or create your own implementation if you want more sophisticated config management.
