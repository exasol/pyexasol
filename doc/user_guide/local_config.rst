Local config
============

Local config is a popular feature among data scientists and analysts working on laptops.
It allows users to store personal Exasol credentials and connection options in a local file and separate it from code.

In order to use local config, please create a file `~/.pyexasol.ini` in your home directory.
Here is the sample content of this file:

.. code-block:: ini

    [my_exasol]
    dsn = myexasol1..5
    user = my_user
    password = my_password
    schema = my_schema
    compression = True
    fetch_dict = True

You may specify any parameters available in ``connect`` function,
except parameters expecting a Python class or function.

You may specify multiple sections. Each section represents a separate connection config.
It might be useful if you have multiple Exasol instances.

In order to create a connection using local config, please call function ``connect_local_config <https://../docs/REFERENCE.md#connect_local_config>``.

.. code-block:: python

    import pyexasol

    C = pyexasol.connect_local_config('my_exasol')

    st = C.execute("SELECT CURRENT_TIMESTAMP")
    print(st.fetchone())

You may specify a different location for the local config file using the `config_path` argument of the `connect_local_config` function.

You may overload the class `ExaLocalConfig` or create your own implementation of local config if you want more sophisticated config management.
