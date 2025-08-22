Configuration File ``.pyexasol.ini``
===================================

Using a local configuration file is a popular feature among data scientists and analysts working on laptops.
It allows users to store personal Exasol credentials and connection options separate from the code.

Simply create a file ``~/.pyexasol.ini``, where the contents should look similar to:

.. code-block:: ini

    [my_exasol]
    dsn = myexasol1..5
    user = my_user
    password = my_password
    schema = my_schema
    compression = True
    fetch_dict = True

If a user has multiple Exasol instances, each of the Exasol instances can have their
access credential defined in a separate section of the ``ini`` file.

To create a connection using a local configuration file, please call :func:`pyexasol.connect_local_config`:

.. code-block:: python

    import pyexasol

    C = pyexasol.connect_local_config(
      config_section='my_exasol',
      config_path="~/.pyexasol.ini"
    )

    st = C.execute("SELECT CURRENT_TIMESTAMP")
    print(st.fetchone())

For more sophisticated configuration management, overload the :class:`ExaLocalConfig` or create a custom implementation of a local configuration loader.
