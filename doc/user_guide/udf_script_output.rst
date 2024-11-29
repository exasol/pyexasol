UDF Script Output
=================

Exasol allows capturing combined output (STDOUT + STDERR) of UDF scripts. It is very helpful for debugging and to extract additional statistics from scripts running in production.

In order to use this feature, user has to run a TCP server and provide the address via session parameters. For example::

    ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = 'myserver:16442';

Exasol may run UDFs in parallel using a large amount of VMs. Each VM opens an individual connection to the TCP Server and keeps it open until the end of execution. The TCP server must be prepared for a large number of simultaneous connections.

PyEXASOL provides such a TCP server for your convenience. It can work in two different modes:

DEBUG MODE
----------

Debug mode is useful for manual debugging during UDF script development.

Connections are accepted from all VMs. The output of the first connection is displayed. Outputs of other connections are discarded.

The server runs forever until stopped by the user.

How to use it:

#. Run the server in debug mode::

        python -m pyexasol_utils.script_output

#. Copy-paste the provided SQL query to your SQL client and execute it;
#. Run queries with UDF scripts, see output in the terminal;
#. Stop the server with (Ctrl + C) when you finish debugging;

Please note: if you have problems getting script output immediately out of VMs, please make sure you **flush** STDOUT / STDERR in your UDF script. Some programming languages (like Python) may buffer output by default.

SCRIPT MODE
-----------

Script mode is useful for production usage and during the last stages of development.

Connections are accepted from all VMs. The output of each VM is stored in a separate log file.

The server runs for a single SQL statement and stops automatically.

How to use it:
1. (optional) Create a base directory for UDF script logs and set it using ``udf_output_dir`` connection option.
2. Execute the query with the UDF script using the function ``execute_udf_output()``.
3. Read and process files returned by the function.

Example::

    stmt, log_files = C.execute_udf_output("SELECT my_script(user_id) FROM table")

    printer.pprint(stmt.fetchall())
    printer.pprint(log_files[0].read_text())

You are responsible for the deletion of log files.

Connectivity Problem
--------------------

Unlike HTTP Transport, the script output TCP server is a real server. It receives incoming connections from Exasol nodes. Those connections might be blocked by firewalls and various network policies. You are responsible for making the host with the TCP server available for incoming connections.

If you want to bind the TCP server to a specific address and port, you may use ``--host``, ``--port`` arguments for debug mode and ``udf_output_bind_address``, ``udf_output_connect_address`` connection options for script mode.
