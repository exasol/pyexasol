# UDF script output

Exasol allows capturing combined output (STDOUT + STDERR) of UDF scripts. It is very helpful for debugging and to extract additional statistics from scripts running in production.

In order to use this feature, user has to run TCP server and provide address via session parameters. For example:

```sql
ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = 'myserver:16442';
```

Exasol may run UDFs in parallel using large amount of VM's. Each VM opens individual connection to TCP Server and keeps it opened until the end of execution. TCP server must be prepared for large number of simultaneous connections.

PyEXASOL provides such TCP server for your convenience. It can work in two different modes:

### DEBUG MODE
Debug mode is useful for manual debugging during UDF script development.

Connections are accepted from all VM's. Output of first connection is displayed. Outputs of other connections are discarded.

Server runs forever until stopped by user.

How to use it:

1. Run server in debug mode;
```
python -m pyexasol script_debug
```
2. Copy-paste provided SQL query to your SQL client and execute it;
3. Run queries with UDF scripts, see output in terminal;
4. Stop server with (Ctrl + C) when you finish debugging;


### SCRIPT MODE
Script mode is useful for production usage and during last stages of development.

Connections are accepted from all VM's. Output of each VM is stored into separate log file.

Server runs for single SQL statement and stops automatically.

How to use it:
1. (optional) Create base directory for UDF script logs and set it using `udf_output_dir` connection option.
2. Execute query with UDF script using function [`execute_udf_output()`](/docs/REFERENCE.md#execute_udf_output).
3. Read and process files returned by function.

Example:
```python
stmt, log_files = C.execute_udf_output("SELECT my_script(user_id) FROM table")

printer.pprint(stmt.fetchall())
printer.pprint(log_files[0].read_text())

```

You are responsible for deletion of log files.

## Connectivity problem

Unlike [HTTP Transport](/docs/HTTP_TRANSPORT.md), script output TCP server is real server. It receives incoming connections from Exasol nodes. Those connections might be blocked by firewalls and various network policies. You are responsible for making host with TCP server available for incoming connections.

If you want to bind TCP server to specific address and port, you may use `--host`, `--port` arguments for debug mode and `udf_output_host`, `udf_output_port` connection options for script mode.
