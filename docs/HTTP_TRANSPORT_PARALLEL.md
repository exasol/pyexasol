# HTTP transport (parallel)

It is possible to run [HTTP Transport](/docs/HTTP_TRANSPORT.md) in parallel. Workload may be distributed across multiple CPU cores and even across multiple servers.

## How it works on high level

1. Parent process opens main connection to Exasol and spawns multiple child processes.
2. Each child process connects to individual Exasol node using [`http_transport()`](/docs/REFERENCE.md#http_transport), gets internal Exasol address (`ipaddr:port` string) using `.address` property, and sends it to parent process.
3. Parent process collects list of internal Exasol addresses from child processes and runs [`export_parallel()`](/docs/REFERENCE.md#export_parallel) or [`import_parallel()`](/docs/REFERENCE.md#import_parallel) function to execute SQL query.
4. Each child process runs callback function and reads or sends chunk of data from or to Exasol.
5. Parent process waits for SQL query and child processes to finish.

![Parallel export](/docs/img/parallel_export.png)

Please note that PyEXASOL does not provide any specific way to send internal Exasol address strings from child processes to parent process. You are free to choose your own way of inter-process communication. For example, you may use [multiprocessing.Pipe](https://docs.python.org/3/library/multiprocessing.html?highlight=Pipes#exchanging-objects-between-processes).

## Examples

- [b03_parallel_export](/examples/b03_parallel_export.py) for EXPORT;
- [b04_parallel_import](/examples/b04_parallel_import.py) for IMPORT;
- [b05_parallel_export_import](/examples/b05_parallel_export_import.py) for EXPORT followed by IMPORT using the same child processes;

## Example of EXPORT query executed in Exasol

This is how complete query looks from Exasol perspective.

```sql
EXPORT my_table INTO CSV
AT 'http://27.1.0.30:33601' FILE '000.csv'
AT 'http://27.1.0.31:41733' FILE '001.csv'
AT 'http://27.1.0.32:45014' FILE '002.csv'
AT 'http://27.1.0.33:42071' FILE '003.csv'
AT 'http://27.1.0.34:36669' FILE '004.csv'
AT 'http://27.1.0.35:36794' FILE '005.csv'
WITH COLUMN HEADERS
;
```
