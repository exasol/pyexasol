# HTTP transport (parallel)

It is possible to run [HTTP Transport](/docs/HTTP_TRANSPORT.md) in parallel. Workload may be distributed across multiple CPU cores and even across multiple servers.

## How it works on high level

1. Parent process opens main connection to Exasol and spawns multiple child processes.
2. Each child process connects to individual Exasol node using [`http_transport()`](/docs/REFERENCE.md#http_transport) function, gets proxy `host:port` string and sends it to parent process.
3. Parent process collects list of proxies from child processes and runs [`export_parallel()`](/docs/REFERENCE.md#export_parallel) or [`import_parallel()`](/docs/REFERENCE.md#import_parallel) function to execute SQL query.
4. Each child process executes callback function and gets or sends chunk of data from or to Exasol.
5. Parent process waits for SQL query and child processes to finish.

![Parallel export](/docs/img/parallel_export.png)

Please note that PyEXASOL does not provide any specific way to send proxy strings from child processes to parent process. You are free to choose your own way of inter-process communication. For example, you may use [multiprocessing.Pipe](https://docs.python.org/3/library/multiprocessing.html?highlight=Pipes#exchanging-objects-between-processes).

## Example

Please see [example_14](/examples/14_parallel_export.py) for EXPORT.

Please see [example_20](/examples/20_parallel_import.py) for IMPORT.

## Example of EXPORT query executed in Exasol

This is how it looks from Exasol perspective.

```sql
EXPORT my_table INTO CSV
AT 'http://27.1.0.30:33601' FILE '000.csv'
AT 'http://27.1.0.31:41733' FILE '001.csv'
AT 'http://27.1.0.32:45014' FILE '002.csv'
AT 'http://27.1.0.33:42071' FILE '003.csv'
AT 'http://27.1.0.34:36669' FILE '004.csv'
AT 'http://27.1.0.35:36794' FILE '005.csv'
```

## Known problems and limitations

- ~~Parallel `IMPORT` is not fully supported right now due to Exasol "N+1 connection" problem described in [`IDEA-370`](https://www.exasol.com/support/browse/IDEA-370) and [`EXA-17055`](https://www.exasol.com/support/browse/EXA-17055). It is possible to make it work using multiple hacks, but code becomes very ugly. Please let me know if you really need it and feel free to upvote relevant issues in Exasol tracker.~~

IMPORT problem was resolved starting from PyEXASOL 0.3.26. Please upgrade PyEXASOL to take full advantage of this feature.
