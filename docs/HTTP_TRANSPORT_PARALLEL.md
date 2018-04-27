# HTTP transport (parallel)

It is possible to run [HTTP Transport](/docs/HTTP_TRANSPORT.md) in parallel. Workload may be distributed across multiple CPU cores and even across multiple servers.

## How it works on high level

1. Parent process opens main connection to Exasol and spawns multiple child processes.
2. Each child process connects to individual Exasol node using [`http_transport()`](/docs/REFERENCE.md#http_transport) function, gets proxy `host:port` string and sends it to parent process.
3. Parent process collects list of proxies from child processes and runs [`export_parallel()`](/docs/REFERNCE.md#export_parallel) function to execute SQL query.
4. Each child process gets chunk of data from Exasol and executes callback function.
5. Parent process waits for SQL query and child processes to finish.

![Parallel export](/docs/img/parallel_export.png)

Please note that PyEXASOL does not provide any specific way to send proxy strings from child processes to parent process. You are free to choose your own way of inter-process communication. For example, you may use [multiprocessing.Pipe](https://docs.python.org/3/library/multiprocessing.html?highlight=Pipes#exchanging-objects-between-processes).

## Example

Please see [example_14](/examples/14_parallel_export.py).

## Known problems and limitations

- Parallel `IMPORT` is not fully supported right now due to Exasol "N+1 connection" problem described in [`IDEA-370`](https://www.exasol.com/support/browse/IDEA-370) and [`EXA-17055`](https://www.exasol.com/support/browse/EXA-17055). It is possible to make it work using multiple hacks, but code becomes very ugly. Please let me know if you really need it and feel free to upvote relevant issues in Exasol tracker.
