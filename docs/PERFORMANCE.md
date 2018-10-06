# Performance tests

Performance of database drivers depends on many factors. Results may vary depending on hardware, network, settings and data set properties. I strongly suggest to make your own performance tests before making any important decisions.

In this sample test I want to compare:

- [PyODBC](https://github.com/mkleehammer/pyodbc)
- [TurbODBC](https://github.com/blue-yonder/turbodbc)
- PyEXASOL

I use Badoo production Exasol cluster for testing:
- 20 nodes
- 800+ CPU cores with hyper-threading
- 14 Tb of RAM
- 10 Gb private network connections
- 1 Gb public network connections

I run three different types of tests:

- Fetching "low random" data set using server in the same data center
- Fetching "high random" data set using server in the same data center
- Fetching data set using local laptop behind VPN and Wifi network (slow network)

I use default number of rows in test table: 10 millions of rows, mixed data types.

I measure total rounded execution time in seconds using `time` command in bash.

## Results

| | [Low random](/performance/_low_random.log) | [High random](/performance/_high_random.log) | [Slow network](/performance/_low_random.log) |
| --- | --- | --- | --- |
| [PyODBC fetchall](/performance/01_pyodbc_fetch.py) | 106 | 107 | - |
| [TurbODBC fetchall](/performance/02_turbodbc_fetch.py) | 56 | 55 | - |
| [PyEXASOL fetchall](/performance/03_pyexasol_fetch.py) | 32 | 39 | 294 |
| [PyEXASOL fetchall+zlib](/performance/03_pyexasol_fetch.py) | - | - | 224 |
| [TurbODBC fetchallnumpy](/performance/04_turbodbc_pandas_numpy.py) | 15 | 15 | - |
| [TurbODBC fetchallarrow](/performance/05_turbodbc_pandas_arrow.py) | 14 | 14 | - |
| [PyEXASOL export_to_pandas](/performance/06_pyexasol_pandas.py) | 11 | 21 | 273 |
| [PyEXASOL export_to_pandas+zlib](/performance/07_pyexasol_pandas_compress.py) | 28 | 53 | 131 |
| [PyEXASOL export_parallel](/performance/08_pyexasol_pandas_parallel.py) | 5 | 7 | - |

### Conclusions

1. PyODBC performance is trash (no surprise).
2. PyEXASOL standard fetching is faster than TurbODBC, but it happens mostly due to less ops with Python objects and due to zip() magic.
3. TurbODBC optimised fetching as numpy or arrow is very efficient and consistent, well done!
4. PyEXASOL export to pandas performance may vary depending on randomness of data set. It highly depends on pandas CSV reader.
5. PyEXASOL fetch and export with ZLIB compression is very good for slow network scenario, but it is bad for fast networks.
6. PyEXASOL parallel export beats everything else because it fully utilizes multiple CPU cores.

## How to run your own test

I strongly encourage you to run your own performance tests. You may use test scripts provided with PyEXASOL as the starting point. Make sure to use your production Exasol cluster for tests. Please do not use Exasol running in Docker locally, it eliminates the whole point of testing.

1. Install PyODBC, TurbODBC, PyEXASOL, pandas.
2. Install Exasol ODBC driver.
3. Download [PyEXASOL source code](https://github.com/badoo/pyexasol/archive/master.zip) and unzip it.
4. Open `/performance/` directory and edit file `\_config.py`. Input your Exasol credentials, set table name and other settings. Set path to ODBC driver.
5. (optional) Run script to prepare data set for testing:
```
python 00_prepare.py
```

That's all. Now you may run examples in any order like common python scripts. E.g.:
```
time python 03_pyexasol_fetch.py
```
