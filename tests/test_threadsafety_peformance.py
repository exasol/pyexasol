import pyexasol
import examples._config as config
import pandas as pd
import numpy as np


def test_peformance_execute(threadsafety, query_count):
    C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, autocommit=False,
                         threadsafety=threadsafety)
    try:
        for i in range(query_count):
            C.execute("select * from %s.test;" % config.schema).fetchall()
    finally:
        C.close()


def test_peformance_export_to_pandas(threadsafety, query_count):
    C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, autocommit=False,
                         threadsafety=threadsafety)
    try:
        for i in range(query_count):
            C.export_to_pandas("select * from %s.test;" % config.schema)
    finally:
        C.close()


def setup():
    C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, autocommit=False)
    try:
        # Create schema if not exist and open it
        C.execute("CREATE SCHEMA IF NOT EXISTS {schema!i}", {'schema': config.schema})
        C.open_schema(config.schema)

        stmt = C.execute(f"""
        CREATE OR REPLACE TABLE test(
          {",".join(["c%s double" % i for i in range(10)])}
        );
        """)
        # insert 10 rows, we only use this few, because we want to measure the overhead of the lock
        df = pd.DataFrame(np.zeros(shape=[10, 10]), columns=["c%s" % i for i in range(10)])
        stmt = C.import_from_pandas(df, (config.schema, "test"))
        C.commit()
    finally:
        C.close()


NUMBER_OF_EXECUTIONS = 1

NUMBER_OF_REPEATS_FOR_EXECUTE = 10

NUMBER_OF_REPEATS_FOR_EXPORT = 5

NUMBER_OF_QUERIES_FOR_EXECUTE = 1000

NUMBER_OF_QUERIES_FOR_EXPORT = 5

NUMBER_OF_TIMINGS = 3

if __name__ == "__main__":
    import timeit

    setup()
    setup = "from __main__ import test_peformance_execute, test_peformance_export_to_pandas"
    for i in range(NUMBER_OF_TIMINGS):
        print("execute", "threadsafety==1 => on",
              sum(timeit.repeat(
                  "test_peformance_execute(1,%s)" % NUMBER_OF_QUERIES_FOR_EXECUTE,
                  setup=setup,
                  repeat=NUMBER_OF_REPEATS_FOR_EXECUTE,
                  number=NUMBER_OF_EXECUTIONS))
              / (NUMBER_OF_EXECUTIONS * NUMBER_OF_REPEATS_FOR_EXECUTE))
        print("execute", "threadsafety==0 => off",
              sum(timeit.repeat(
                  "test_peformance_execute(0,%s)" % NUMBER_OF_QUERIES_FOR_EXECUTE,
                  setup=setup,
                  repeat=NUMBER_OF_REPEATS_FOR_EXECUTE,
                  number=NUMBER_OF_EXECUTIONS))
              / (NUMBER_OF_EXECUTIONS * NUMBER_OF_REPEATS_FOR_EXECUTE))
        print("export_to_pandas", "threadsafety==1 => on",
              sum(timeit.repeat(
                  "test_peformance_export_to_pandas(1,%s)" % NUMBER_OF_QUERIES_FOR_EXPORT,
                  setup=setup,
                  repeat=NUMBER_OF_REPEATS_FOR_EXECUTE,
                  number=NUMBER_OF_EXECUTIONS))
              / (NUMBER_OF_EXECUTIONS * NUMBER_OF_REPEATS_FOR_EXECUTE))
        print("export_to_pandas", "threadsafety==0 => off",
              sum(timeit.repeat(
                  "test_peformance_export_to_pandas(0,%s)" % NUMBER_OF_QUERIES_FOR_EXPORT,
                  setup=setup,
                  repeat=NUMBER_OF_REPEATS_FOR_EXECUTE,
                  number=NUMBER_OF_EXECUTIONS))
              / (NUMBER_OF_EXECUTIONS * NUMBER_OF_REPEATS_FOR_EXECUTE))
