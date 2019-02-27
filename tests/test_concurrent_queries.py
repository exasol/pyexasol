import time
import unittest
from typing import List

import pyexasol
import examples._config as config
from pyexasol import ExaQueryError
import threading

THREAD_COUNT = 5
QUERY_COUNT_PER_THREAD = 3

class TestConcurrentQueries(unittest.TestCase):
    def execute_udf_sleep(self, C, result):
        try:
            stmt = C.execute("select sleep(1.0);")
            result.append("OK")
        except ExaQueryError as e:
            result.append(e.message)
        except Exception as e:
            result.append(str(e))

    def test_concurrent_executes(self):
        C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, autocommit=False)
        try:
            C.execute("CREATE SCHEMA IF NOT EXISTS {schema!i}", {'schema': config.schema})
            C.open_schema(config.schema)

            stmt = C.execute("""
            CREATE OR REPLACE PYTHON SET SCRIPT SLEEP(test double)
            EMITS (test double) AS
    
            def run(ctx):
                import time
                time.sleep( 10 )
            \
            """)

            result = []
            t1 = threading.Thread(target=self.execute_udf_sleep, args=(C, result))
            t1.start()
            time.sleep(1) # wait for statement in thread to start
            stmt = C.execute("select 1;")
            t1.join()
            self.assertEqual(result[0],"OK")
        finally:
            C.close()

    def export_pandas_udf_sleep(self, C, result):
        try:
            stmt = C.export_to_pandas("select sleep(1.0);")
            result.append("OK")
        except ExaQueryError as e:
            result.append(e.message)
        except Exception as e:
            result.append(str(e))

    def test_concurrent_export_pandas(self):
        C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, autocommit=False)
        try:
            # Create schema if not exist and open it
            C.execute("CREATE SCHEMA IF NOT EXISTS {schema!i}", {'schema': config.schema})
            C.open_schema(config.schema)

            stmt = C.execute("""
            CREATE OR REPLACE PYTHON SET SCRIPT SLEEP(test double)
            EMITS (test double) AS

            def run(ctx):
                import time
                time.sleep( 10 )
            \
            """,{'schema': config.schema})

            result = []
            t1 = threading.Thread(target=self.export_pandas_udf_sleep, args=(C, result))
            t1.start()
            time.sleep(1)  # wait for statement in thread to start
            stmt = C.export_to_pandas("select 1;")
            t1.join()
            self.assertEqual(result[0], "OK")
        finally:
            C.close()


    def execute_udf_sleep_print_id(self, C, thread_id, result):
        try:
            for query_id in range(QUERY_COUNT_PER_THREAD):
                print("Before Execute Thread %s Query %s" % (thread_id, query_id))
                stmt = C.execute("select sleep(1.0);")
                print("After Execute Thread %s Query %s" % (thread_id, query_id))
            result.append("OK")
        except ExaQueryError as e:
            result.append(e.message)
        except Exception as e:
            result.append(str(e))

    def test_many_concurrent_queries(self):
        C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, autocommit=False)
        try:
            C.execute("CREATE SCHEMA IF NOT EXISTS {schema!i}", {'schema': config.schema})
            C.open_schema(config.schema)

            stmt = C.execute("""
            CREATE OR REPLACE PYTHON SET SCRIPT SLEEP(test double)
            EMITS (test double) AS
    
            def run(ctx):
                import time
                time.sleep( 1 )
            \
            """)
            threads = []

            results_of_threads = []
            for thread_id in range(THREAD_COUNT):
                result = []
                results_of_threads.append(result)
                thread = threading.Thread(target=self.execute_udf_sleep_print_id, args=(C, thread_id, result))
                threads.append(thread)
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertSequenceEqual(
                [result[0] for result in results_of_threads],
                ["OK" for thread_id in results_of_threads],
                List
            )
        finally:
            C.close()

if __name__ == '__main__':
    unittest.main()
