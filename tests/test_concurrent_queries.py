import time
import unittest
from typing import List

import pyexasol
import examples._config as config
import threading
import pandas as pd
import numpy as np

from pyexasol.exceptions import ExaConcurrencyError

THREAD_COUNT = 5
QUERY_COUNT_PER_THREAD = 3

class TestConcurrentQueries(unittest.TestCase):
    def execute_udf_sleep(self, C, result):
        try:
            stmt = C.execute("select sleep(1.0);")
            result.append("ERROR: Other thread used connection")
        except ExaConcurrencyError as e:
            result.append("OK")
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
            result.append("ERROR: Other thread used connection")
        except ExaConcurrencyError as e:
            result.append("OK")
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
            """)

            result = []
            t1 = threading.Thread(target=self.export_pandas_udf_sleep, args=(C, result))
            t1.start()
            time.sleep(1)  # wait for statement in thread to start
            stmt = C.export_to_pandas("select 1;")
            t1.join()
            self.assertEqual(result[0], "OK")
        finally:
            C.close()

    def import_pandas(self, C, df, result):
        try:
            stmt = C.import_from_pandas(df,"test")
            result.append("ERROR: Other thread used connection")
        except ExaConcurrencyError as e:
            result.append("OK")
        except Exception as e:
            result.append(str(e))

    def test_concurrent_import_pandas(self):
        C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, autocommit=False)
        try:
            # Create schema if not exist and open it
            C.execute("CREATE SCHEMA IF NOT EXISTS {schema!i}", {'schema': config.schema})
            C.open_schema(config.schema)

            stmt = C.execute(f"""
            CREATE OR REPLACE TABLE test(
              {",".join(["c%s double"%i for i in range(10)])}
            );
            """)
            df = pd.DataFrame(np.zeros(shape=[100000,10]),columns=["c%s"%i for i in range(10)])

            result = []
            t1 = threading.Thread(target=self.import_pandas, args=(C, df, result))
            t1.start()
            time.sleep(1)  # wait for statement in thread to start
            stmt = C.import_from_pandas(df,"test")
            t1.join()
            self.assertEqual(result[0], "OK")
            self.assertEqual(C.execute("select count(1) from test;").fetchval(),100000)
        finally:
            C.close()

if __name__ == '__main__':
    unittest.main()
