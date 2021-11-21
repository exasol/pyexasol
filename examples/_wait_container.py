import _config as config
import pyexasol
import time


SLEEP_TIMEOUT = 5
QUERY_TIMEOUT = 10

TEST_JAVA_UDF = """
CREATE OR REPLACE JAVA SET SCRIPT test_java(val BOOLEAN) RETURNS BOOLEAN AS
%jvmoption -Xms16m -Xmx128m -Xss512k;

class TEST_JAVA {
    static Boolean run(ExaMetadata exa, ExaIterator ctx) throws Exception {
        return ctx.getBoolean(0);
    }
}
"""


def wait_for_connection():
    while True:
        try:
            return pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, query_timeout=QUERY_TIMEOUT)
        except pyexasol.ExaConnectionFailedError as e:
            print(e)
            time.sleep(SLEEP_TIMEOUT)


def wait_for_java(connection: pyexasol.ExaConnection):
    while True:
        try:
            connection.execute(TEST_JAVA_UDF)
            connection.execute("SELECT test_jav1a(true)")

            return
        except pyexasol.ExaQueryError as e:
            print(e)
            time.sleep(SLEEP_TIMEOUT)


if __name__ == '__main__':
    start_ts = time.time()

    # Wait fo connection server to go online
    C = wait_for_connection()
    print(f"Connection server was started in {time.time() - start_ts:.3f}")

    # Create schema if not exist and open it
    C.execute("CREATE SCHEMA IF NOT EXISTS {schema!i}", {'schema': config.schema})
    C.open_schema(config.schema)

    # Wait for availability of Java in BucketFS
    wait_for_java(C)
    print(f"Java UDF was ready in {time.time() - start_ts:.3f}")
