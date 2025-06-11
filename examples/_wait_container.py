import ssl
import time

import examples._config as config
import pyexasol

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
            connection = pyexasol.connect(
                dsn=config.dsn,
                user=config.user,
                password=config.password,
                query_timeout=QUERY_TIMEOUT,
                websocket_sslopt=config.websocket_sslopt,
            )
            connection.execute(
                "CREATE SCHEMA IF NOT EXISTS {schema!i}", {"schema": config.schema}
            )

            return
        except (pyexasol.ExaError, ssl.SSLError) as e:
            print(e)
            time.sleep(SLEEP_TIMEOUT)


def wait_for_java():
    while True:
        try:
            connection = pyexasol.connect(
                dsn=config.dsn,
                user=config.user,
                password=config.password,
                schema=config.schema,
                query_timeout=QUERY_TIMEOUT,
                websocket_sslopt=config.websocket_sslopt,
            )

            connection.execute(TEST_JAVA_UDF)
            connection.execute("SELECT test_java(true)")

            return
        except (pyexasol.ExaError, ssl.SSLError) as e:
            print(e)
            time.sleep(SLEEP_TIMEOUT)


if __name__ == "__main__":
    start_ts = time.time()

    # Wait fo connection server to go online
    C = wait_for_connection()
    print(f"Connection server was started in {time.time() - start_ts:.3f}")

    # Wait for availability of Java in BucketFS
    wait_for_java()
    print(f"Java UDF was ready in {time.time() - start_ts:.3f}")
