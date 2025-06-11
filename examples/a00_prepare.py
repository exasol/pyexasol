"""
Prepare tables and data for other examples
"""

import datetime
import decimal
import pprint
import random
import string

import examples._config as config
import pyexasol
from pyexasol import ExaTimeDelta

bool_values = [True, False]
user_statuses = ["ACTIVE", "PENDING", "SUSPENDED", "DISABLED"]


def users_generator():
    for i in range(10000):
        yield (
            i,
            "".join(random.choices(string.ascii_uppercase + string.digits, k=10)),
            random_date(),
            random_ts(),
            random.choice(bool_values),
            decimal.Decimal(random.randint(0, 100)) / 100,
            None if random.randint(0, 10) == 10 else random.randint(0, 10000) / 100,
            random.choice(user_statuses),
        )


def payments_generator():
    for i in range(100000):
        gross_amt = decimal.Decimal(random.randint(0, 1000)) / 100

        yield (
            random.randint(1, 10000),
            "-".join(
                [
                    str(random.randint(100, 300)),
                    str(random.randint(100, 300)),
                    str(random.randint(100, 300)),
                ]
            ),
            random_ts(),
            gross_amt,
            gross_amt * decimal.Decimal("0.7"),
        )


def random_date():
    start_date = datetime.date(2018, 1, 1)
    return start_date + datetime.timedelta(random.randint(1, 365))


def random_ts():
    date = random_date()
    time = datetime.time(
        random.randint(0, 23),
        random.randint(0, 59),
        random.randint(0, 59),
        random.randint(0, 999) * 1000,
    )

    return datetime.datetime.combine(date, time)


C = pyexasol.connect(
    dsn=config.dsn,
    user=config.user,
    password=config.password,
    autocommit=False,
    websocket_sslopt=config.websocket_sslopt,
)

# Ensure desired format for timestamps, regardless of SYSTEM settings
C.execute(f"ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6'")

# Create schema if not exist and open it
C.execute("CREATE SCHEMA IF NOT EXISTS {schema!i}", {"schema": config.schema})
C.open_schema(config.schema)

C.execute(
    """
    CREATE OR REPLACE TABLE users
    (
        user_id         DECIMAL(18,0),
        user_name       VARCHAR(255),
        register_dt     DATE,
        last_visit_ts   TIMESTAMP,
        is_female       BOOLEAN,
        user_rating     DECIMAL(10,5),
        user_score      DOUBLE,
        status          VARCHAR(50)
    )
"""
)

C.execute(
    """
CREATE OR REPLACE TABLE payments
(
    user_id         DECIMAL(18,0),
    payment_id      VARCHAR(255),
    payment_ts      TIMESTAMP,
    gross_amt       DECIMAL(15,5),
    net_amt         DECIMAL(15,5)
)
"""
)

C.execute(
    """
CREATE OR REPLACE TABLE parallel_import
(
    user_id         DECIMAL(18,0),
    user_name       VARCHAR(255),
    shard_id        DECIMAL(18,0)
)
"""
)

C.execute(
    """
CREATE OR REPLACE TABLE tab1
(
    id      DECIMAL(9,0)
)
"""
)

C.execute(
    """
CREATE OR REPLACE TABLE tab2
(
    id      DECIMAL(9,0)
)
"""
)

C.execute("CREATE OR REPLACE TABLE users_copy LIKE users")
C.execute("CREATE OR REPLACE TABLE payments_copy LIKE payments")

C.execute(
    "CREATE OR REPLACE TABLE users_copy_reordered LIKE users(register_dt, status, user_id, user_name)"
)

C.import_from_iterable(users_generator(), "users")
C.import_from_iterable(payments_generator(), "payments")

C.execute(
    """
CREATE OR REPLACE TABLE edge_case
(
    dec36_0         DECIMAL(36,0),
    dec36_36        DECIMAL(36,36),
    dbl             DOUBLE,
    bl              BOOLEAN,
    dt              DATE,
    ts              TIMESTAMP,
    var100          VARCHAR(100),
    var2000000      VARCHAR(2000000)
)
"""
)

C.execute(
    """
CREATE OR REPLACE TABLE "camelCaseTable"
(
    "camelCaseColumn!" DECIMAL(18,0)
)
"""
)

C.execute(
    """
CREATE OR REPLACE VIEW users_view
AS
SELECT * FROM users
"""
)

C.execute(
    """
CREATE OR REPLACE JAVA SET SCRIPT "ECHO_JAVA" ("str" VARCHAR(255) UTF8) RETURNS VARCHAR(255) UTF8 AS
%jvmoption -Xms16m -Xmx128m -Xss512k;

import java.util.Arrays;

class ECHO_JAVA {
    static String run(ExaMetadata exa, ExaIterator ctx) throws Exception {
        System.out.println("VM_ID: " + exa.getVmId());
        System.out.println("NODE_ID: " + exa.getNodeId());

        System.out.println("This is custom STDOUT");
        System.err.println("This is custom STDERR");

        System.out.println("Total memory:" + Runtime.getRuntime().totalMemory() / 1024 / 1024 + "Mb");
        System.out.println("Max memory:" + Runtime.getRuntime().maxMemory() / 1024 / 1024 + "Mb");

        String[][] deepArray = new String[][] {{"John", "Mary"}, {"Alice", "Bob"}};
        System.out.println(Arrays.deepToString(deepArray));

        return ctx.getString(0);
    }
}
"""
)

C.execute(
    """
CREATE OR REPLACE JAVA SET SCRIPT "SLEEP_JAVA" ("sleep_interval" DECIMAL(18,0)) RETURNS DECIMAL(18,0) AS
%jvmoption -Xms16m -Xmx128m -Xss512k;

class SLEEP_JAVA {
    static Long run(ExaMetadata exa, ExaIterator ctx) throws Exception {
        try {
            Thread.sleep(1000 * ctx.getLong(0));
        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        return ctx.getLong(0);
    }
}
"""
)

C.execute(
    """
CREATE OR REPLACE TABLE interval_test
(
    id INTEGER,
    from_ts TIMESTAMP,
    to_ts TIMESTAMP,
    expected_timedelta VARCHAR(100)
)
"""
)

interval_timestamps = [
    {
        "from": datetime.datetime(2021, 1, 10),
        "to": datetime.datetime(2021, 1, 9, 17, 35, 11, 297000),
    },
    {"from": datetime.datetime(2021, 1, 10), "to": datetime.datetime(2021, 1, 10)},
    {
        "from": datetime.datetime(2021, 1, 10),
        "to": datetime.datetime(2021, 1, 10, 17, 35, 11, 297000),
    },
    {"from": datetime.datetime(2021, 1, 10), "to": datetime.datetime(2021, 1, 11)},
    {
        "from": datetime.datetime(2021, 1, 10),
        "to": datetime.datetime(2021, 1, 11, 17, 35, 11, 297000),
    },
    {
        "from": datetime.datetime(2021, 1, 10),
        "to": datetime.datetime(2021, 1, 9, 17, 35, 11, 297000),
    },
]

for id, ts in enumerate(interval_timestamps):
    delta = pprint.pformat(ExaTimeDelta.from_timedelta(ts["to"] - ts["from"]))
    C.execute(
        f"INSERT INTO interval_test VALUES ({id}, '{ts['from']}', '{ts['to']}', '{delta}')"
    )

C.commit()
print("Test data was prepared")
