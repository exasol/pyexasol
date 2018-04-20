"""
Example 0
Prepare tables and data for other examples
"""

import pyexasol as E
import _config as config

import datetime
import random
import string
import decimal

bool_values = [True, False]


def users_generator():
    for i in range(10000):
        yield (i,
               ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),
               random_date(),
               random_ts(),
               random.choice(bool_values),
               decimal.Decimal(random.randint(0, 100)) / 100,
               None if random.randint(0, 10) == 10 else random.randint(0, 10000) / 100
              )


def payments_generator():
    for i in range(100000):
        gross_amt = decimal.Decimal(random.randint(0, 1000)) / 100

        yield (random.randint(1, 10000),
               '-'.join([str(random.randint(100, 300)), str(random.randint(100, 300)), str(random.randint(100, 300))]),
               random_ts(),
               gross_amt,
               gross_amt * decimal.Decimal('0.7')
              )


def random_date():
    start_date = datetime.date(2018, 1, 1)
    return start_date+datetime.timedelta(random.randint(1, 365))


def random_ts():
    date = random_date()
    time = datetime.time(random.randint(0, 23), random.randint(0,59), random.randint(0,59), random.randint(0, 999) * 1000)

    return datetime.datetime.combine(date, time)


C = E.connect(dsn=config.dsn, user=config.user, password=config.password, autocommit=False)

# Create schema if not exist and make it default
C.execute("CREATE SCHEMA IF NOT EXISTS {}".format(config.schema))
C.execute("OPEN SCHEMA {}".format(config.schema))

C.execute("""
    CREATE OR REPLACE TABLE users
    (
        user_id         DECIMAL(18,0),
        user_name       VARCHAR(255),
        register_dt     DATE,
        last_visit_ts   TIMESTAMP,
        is_female       BOOLEAN,
        user_rating     DECIMAL(10,5),
        user_score      DOUBLE
    )
""")

C.execute("""
CREATE OR REPLACE TABLE payments
(
    user_id         DECIMAL(18,0),
    payment_id      VARCHAR(255),
    payment_ts      TIMESTAMP,
    gross_amt       DECIMAL(15,5),
    net_amt         DECIMAL(15,5)
)
""")

C.execute("CREATE OR REPLACE TABLE users_copy LIKE users")
C.execute("CREATE OR REPLACE TABLE payments_copy LIKE payments")

C.import_from_iterable(users_generator(), 'users')
C.import_from_iterable(payments_generator(), 'payments')

C.execute("""
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
""")

C.execute("""
CREATE OR REPLACE VIEW users_view
AS
SELECT * FROM users
""")

C.commit()
print("Test data was prepared")
