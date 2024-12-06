"""
Prepare tables and data for performance tests
"""

import sys

import _config as config
import pyodbc
import turbodbc

import pyexasol

print(f"Python: {sys.version}")
print(f"PyEXASOL: {pyexasol.__version__}")
print(f"PyODBC: {pyodbc.version}")
print(f"TurbODBC: {turbodbc.__version__}")
print(f"Creating random data set for tests, {config.number_of_rows} rows")
print(f"Please wait, it may take a few minutes")


C = pyexasol.connect(
    dsn=config.dsn, user=config.user, password=config.password, autocommit=False
)

# Create schema if not exist and open it
C.execute("CREATE SCHEMA IF NOT EXISTS {schema!i}", {"schema": config.schema})
C.open_schema(config.schema)

C.execute(
    """
    CREATE OR REPLACE TABLE p_high_random
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
    CREATE OR REPLACE TABLE p_low_random
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
    INSERT INTO p_high_random
    SELECT FLOOR(RANDOM() * POWER(10, 10)) AS user_id
      , CHR(65 + FLOOR(RANDOM() * 26))
        || CHR(65 + FLOOR(RANDOM() * 26))
        || CHR(65 + FLOOR(RANDOM() * 26))
        || CHR(65 + FLOOR(RANDOM() * 26))
        || CHR(65 + FLOOR(RANDOM() * 26)) AS user_name
      , ADD_DAYS(DATE'2018-01-01', FLOOR(RANDOM() * 365)) AS register_dt
      , ADD_SECONDS(TIMESTAMP'2018-01-01 00:00:00', FLOOR(RANDOM() * 365 * 24 * 60 * 60)) AS last_visit_ts
      , CASE WHEN FLOOR(RANDOM() * 2) = 0 THEN FALSE ELSE TRUE END AS is_female
      , FLOOR(RANDOM() * 100) / 100 AS user_rating
      , RANDOM() AS user_score
      -- Exasol has a weird bug with CASE WHEN combined with RANDOM(), so we have to use REPLACE instead
      , REPLACE(REPLACE(REPLACE(REPLACE(FLOOR(RANDOM() * 4), '0', 'ACTIVE'), '1', 'PENDING'), '2', 'SUSPENDED'), '3', 'DISABLED') AS status
    FROM dual
    CONNECT BY level <= {number_of_rows!d}
""",
    {"number_of_rows": config.number_of_rows},
)

C.execute(
    """
    INSERT INTO p_low_random
    SELECT FLOOR(RANDOM() * 10) AS user_id
      , CHR(65 + FLOOR(RANDOM() * 3))
        || CHR(65 + FLOOR(RANDOM() * 3))
        || CHR(65 + FLOOR(RANDOM() * 3))
        || CHR(65 + FLOOR(RANDOM() * 3))
        || CHR(65 + FLOOR(RANDOM() * 3)) AS user_name
      , ADD_DAYS(DATE'2018-01-01', FLOOR(RANDOM() * 20)) AS register_dt
      , ADD_SECONDS(TIMESTAMP'2018-01-01 00:00:00', FLOOR(RANDOM() * 20) * 24 * 60 * 60) AS last_visit_ts
      , CASE WHEN FLOOR(RANDOM() * 2) = 0 THEN FALSE ELSE TRUE END AS is_female
      , FLOOR(RANDOM() * 5) / 100 AS user_rating
      , FLOOR(RANDOM() * 10) AS user_score
      -- Exasol has a weird bug with CASE WHEN combined with RANDOM(), so we have to use REPLACE instead
      , REPLACE(REPLACE(REPLACE(REPLACE(FLOOR(RANDOM() * 4), '0', 'ACTIVE'), '1', 'PENDING'), '2', 'SUSPENDED'), '3', 'DISABLED') AS status
    FROM dual
    CONNECT BY level <= {number_of_rows!d}
""",
    {"number_of_rows": config.number_of_rows},
)

C.commit()
print("Test data was prepared")
