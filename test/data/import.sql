DROP SCHEMA IF EXISTS PYEXASOL_TEST CASCADE;
CREATE SCHEMA PYEXASOL_TEST;

CREATE OR REPLACE TABLE PYEXASOL_TEST.USERS
(
    user_id         DECIMAL(18,0),
    user_name       VARCHAR(255),
    register_dt     DATE,
    last_visit_ts   TIMESTAMP,
    is_female       BOOLEAN,
    user_rating     DECIMAL(10,5),
    user_score      DOUBLE,
    status          VARCHAR(50)
);

IMPORT INTO PYEXASOL_TEST.USERS FROM LOCAL CSV FILE 'users.csv' COLUMN SEPARATOR = ',' ROW SEPARATOR = 'CRLF';


CREATE OR REPLACE TABLE PAYMENTS 
(
    user_id         DECIMAL(18,0),
    payment_id      VARCHAR(255),
    payment_ts      TIMESTAMP,
    gross_amt       DECIMAL(15, 5),
    net_amt         DECIMAL(15, 5)
);

IMPORT INTO PYEXASOL_TEST.PAYMENTS FROM LOCAL CSV FILE 'payments.csv' COLUMN SEPARATOR = ',' ROW SEPARATOR = 'CRLF';

