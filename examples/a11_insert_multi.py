"""
INSERT multiple rows using prepared statements
"""

import pyexasol
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)


C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)

# Insert from list
C.execute("TRUNCATE TABLE users_copy")

all_users = C.execute("SELECT * FROM users").fetchall()
st = C.ext.insert_multi("users_copy", all_users)

print(f'INSERTED {st.rowcount()} rows in {st.execution_time}s')


# Insert from generator with shuffled order of columns
C.execute("TRUNCATE TABLE users_copy")


def users_generator():
    for i in range(10000):
        yield (i, 'abcabc', '2019-01-01 01:02:03', '2019-02-01', 'PENDING', False)


st = C.ext.insert_multi("users_copy", users_generator(), columns=['user_id', 'user_name', 'last_visit_ts', 'register_dt', 'status', 'is_female'])

print(f'INSERTED {st.rowcount()} rows in {st.execution_time}s')


# Attempt to insert empty data leads to failure, unlike .import_from_iterable()
try:
    C.ext.insert_multi("users_copy", [])
except pyexasol.ExaRuntimeError as e:
    print(e)
