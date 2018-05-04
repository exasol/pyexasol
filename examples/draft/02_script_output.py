"""
Draft example 2
Script output server

This test can only work with correct firewall / network settings and sufficient parallelism
"""

import pyexasol as E

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

config = {
    'dsn': 'exasolpool1..5.mlan',
    'user': 'sys',
    'password': 'xxx',
    'schema': 'xxx',
    'table_name': 'xxx',
    'num_parallel': 800
}

C = E.connect(dsn=config['dsn'], user=config['user'], password=config['password'], schema=config['schema'])

params = {
    'table_name': config['table_name'],
    'num_parallel': config['num_parallel'],
}

stmt, output_dir = C.execute_with_udf_output("SELECT wr.echo_java(profile_user_id) FROM {table_name!i} GROUP BY CEIL(RANDOM() * {num_parallel!d})", params)

printer.pprint(stmt.fetchall())
printer.pprint(sorted(list(output_dir.glob('*.log'))))
