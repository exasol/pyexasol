"""
Simple benchmark for Exasol

"""
import multiprocessing
import os
import pathlib
import pyexasol
import queue
import sys
import time


BUSY_LOOP_DELAY = 0.2
STARTUP_WORKER_DELAY = 1


class ExaBenchWorker(multiprocessing.Process):
    def __init__(self, proc_num, args, sql_text, queue):
        self.proc_num = proc_num
        self.args = args
        self.sql_text = sql_text
        self.queue = queue

        super().__init__()

    def run(self):
        C = pyexasol.connect(dsn=args.dsn, user=args.user, password=args.password, schema=args.schema, autocommit=False)
        self.queue.put(C.session_id())

        sql_comment = f'/* bench_run_id={self.proc_num} */'

        try:
            C.execute(f"ALTER SESSION SET QUERY_CACHE='OFF' {sql_comment}")

            for sql_query in sql_text.split(';'):
                formatted_sql_query = sql_query.strip("\n ")
                if len(formatted_sql_query) == 0:
                    continue

                C.execute(f"{formatted_sql_query} {sql_comment}")

            C.execute(f"COMMIT {sql_comment}")
        except pyexasol.ExaQueryError as e:
            self.queue.put(str(e))
        finally:
            C.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(prog=f'python -m pyexasol_utils.bench [action]')

    parser.add_argument('action', help='Benchmark action (prepare, set1, set2, etc.), OR path to SQL file with commands')
    parser.add_argument('--dsn', default=os.environ.get('EXAHOST', ''), help='Exasol DSN (host:port)')
    parser.add_argument('--user', default=os.environ.get('EXAUID', ''), help='Exasol user name')
    parser.add_argument('--password', default=os.environ.get('EXAPWD', ''), help='Exasol password')
    parser.add_argument('--schema', default=os.environ.get('EXASCHEMA', 'PYEXASOL_BENCH'), help='Exasol schema for tests (default: PYEXASOL_BENCH)')
    parser.add_argument('--scale', default=1.0, type=float, help='Scaling factor for the amount of generated data (default: 1.0)')
    parser.add_argument('--parallel', default=1, type=int, help='Number of parallel executions')

    args = parser.parse_args()

    # Check connectivity first
    C = pyexasol.connect(dsn=args.dsn, user=args.user, password=args.password)

    # Prepare schema
    if args.action == 'prepare':
        if args.parallel > 1:
            raise ValueError("Cannot run [prepare.sql] with more than 1 parallel processes")

        if not C.meta.schema_exists(args.schema):
            C.execute("CREATE SCHEMA {schema!i}", {"schema": args.schema})

    # Get SQL text
    bench_sql_path = pathlib.Path(__file__).parent / "bench_sql" / f"{args.action}.sql"
    bench_sql_path.resolve()

    if not bench_sql_path.is_file():
        bench_sql_path = pathlib.Path(args.action)

        if not bench_sql_path.is_file():
            raise ValueError("Action argument is neither a standard action, nor path to SQL file")

    sql_text = C.format.format(bench_sql_path.read_text(), scale=args.scale)

    # Show startup message
    if args.action == 'prepare':
        print(f"Running [{bench_sql_path.name}] with scale factor of [{args.scale}]", file=sys.stderr)
    else:
        print(f"Running [{bench_sql_path.name}] in [{args.parallel}] parallel processes", file=sys.stderr)

    # Start worker processes
    process_pool = list()
    message_queue = multiprocessing.Queue()

    for i in range(args.parallel):
        proc = ExaBenchWorker(i, args, sql_text, message_queue)
        proc.start()

        process_pool.append(proc)
        time.sleep(STARTUP_WORKER_DELAY)

    while len(process_pool) > 0:
        time.sleep(BUSY_LOOP_DELAY)

        for p in process_pool:
            if not p.is_alive():
                p.join()
                process_pool.remove(p)

                print(f"Finished worker {p.proc_num}", file=sys.stderr)

    # Display errors
    error_cnt = 0
    session_id_list = list()

    while True:
        try:
            msg = str(message_queue.get_nowait())

            if msg.isnumeric():
                session_id_list.append(msg)
            else:
                print(msg, file=sys.stderr)
                error_cnt = error_cnt + 1
        except queue.Empty:
            break

    # Flush statistics
    C.execute("FLUSH STATISTICS")

    if error_cnt > 0:
        print(f"Error count: {error_cnt}", file=sys.stderr)
        exit(0)

    # Output SQL to get results
    run_id_regex_replace = r"REGEXP_REPLACE(sql_text, '(?s)^.*\/\* bench_run_id=(\d+) \*\/.*$', '\1')"

    print(f"""
Durations SQL:
SELECT session_id
    , stmt_id
    , CAST({run_id_regex_replace} AS DECIMAL(9,0)) AS run_id
    , command_name
    , duration
    , avg(duration) OVER (PARTITION BY stmt_id) AS avg_duration
    , sql_text
FROM "$EXA_AUDIT_SQL"
WHERE session_id IN ({','.join(session_id_list)})
    AND stmt_id > 1
ORDER BY 2,3;
    """, file=sys.stderr)

    print(f"""
Profiling details SQL:
SELECT session_id
    , stmt_id
    , command_name
    , part_id
    , iproc
    , duration
    , part_name
    , part_info
    , object_schema
    , object_name
    , object_rows
    , in_rows
    , out_rows
    , remarks
FROM "$EXA_PROFILE_DETAILS_LAST_DAY"
WHERE session_id = {session_id_list[-1]}
    AND stmt_id > 1
ORDER BY 2,4,5;
    """, file=sys.stderr)

    C.close()
