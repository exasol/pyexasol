"""
Stress Test 1
DO NOT RUN ON PRODUCTION (!)

Apply this command if you have fork-related issues in MacOS:
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

Pushing Exasol server with 100+ sessions running in parallel
"""

import multiprocessing
import pprint
import time

import examples._config as config
import pyexasol

printer = pprint.PrettyPrinter(indent=4, width=140)


class SessionProc(multiprocessing.Process):
    def __init__(self, id, sleep_interval):
        self.id = id
        self.sleep_interval = sleep_interval

        super().__init__()

    def run(self):
        C = pyexasol.connect(
            dsn=config.dsn,
            user=config.user,
            password=config.password,
            schema=config.schema,
        )

        print(
            f"START ID: {self.id:03} S: {C.session_id()} SLEEP: {self.sleep_interval:02}, T:{C.login_time} A:{C.ws_req_time}",
            flush=True,
        )
        C.execute(
            "SELECT sleep_java({sleep_interval!d})",
            {"sleep_interval": self.sleep_interval},
        )
        print(
            f"STOP  ID: {self.id:03} S: {C.session_id()} SLEEP: {self.sleep_interval:02}",
            flush=True,
        )

        C.close()


# This condition is required for 'spawn' multiprocessing implementation (Windows)
# Feel free to skip it for POSIX operating systems
if __name__ == "__main__":
    process_settings = [
        {"amount": 60, "sleep_interval": 60},
        {"amount": 50, "sleep_interval": 20},
        {"amount": 10, "sleep_interval": 30},
    ]

    id = 0
    pool = []

    # Start all sub-processes, try opening 100+ connections, get blocked
    for p in process_settings:
        for i in range(p["amount"]):
            id = id + 1

            proc = SessionProc(id, p["sleep_interval"])
            proc.start()

            pool.append(proc)
            time.sleep(0.2)

    # Wait for all processes to unblock and eventually finish
    for proc in pool:
        proc.join()
