import pyexasol
import time
import _config as config

check_timeout = 180
bucket_fs_extra_timeout = 30
start_ts = time.time()

while True:
    try:
        C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password)
        print(f"Exasol was started in {time.time() - start_ts}s")
        print(f"Sleeping extra {bucket_fs_extra_timeout}s to allow BucketFS to startup")

        time.sleep(bucket_fs_extra_timeout)
        break
    except pyexasol.ExaConnectionError:
        if (time.time() - start_ts) > check_timeout:
            raise RuntimeError(f"Exasol did not start in {check_timeout}s, aborting test")

        time.sleep(2)
