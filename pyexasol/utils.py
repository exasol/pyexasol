import re
import random
import rsa
import base64
import pathlib
import tempfile

from . import constant


def get_output_dir_for_statement(output_dir, session_id, stmt_id):
    if output_dir is None:
        output_dir = tempfile.gettempdir()

    full_path = pathlib.Path(output_dir) / f'{session_id}_{stmt_id}'
    full_path.mkdir(parents=True, exist_ok=True)

    return full_path


def encrypt_password(public_key_pem, password):
    pk = rsa.PublicKey.load_pkcs1(public_key_pem.encode())
    return base64.b64encode(rsa.encrypt(password.encode(), pk)).decode()


def get_random_host_port_from_dsn(dsn):
    """
    Parse dsn, unpack it and return hosts in random order
    Random must happen here, otherwise people may put unbalanced load on Exasol nodes
    """
    idx = dsn.find(':')

    if idx > -1:
        port = int(dsn[idx+1:])
        dsn = dsn[:idx]
    else:
        port = constant.DEFAULT_PORT

    res = []
    regexp = re.compile(r'^(.+?)(\d+)\.\.(\d+)(.*)$')

    for part in dsn.split(','):
        match = regexp.search(part)

        if match:
            for i in range(int(match.group(2)), int(match.group(3))):
                res.append((match.group(1) + str(i) + match.group(4), port))
        else:
            res.append((part, port))

    random.shuffle(res)

    return res
