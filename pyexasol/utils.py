import re
import random
import string
import rsa
import base64

from . import constant


def encrypt_password(public_key_pem, password):
    pk = rsa.PublicKey.load_pkcs1(public_key_pem.encode())
    return base64.b64encode(rsa.encrypt(password.encode(), pk)).decode()


def get_random_filename_for_http(compression):
    """
    Return fake file name to be used for Exasol tunneled EXPORT / IMPORT
    """
    filename = ''.join(random.choices(string.ascii_uppercase + string.digits, k=32)) + '.csv'

    if compression:
        filename += '.gz'

    return filename


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
