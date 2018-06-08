import re
import random
import rsa
import base64
import pathlib
import tempfile
import socket
import ssl

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


def get_host_ip_for_enter_parallel(ws_host):
    return socket.gethostbyname(ws_host)


def generate_adhoc_ssl_context():
    """
    Create temporary self-signed certificate for encrypted HTTP transport
    Exasol does not check validity of certificates
    """
    from OpenSSL import crypto

    k = crypto.PKey()
    k.generate_key(crypto.TYPE_RSA, 2048)

    cert = crypto.X509()
    cert.set_serial_number(1)
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(60 * 60 * 24 * 365)

    cert.set_pubkey(k)
    cert.sign(k, 'sha256')

    cert_file = tempfile.NamedTemporaryFile()
    cert_file.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
    cert_file.flush()

    key_file = tempfile.NamedTemporaryFile()
    key_file.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, k))
    key_file.flush()

    context = ssl.SSLContext()
    context.verify_mode = ssl.CERT_NONE
    context.load_cert_chain(certfile=cert_file.name, keyfile=key_file.name)

    cert_file.close()
    key_file.close()

    return context
