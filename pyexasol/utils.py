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


def get_host_port_list_from_dsn(dsn, shuffle=False):
    """
    Parse dsn, expand it and resolve all IP addresses
    Return list of host:port tuples in natural order (default) or in randomly shuffled order
    Random is useful to guarantee good distribution of workload across all nodes
    """
    idx = dsn.find(':')

    if idx > -1:
        port = int(dsn[idx+1:])
        dsn = dsn[:idx]
    else:
        port = constant.DEFAULT_PORT

    result = []
    regexp = re.compile(r'^(.+?)(\d+)\.\.(\d+)(.*)$')

    for host in dsn.split(','):
        match = regexp.search(host)

        if match:
            for i in range(int(match.group(2)), int(match.group(3)) + 1):
                host = match.group(1) + str(i) + match.group(4)
                result.extend(get_host_port_ip_address_list(host, port))
        else:
            result.extend(get_host_port_ip_address_list(host, port))

    if shuffle:
        random.shuffle(result)
    else:
        result.sort()

    return result


def get_host_port_ip_address_list(host, port):
    hostname, aliaslist, ipaddrlist = socket.gethostbyname_ex(host)
    result = list()

    for ipaddr in ipaddrlist:
        result.append((ipaddr, port))

    return result


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

    # TemporaryDirectory is used instead of NamedTemporaryFile for compatibility with Windows
    with tempfile.TemporaryDirectory(prefix='pyexasol_ssl_') as tempdir:
        tempdir = pathlib.Path(tempdir)

        cert_file = open(tempdir / 'cert', 'wb')
        cert_file.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
        cert_file.close()

        key_file = open(tempdir / 'key', 'wb')
        key_file.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, k))
        key_file.close()

        context = ssl.SSLContext()
        context.verify_mode = ssl.CERT_NONE
        context.load_cert_chain(certfile=cert_file.name, keyfile=key_file.name)

        return context
