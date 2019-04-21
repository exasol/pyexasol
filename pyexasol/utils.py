import base64
import os
import pathlib
import rsa
import ssl
import sys
import tempfile


def get_output_dir_for_statement(output_dir, session_id, stmt_id):
    if output_dir is None:
        output_dir = tempfile.gettempdir()

    full_path = pathlib.Path(output_dir) / f'{session_id}_{stmt_id}'
    full_path.mkdir(parents=True, exist_ok=True)

    return full_path


def encrypt_password(public_key_pem, password):
    pk = rsa.PublicKey.load_pkcs1(public_key_pem.encode())
    return base64.b64encode(rsa.encrypt(password.encode(), pk)).decode()


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


def check_orphaned(initial_ppid):
    """
    Raise exception if current process is "orphaned" (parent process is dead)
    It is useful to stop PyEXASOL HTTP servers from being stuck in process list after parent process was killed

    Currently it works only for POSIX OS
    Please let me know if you know a good way to detect orphans on Windows
    """
    if sys.platform != "win32" and initial_ppid and os.getppid() != int(initial_ppid):
        raise RuntimeError(f"Current process is orphaned, initial ppid={initial_ppid}, current ppid={os.getppid()}")


def get_pid():
    """
    Some special code to handle Windows might be added later to this function
    """
    return os.getpid()
