from __future__ import annotations

import hashlib
import socket
import socketserver
import struct
import sys
from ssl import SSLContext


class ExaTunnelServer(socketserver.TCPServer):
    """
    Generic server for an Exasol HTTP tunnel.

    Establishes the tunnel with Exasol, optionally wraps it in ad-hoc TLS and exposes
    the resulting Exasol address. Subclasses define how requests are handled and
    which data they serve.
    """

    exa_address_ipaddr: str
    exa_address_port: int
    exa_address_public_key: str | None = None

    total_clients: int = 0
    is_terminated: bool = False

    timeout: int | None = 1

    def __init__(self, *args, **kwargs):
        self.encryption: bool = kwargs.pop("encryption", True)
        super().__init__(*args, **kwargs)

    def server_bind(self):
        self.set_sock_opts()

        """ Special Exasol packet to establish tunneling and return an internal Exasol address, which can be used in a query """
        self.socket.connect(self.server_address)
        self.socket.sendall(struct.pack("iii", 0x02212102, 1, 1))
        _, port, ipaddr = struct.unpack("ii16s", self.socket.recv(24))

        self.exa_address_ipaddr = ipaddr.replace(b"\x00", b"").decode()
        self.exa_address_port = port

        if self.encryption:
            context, public_key_sha = self.generate_adhoc_ssl_context()
            self.socket = context.wrap_socket(
                self.socket, server_side=True, do_handshake_on_connect=False
            )
            self.exa_address_public_key = public_key_sha

    def server_activate(self):
        # No listen(): the socket is an outgoing connection to Exasol (see server_bind),
        # so there is nothing to listen on.
        pass

    def get_request(self):
        return self.socket, self.server_address

    def shutdown_request(self, request):
        # Intentionally a no-op: the request is the tunnel socket itself, which is
        # reused for the whole transfer and is closed by the server, not per request.
        pass

    def close_request(self, request):
        # Intentionally a no-op: closing here would tear down the tunnel socket
        # (see shutdown_request).
        pass

    def set_sock_opts(self):
        # only large packets are expected for HTTP transport
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # default keep-alive once a minute, 5 probes
        keepidle = 60
        keepintvl = 60
        keepcnt = 5

        if sys.platform.startswith("linux"):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, keepidle)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, keepintvl)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, keepcnt)

        elif sys.platform.startswith("darwin"):
            # TCP_KEEPALIVE = 0x10
            # https://bugs.python.org/issue34932
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.socket.setsockopt(socket.IPPROTO_TCP, 0x10, keepidle)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, keepintvl)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, keepcnt)

        elif sys.platform.startswith("win"):
            self.socket.ioctl(
                socket.SIO_KEEPALIVE_VALS, (1, keepidle * 1000, keepintvl * 1000)
            )

    @staticmethod
    def generate_adhoc_ssl_context() -> tuple[SSLContext, str]:
        """
        Create temporary self-signed certificate for encrypted HTTP transport
        Exasol does not check validity of certificates
        """
        from base64 import b64encode
        from datetime import (
            datetime,
            timedelta,
            timezone,
        )
        from pathlib import Path
        from ssl import (
            CERT_NONE,
            PROTOCOL_TLS_SERVER,
        )
        from tempfile import TemporaryDirectory

        from cryptography import x509
        from cryptography.hazmat.primitives import (
            hashes,
            serialization,
        )
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID

        key_pair = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )

        # For a self-signed certificate, subject and issuer are identical.
        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COUNTRY_NAME, "DE"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Franconia"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "Nuremberg"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Exasol AG"),
                x509.NameAttribute(NameOID.COMMON_NAME, "exasol.com"),
            ]
        )
        today = datetime.now(timezone.utc)
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key_pair.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(today)
            .not_valid_after(today + timedelta(days=365))
            .sign(key_pair, hashes.SHA256())
        )

        der_data = key_pair.public_key().public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        sha256_hash = hashlib.sha256(der_data).digest()
        base64_encoded = b64encode(sha256_hash)
        public_key_sha256 = base64_encoded.decode("utf-8")

        # TemporaryDirectory is used instead of NamedTemporaryFile for compatibility with Windows
        with TemporaryDirectory(prefix="pyexasol_ssl_") as tempdir:
            directory = Path(tempdir)

            cert_file = open(directory / "cert", "wb")
            cert_file.write(cert.public_bytes(serialization.Encoding.PEM))
            cert_file.close()

            key_file = open(directory / "key", "wb")
            key_file.write(
                key_pair.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )
            key_file.close()

            context = SSLContext(PROTOCOL_TLS_SERVER)
            context.verify_mode = CERT_NONE
            context.load_cert_chain(certfile=cert_file.name, keyfile=key_file.name)

            return context, public_key_sha256
