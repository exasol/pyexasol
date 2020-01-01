"""
Fake TCP Server to process HTTP transport requests.

Instead of listening for incoming connections, it connects to Exasol and uses proxy magic.
This approach allows to bypass various connectivity problems (e.g. firewall).
"""

import os
import socketserver
import struct
import sys
import zlib


class ExaTCPServer(socketserver.TCPServer):
    timeout = 5

    def __init__(self, *args, **kwargs):
        self.proxy_host = None
        self.proxy_port = None

        self.compression = kwargs.pop('compression', False)
        self.encryption = kwargs.pop('encryption', False)

        self.total_clients = 0

        super().__init__(*args, **kwargs)

    def server_bind(self):
        """ Special Exasol packet to establish tunneling and return proxy host and port which can be used in query """
        self.socket.connect(self.server_address)
        self.socket.sendall(struct.pack("iii", 0x02212102, 1, 1))
        _, port, host = struct.unpack("ii16s", self.socket.recv(24))

        self.proxy_host = host.replace(b'\x00', b'').decode()
        self.proxy_port = port

        if self.encryption:
            context = self.generate_adhoc_ssl_context()
            self.socket = context.wrap_socket(self.socket, server_side=True, do_handshake_on_connect=False)

    def server_activate(self): pass

    def get_request(self):
        return self.socket, self.server_address

    def shutdown_request(self, request): pass

    def close_request(self, request): pass

    @staticmethod
    def check_orphaned(initial_ppid):
        """
        Raise exception if current process is "orphaned" (parent process is dead)
        It is useful to stop PyEXASOL HTTP servers from being stuck in process list after parent process was killed

        Currently it works only for POSIX OS
        Please let me know if you know a good way to detect orphans on Windows
        """
        current_ppid = os.getppid()

        if sys.platform != "win32" and initial_ppid and current_ppid != initial_ppid:
            raise RuntimeError(f"Current process is orphaned, initial ppid={initial_ppid}, current ppid={current_ppid}")

    @staticmethod
    def generate_adhoc_ssl_context():
        """
        Create temporary self-signed certificate for encrypted HTTP transport
        Exasol does not check validity of certificates
        """
        from OpenSSL import crypto

        import pathlib
        import ssl
        import tempfile

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


class ExaHTTPRequestHandler(socketserver.StreamRequestHandler):
    """
    Oversimplified request handler to reduce "import" overhead
    """
    default_headers = b'HTTP/1.1 200 OK\r\n' \
                      b'Connection: close\r\n' \
                      b'\r\n'

    def handle(self):
        self.server.total_clients += 1

        # Extract method from the first header
        method = str(self.rfile.readline(), 'iso-8859-1').split()[0]

        # Skip all other headers
        while self.rfile.readline() != b'\r\n':
            pass

        if method == 'PUT':
            self.method_put()
        else:
            self.method_get()

    def method_put(self):
        # Compressed data loop
        if self.server.compression:
            d = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)

            while True:
                data = self.read_chunk()

                if data is None:
                    sys.stdout.buffer.write(d.flush())
                    break

                sys.stdout.buffer.write(d.decompress(data))

        # Normal data loop
        else:
            while True:
                data = self.read_chunk()

                if data is None:
                    break

                sys.stdout.buffer.write(data)

        sys.stdout.buffer.close()

        self.wfile.write(self.default_headers)

    def read_chunk(self):
        hex_length = self.rfile.readline().rstrip()

        if len(hex_length) == 0:
            chunk_len = 0
        else:
            chunk_len = int(hex_length, 16)

        if chunk_len == 0:
            return None

        data = self.rfile.read(chunk_len)

        if self.rfile.read(2) != b'\r\n':
            raise RuntimeError('Got wrong chunk delimiter in HTTP')

        return data

    def method_get(self):
        self.wfile.write(self.default_headers)

        # Compressed data loop
        if self.server.compression:
            c = zlib.compressobj(level=1, wbits=16 + zlib.MAX_WBITS)

            while True:
                data = sys.stdin.buffer.read(65536)

                if data is None or len(data) == 0:
                    self.wfile.write(c.flush(zlib.Z_FINISH))
                    break

                self.wfile.write(c.compress(data))

        # Normal data loop
        else:
            while True:
                data = sys.stdin.buffer.read(65536)

                if data is None or len(data) == 0:
                    break

                self.wfile.write(data)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(prog='python -m pyexasol_utils.http_transport'
                                     , description='TCP server for PyEXASOL HTTP transport')

    parser.add_argument('--host', help='Exasol host')
    parser.add_argument('--port', help='Exasol port', type=int)
    parser.add_argument('--mode', help='EXPORT or IMPORT')
    parser.add_argument('--ppid', help='PID of parent process', default=0, type=int)
    parser.add_argument('--compression', default=False, help='Enable compression', action='store_true')
    parser.add_argument('--encryption', default=False, help='Enable encryption', action='store_true')

    args = parser.parse_args()

    # Start TCP server
    server = ExaTCPServer((args.host, args.port)
                          , ExaHTTPRequestHandler
                          , compression=args.compression, encryption=args.encryption)

    # Send proxy string to the main process
    sys.stdout.buffer.write(f'{server.proxy_host}:{server.proxy_port}\n'.encode())
    sys.stdout.buffer.flush()

    # Handle exactly one connection
    while server.total_clients == 0:
        server.handle_request()
        server.check_orphaned(args.ppid)

    server.server_close()
