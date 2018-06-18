"""
Standalone functions to be executed as python module
Usage: python -m pyexasol <command> <options>
"""

import argparse


parser = argparse.ArgumentParser(prog=f'python -m pyexasol')
subparsers = parser.add_subparsers(title=None, dest='command', metavar='<command>')
subparsers.required = True


subp = subparsers.add_parser('http', help='Run proxy TCP server for HTTP transport')
subp.add_argument('--host', help='Exasol host')
subp.add_argument('--port', help='Exasol port', type=int)
subp.add_argument('--mode', help='EXPORT or IMPORT')
subp.add_argument('--compression', default=False, help='Enable compression', action='store_true')
subp.add_argument('--encryption', default=False, help='Enable encryption', action='store_true')


subp = subparsers.add_parser('script_debug', help='Run script output server for debugging')
subp.add_argument('--host', default=None, help='Specific address to bind TCPServer (default: 0.0.0.0)')
subp.add_argument('--port', default=None, help='Specific port to bind TCPServer (default: random port)')

subp = subparsers.add_parser('version', help='Show PyEXASOL version')

args = parser.parse_args()

if args.command == 'http':
    from .http_transport import ExaHTTPProcess

    obj = ExaHTTPProcess(args.host, args.port, args.compression, args.encryption, args.mode)
    obj.init_server()

    obj.send_proxy()
    obj.handle_request()

elif args.command == 'script_debug':
    from .script_output import ExaScriptOutput

    obj = ExaScriptOutput(args.host, args.port)
    output_address = obj.init_debug_mode()

    print(f"ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '{output_address}';", flush=True)

    obj.wait_debug_mode()
elif args.command == 'version':
    from .version import __version__
    print(f'PyEXASOL {__version__}')
