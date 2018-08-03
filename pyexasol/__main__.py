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
subp.add_argument('--ppid', help='PID of parent process', default=None)
subp.add_argument('--compression', default=False, help='Enable compression', action='store_true')
subp.add_argument('--encryption', default=False, help='Enable encryption', action='store_true')


subp = subparsers.add_parser('script_output', help='Run script output server and capture output of ALL VMs')
subp.add_argument('--host', default='0.0.0.0', help='Specific address to bind TCPServer (default: 0.0.0.0)')
subp.add_argument('--port', default=0, help='Specific port to bind TCPServer (default: random port)', type=int)
subp.add_argument('--output-dir', help='Directory to write enumerated log files, one file per VM')
subp.add_argument('--ppid', help='PID of parent process', default=None)


subp = subparsers.add_parser('script_debug', help='Run script output server for debugging and display output of ONE VM')
subp.add_argument('--host', default='0.0.0.0', help='Specific address to bind TCPServer (default: 0.0.0.0)')
subp.add_argument('--port', default=0, help='Specific port to bind TCPServer (default: random port)', type=int)

subp = subparsers.add_parser('version', help='Show PyEXASOL version')

args = parser.parse_args()

if args.command == 'http':
    from .http_transport import ExaHTTPProcess

    obj = ExaHTTPProcess(args.host, args.port, args.compression, args.encryption, args.mode, args.ppid)
    obj.init_server()

    obj.send_proxy()
    obj.handle_request()

elif args.command == 'script_output':
    from .script_output import ExaScriptOutputProcess

    obj = ExaScriptOutputProcess(args.host, args.port, args.output_dir, args.ppid)
    obj.init_server_script_mode()

    obj.send_output_address()
    obj.handle_requests_script_mode()

elif args.command == 'script_debug':
    from .script_output import ExaScriptOutputProcess

    obj = ExaScriptOutputProcess(args.host, args.port)
    obj.init_server_debug_mode()

    output_address = obj.get_output_address()
    print(f"ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '{output_address}';", flush=True)

    obj.handle_requests_debug_mode()

elif args.command == 'version':
    from .version import __version__
    print(f'PyEXASOL {__version__}')
