"""
Standalone functions to be executed as python module
Usage: python -m pyexasol <command> <options>
"""

import argparse
from .script_output import ExaScriptOutput
from .version import __version__


parser = argparse.ArgumentParser(prog=f'python -m pyexasol')
subparsers = parser.add_subparsers(title=None, dest='command', metavar='<command>')
subparsers.required = True

subp = subparsers.add_parser('script_debug', help='Run script output server for debugging')
subp.add_argument('--host', default=None, help='Specific address to bind TCPServer (default: 0.0.0.0)')
subp.add_argument('--port', default=None, help='Specific port to bind TCPServer (default: random port)')

subp = subparsers.add_parser('version', help='Show PyEXASOL version')

args = parser.parse_args()

if args.command == 'script_debug':
    obj = ExaScriptOutput(args.host, args.port)

    output_address = obj.init_debug_mode()
    print(f"ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '{output_address}';")

    obj.wait_debug_mode()
elif args.command == 'version':
    print(f'PyEXASOL {__version__}')
