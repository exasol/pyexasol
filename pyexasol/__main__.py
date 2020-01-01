"""
Usage: python -m pyexasol <command> <options>
"""

import argparse


parser = argparse.ArgumentParser(prog=f'python -m pyexasol')
subparsers = parser.add_subparsers(title=None, dest='command', metavar='<command>')
subparsers.required = True

subparsers.add_parser('script_output')
subparsers.add_parser('script_debug')

args = parser.parse_args()

if args.command == 'script_output':
    raise RuntimeError("'-m pyexasol script_output' is no longer available, "
                       "please use '-m pyexasol_utils.script_output' instead")

elif args.command == 'script_debug':
    raise RuntimeError("'-m pyexasol script_debug' is no longer available, "
                       "please use '-m pyexasol_utils.script_output' instead")
