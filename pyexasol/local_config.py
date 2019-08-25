import configparser
import pathlib


class ExaLocalConfig(object):
    """
    Parse local config file
    Prepare arguments for pyexasol.connect() method

    Arguments accepting functions class names and tuples are not supported
    You may pass such arguments directly in code using **kwargs
    """

    default_filename = '.pyexasol.ini'

    arg_types = {
        'autocommit': bool,
        'snapshot_transactions': bool,
        'socket_timeout': int,
        'query_timeout': int,
        'compression': bool,
        'encryption': bool,
        'fetch_dict': bool,
        'fetch_size_bytes': int,
        'lower_ident': bool,
        'quote_ident': bool,
        'verbose_error': bool,
        'debug': bool,
        'udf_output_port': int,
    }

    def __init__(self, config_path=None):
        if config_path:
            self.path = pathlib.Path(config_path)
        else:
            self.path = self.get_default_path()

        if not self.path.exists():
            raise RuntimeError(f"PyEXASOL config file [{self.path}] not found")

        self.parser = configparser.ConfigParser()
        self.parser.read(self.path, encoding='utf-8')

    def get_args(self, section):
        args = dict()

        if not self.parser.has_section(section):
            raise ValueError(f'Section [{section}] in PyEXASOL config file [{self.path}] not found')

        for k in self.parser[section]:
            t = self.get_arg_type(k)

            if t == bool:
                args[k] = self.parser[section].getboolean(k)
            elif t == int:
                args[k] = self.parser[section].getint(k)
            elif t == float:
                args[k] = self.parser[section].getfloat(k)
            else:
                args[k] = self.parser[section].get(k)

        return args

    def get_default_path(self):
        return pathlib.Path.home() / self.default_filename

    def get_arg_type(self, k):
        return self.arg_types.get(k, str)
