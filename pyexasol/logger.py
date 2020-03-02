import logging
import datetime
import pathlib

from . import constant
from .exceptions import ExaRuntimeError


class ExaLogger(logging.Logger):
    def __init__(self, connection, name, level=logging.NOTSET):
        self.connection = connection
        super().__init__(name, level)

    def add_default_handler(self):
        if self.connection.options['debug']:
            if self.connection.options['debug_logdir']:
                logdir = pathlib.Path(self.connection.options['debug_logdir'])

                if not logdir.is_dir():
                    raise ExaRuntimeError(self.connection, 'Not a directory: ' + str(logdir))

                handler = logging.FileHandler(logdir / self._get_log_filename(), encoding='utf-8')
            else:
                handler = logging.StreamHandler()
        else:
            handler = logging.NullHandler()

        formatter = logging.Formatter('%(asctime)s %(message)s')
        formatter.default_msec_format = '%s.%03d'

        handler.setFormatter(formatter)

        self.addHandler(handler)

    def debug_json(self, message, data):
        if self.isEnabledFor(logging.DEBUG):
            json_str = self.connection.json_encode(data, indent=4)

            if len(json_str) > constant.LOGGER_MAX_JSON_LENGTH:
                json_str = f'{json_str[0:constant.LOGGER_MAX_JSON_LENGTH]}\n------ TRUNCATED TOO LONG MESSAGE ------\n'

            self.debug(f'[{message}]\n{json_str}')

    def _get_log_filename(self):
        return f'{self.name}_{datetime.datetime.now().strftime(constant.LOGGER_FILENAME_TIMESTAMP_FORMAT)}.log'

    def __repr__(self):
        level = logging.getLevelName(self.getEffectiveLevel())
        return f'<{self.__class__.__name__} session_id={self.connection.session_id()} level={level}>'
