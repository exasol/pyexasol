import string
import re


class ExaFormatter(string.Formatter):
    safe_ident_regexp = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
    safe_decimal_regexp = re.compile(r'^(\+|-)?[0-9]+(\.[0-9]+)?$')
    safe_float_regexp = re.compile(r'^(\+|-)?[0-9]+(\.[0-9]+((e|E)(\+|-)[0-9]+)?)?$')

    def __init__(self, connection):
        self.connection = connection

        self.conversions = {
            's': self.quote,
            'd': self.safe_decimal,
            'f': self.safe_float,
            'i': self.safe_ident,
            'q': self.quote_ident,
            'r': str
        }

        self.default_conversion = self.quote

    def format_field(self, value, format_spec):
        if format_spec != '':
            raise ValueError("format_spec is disabled for ExaFormatter")

        return value

    def convert_field(self, value, conversion):
        if conversion is None:
            return self.default_conversion(value)

        if conversion in self.conversions:
            return self.conversions[conversion](value)

        raise ValueError("Unknown conversion specifier {0!s}".format(conversion))

    @classmethod
    def escape(cls, val):
        return str(val).replace("'", "''")

    @classmethod
    def escape_ident(cls, val):
        return str(val).replace('"', '""')

    @classmethod
    def escape_like(cls, val):
        return cls.escape(val).replace('\\', '\\\\').replace('%', '\%').replace('_', '\_')

    @classmethod
    def quote(cls, val):
        if val is None:
            return 'NULL'

        return f"'{cls.escape(val)}'"

    @classmethod
    def quote_ident(cls, val):
        if isinstance(val, tuple):
            return '.'.join([cls.quote_ident(x) for x in val])

        return f'"{cls.escape_ident(val)}"'

    @classmethod
    def safe_ident(cls, val):
        if isinstance(val, tuple):
            return '.'.join([cls.safe_ident(x) for x in val])

        val = str(val)

        if not cls.safe_ident_regexp.match(val):
            raise ValueError(f'Value [{val}] is not safe identifier')

        return val

    @classmethod
    def safe_float(cls, val):
        if val is None:
            return 'NULL'

        val = str(val)

        if not cls.safe_float_regexp.match(val):
            raise ValueError(f'Value [{val}] is not safe number')

        return val

    @classmethod
    def safe_decimal(cls, val):
        if val is None:
            return 'NULL'

        val = str(val)

        if not cls.safe_decimal_regexp.match(val):
            raise ValueError(f'Value [{val}] is not safe integer')

        return val
