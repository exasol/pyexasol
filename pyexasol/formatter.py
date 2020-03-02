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

        self.default_conversion = 's'

        # Set default treatment for identifiers passed as strings to relevant functions
        if self.connection.options['quote_ident']:
            self.default_format_ident = self.quote_ident  # Identifiers will be quoted and escaped
            self.default_format_ident_value = str         # Identifier values will be left unchanged
        else:
            self.default_format_ident = self.safe_ident   # Identifiers will only be checked for safety
            self.default_format_ident_value = str.upper   # Identifier values will be transformed to upper-case

    def format_field(self, value, format_spec):
        if format_spec != '':
            raise ValueError("format_spec is disabled for ExaFormatter")

        return value

    def convert_field(self, value, conversion):
        if conversion is None:
            conversion = self.default_conversion

        if conversion not in self.conversions:
            raise ValueError(f"Unknown conversion {conversion}")

        if isinstance(value, list):
            if not value:
                raise ValueError("Trying to format an empty list")
            return ', '.join([self.conversions[conversion](v) for v in value])
        else:
            return self.conversions[conversion](value)

    @classmethod
    def escape(cls, val):
        return str(val).replace("'", "''")

    @classmethod
    def escape_ident(cls, val):
        return str(val).replace('"', '""')

    @classmethod
    def escape_like(cls, val):
        return cls.escape(val).replace('\\', '\\\\').replace('%', r'\%').replace('_', r'\_')

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
            if '.' in val:
                parts = val.split('.')
                raise ValueError(f"Value [{val}] is not a safe identifier. Please use tuple to pass schema names. "
                                 f"Example: ('{parts[0]}', '{parts[1]}')")
            elif '"' in val:
                raise ValueError(f"Value [{val}] is not a safe identifier. Please use 'quote_ident' or '!q' conversion "
                                 f"to pass identifiers with lowercase or special characters.")
            else:
                raise ValueError(f"Value [{val}] is not a safe identifier")

        return val

    @classmethod
    def safe_float(cls, val):
        if val is None:
            return 'NULL'

        val = str(val)

        if not cls.safe_float_regexp.match(val):
            raise ValueError(f'Value [{val}] is not a safe float')

        return val

    @classmethod
    def safe_decimal(cls, val):
        if val is None:
            return 'NULL'

        val = str(val)

        if not cls.safe_decimal_regexp.match(val):
            raise ValueError(f'Value [{val}] is not a safe integer')

        return val

    def __repr__(self):
        return f'<{self.__class__.__name__} session_id={self.connection.session_id()}>'
