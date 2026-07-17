from enum import (
    Enum,
    auto,
)


class ScanState(Enum):
    NORMAL = auto()
    SINGLE_QUOTE = auto()
    DOUBLE_QUOTE = auto()
    LINE_COMMENT = auto()
    BLOCK_COMMENT = auto()
    SCRIPT_BODY = auto()


class ScriptHeaderState(Enum):
    NONE = auto()
    SAW_CREATE = auto()
    SAW_POSSIBLE_SCRIPT_LANGUAGE = auto()
    SAW_SCRIPT_LANGUAGE = auto()
    SAW_SCRIPT = auto()


SCRIPT_HEADER_WORDS = {
    "ADAPTER",
    "JAVA",
    "LUA",
    "OR",
    "PREPROCESSOR",
    "PYTHON",
    "PYTHON3",
    "R",
    "REPLACE",
    "SCALAR",
    "SET",
}


class _CommentStripper:
    def __init__(self, sql: str) -> None:
        self.sql = sql
        self.output: list[str] = []
        self.state = ScanState.NORMAL
        self.position = 0

    def run(self) -> str:
        while self.position < len(self.sql):
            self.consume_current_character()

        return "".join(self.output)

    def consume_current_character(self) -> None:
        if self.state == ScanState.SINGLE_QUOTE:
            self.consume_quote("'")
        elif self.state == ScanState.DOUBLE_QUOTE:
            self.consume_quote('"')
        elif self.current_char == "'":
            self.enter_quote(ScanState.SINGLE_QUOTE)
        elif self.current_char == '"':
            self.enter_quote(ScanState.DOUBLE_QUOTE)
        elif self.current_char == "-" and self.next_char == "-":
            self.consume_line_comment()
        elif self.current_char == "/" and self.next_char == "*":
            self.consume_block_comment()
        else:
            self.append_current_char()

    @property
    def current_char(self) -> str:
        return self.sql[self.position]

    @property
    def next_char(self) -> str | None:
        lookahead = self.position + 1
        return self.sql[lookahead] if lookahead < len(self.sql) else None

    def consume_quote(self, quote: str) -> None:
        self.append_current_char()
        if self.output[-1] == quote:
            self.state = ScanState.NORMAL

    def enter_quote(self, state: ScanState) -> None:
        self.output.append(self.current_char)
        self.state = state
        self.position += 1

    def append_current_char(self) -> None:
        self.output.append(self.current_char)
        self.position += 1

    def consume_line_comment(self) -> None:
        self.output.append(" ")
        self.position += 2
        while self.position < len(self.sql) and self.current_char != "\n":
            self.position += 1

    def consume_block_comment(self) -> None:
        self.output.append(" ")
        self.position += 2
        while self.position < len(self.sql):
            if self.current_char == "*" and self.next_char == "/":
                self.position += 2
                return
            self.position += 1


def strip_comments(sql: str) -> str:
    return _CommentStripper(sql).run()


class _SqlScriptSplitter:
    def __init__(self, sql: str) -> None:
        self.sql = sql
        self.statements: list[str] = []
        self.current: list[str] = []
        self.word: list[str] = []
        self.state = ScanState.NORMAL
        self.script_header = ScriptHeaderState.NONE
        self.line_start = True
        self.position = 0

    def run(self) -> list[str]:
        while self.position < len(self.sql):
            self.consume_current_character()

        self.finish_word()
        self.flush_statement()
        return self.statements

    def consume_current_character(self) -> None:
        if self.state == ScanState.NORMAL:
            self.consume_normal()
        elif self.state == ScanState.SCRIPT_BODY:
            self.consume_script_body()
        elif self.state == ScanState.SINGLE_QUOTE:
            self.consume_quote("'")
        elif self.state == ScanState.DOUBLE_QUOTE:
            self.consume_quote('"')
        elif self.state == ScanState.LINE_COMMENT:
            self.consume_line_comment()
        else:
            self.consume_block_comment()

    @property
    def current_char(self) -> str:
        return self.sql[self.position]

    @property
    def next_char(self) -> str | None:
        lookahead = self.position + 1
        return self.sql[lookahead] if lookahead < len(self.sql) else None

    def consume_normal(self) -> None:
        if self.current_char.isalnum() or self.current_char == "_":
            self.word.append(self.current_char)
            self.append_current_char()
        elif self.finish_word():
            self.enter_script_body()
        elif self.current_char == "'":
            self.enter_state(ScanState.SINGLE_QUOTE)
        elif self.current_char == '"':
            self.enter_state(ScanState.DOUBLE_QUOTE)
        elif self.current_char == "-" and self.next_char == "-":
            self.enter_two_character_state(ScanState.LINE_COMMENT)
        elif self.current_char == "/" and self.next_char == "*":
            self.enter_two_character_state(ScanState.BLOCK_COMMENT)
        elif self.current_char == ";":
            self.script_header = ScriptHeaderState.NONE
            self.flush_statement()
            self.position += 1
        else:
            self.append_current_char()

    def consume_script_body(self) -> None:
        if self.slash_terminates_script_body():
            self.finish_script_body()
            return

        self.current.append(self.current_char)
        if self.current_char == "\n":
            self.line_start = True
        elif self.current_char not in " \t":
            self.line_start = False
        self.position += 1

    def consume_quote(self, quote: str) -> None:
        self.append_current_char()
        if self.current[-1] == quote:
            self.state = ScanState.NORMAL

    def consume_line_comment(self) -> None:
        self.append_current_char()
        if self.current[-1] == "\n":
            self.state = ScanState.NORMAL

    def consume_block_comment(self) -> None:
        next_char = self.next_char
        self.current.append(self.current_char)
        if self.current_char == "*" and next_char == "/":
            self.current.append(next_char)
            self.state = ScanState.NORMAL
            self.position += 2
        else:
            self.position += 1

    def enter_state(self, state: ScanState) -> None:
        self.current.append(self.current_char)
        self.state = state
        self.position += 1

    def enter_two_character_state(self, state: ScanState) -> None:
        next_char = self.next_char
        if next_char is None:
            msg = "Expected two-character token while splitting SQL script."
            raise ValueError(msg)
        self.current.append(self.current_char)
        self.current.append(next_char)
        self.state = state
        self.position += 2

    def enter_script_body(self) -> None:
        self.script_header = ScriptHeaderState.NONE
        self.state = ScanState.SCRIPT_BODY
        self.current.append(self.current_char)
        self.line_start = self.current_char in " \t\n"
        self.position += 1

    def finish_script_body(self) -> None:
        if self.current and self.current[-1] == "\n":
            self.current.pop()
        self.flush_statement()
        self.script_header = ScriptHeaderState.NONE
        self.state = ScanState.NORMAL
        self.position += 1
        self.skip_script_terminator_trailing_whitespace()

    def skip_script_terminator_trailing_whitespace(self) -> None:
        while self.position < len(self.sql) and self.current_char in " \t":
            self.position += 1
        if self.position < len(self.sql) and self.current_char == "\r":
            self.position += 1

    def append_current_char(self) -> None:
        self.current.append(self.current_char)
        self.position += 1

    def flush_statement(self) -> None:
        statement = "".join(self.current).strip()
        if statement and strip_comments(statement).strip():
            self.statements.append(statement)
        self.current.clear()

    def finish_word(self) -> bool:
        if not self.word:
            return False

        upper_word = "".join(self.word).upper()
        self.word.clear()
        return self.update_script_header(upper_word)

    def update_script_header(self, upper_word: str) -> bool:
        if self.script_header == ScriptHeaderState.NONE:
            self.update_empty_script_header(upper_word)
        elif self.script_header == ScriptHeaderState.SAW_CREATE:
            self.update_create_script_header(upper_word)
        elif self.script_header == ScriptHeaderState.SAW_POSSIBLE_SCRIPT_LANGUAGE:
            self.update_possible_language_script_header(upper_word)
        elif self.script_header == ScriptHeaderState.SAW_SCRIPT_LANGUAGE:
            self.update_language_script_header(upper_word)
        elif upper_word == "AS":
            return True

        return False

    def update_empty_script_header(self, upper_word: str) -> None:
        if upper_word == "CREATE":
            self.script_header = ScriptHeaderState.SAW_CREATE

    def update_create_script_header(self, upper_word: str) -> None:
        if upper_word == "SCRIPT":
            self.script_header = ScriptHeaderState.SAW_SCRIPT
        elif upper_word not in SCRIPT_HEADER_WORDS:
            self.script_header = ScriptHeaderState.SAW_POSSIBLE_SCRIPT_LANGUAGE

    def update_possible_language_script_header(self, upper_word: str) -> None:
        if upper_word in SCRIPT_HEADER_WORDS:
            self.script_header = ScriptHeaderState.SAW_SCRIPT_LANGUAGE
        else:
            self.script_header = ScriptHeaderState.NONE

    def update_language_script_header(self, upper_word: str) -> None:
        if upper_word == "SCRIPT":
            self.script_header = ScriptHeaderState.SAW_SCRIPT
        elif upper_word not in SCRIPT_HEADER_WORDS:
            self.script_header = ScriptHeaderState.NONE

    def slash_terminates_script_body(self) -> bool:
        if not self.line_start or self.current_char != "/":
            return False

        lookahead = self.position + 1
        while lookahead < len(self.sql) and self.sql[lookahead] in " \t":
            lookahead += 1

        if lookahead == len(self.sql) or self.sql[lookahead] == "\n":
            return True

        return self.sql[lookahead] == "\r" and (
            lookahead + 1 == len(self.sql) or self.sql[lookahead + 1] == "\n"
        )


def split_sql_script(sql: str) -> list[str]:
    """
    Split an Exasol SQL script into executable statements.

    The splitter is lexical, not a full SQL parser. It treats semicolons as
    statement terminators except inside string literals, quoted identifiers,
    line comments, block comments, and Exasol script bodies. Exasol script
    bodies are entered after ``CREATE ... SCRIPT ... AS`` and are terminated by
    a standalone ``/`` line.
    """
    return _SqlScriptSplitter(sql).run()
