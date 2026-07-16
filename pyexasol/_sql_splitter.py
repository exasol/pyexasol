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
    SAW_SCRIPT = auto()


SCRIPT_HEADER_WORDS = {
    "ADAPTER",
    "JAVA",
    "LUA",
    "OR",
    "PYTHON",
    "PYTHON3",
    "R",
    "REPLACE",
    "SCALAR",
    "SET",
}


# This explicit state machine is intentionally kept in one function so quote and
# comment handling stays aligned with the main splitter scanner.
def strip_comments(sql: str) -> str:  # NOSONAR
    output: list[str] = []
    state = ScanState.NORMAL
    i = 0

    while i < len(sql):
        char = sql[i]
        next_char = sql[i + 1] if i + 1 < len(sql) else None

        if state == ScanState.SINGLE_QUOTE:
            output.append(char)
            if char == "'":
                state = ScanState.NORMAL
            i += 1
            continue

        if state == ScanState.DOUBLE_QUOTE:
            output.append(char)
            if char == '"':
                state = ScanState.NORMAL
            i += 1
            continue

        if char == "'":
            output.append(char)
            state = ScanState.SINGLE_QUOTE
            i += 1
            continue

        if char == '"':
            output.append(char)
            state = ScanState.DOUBLE_QUOTE
            i += 1
            continue

        if char == "-" and next_char == "-":
            output.append(" ")
            i += 2
            while i < len(sql) and sql[i] != "\n":
                i += 1
            continue

        if char == "/" and next_char == "*":
            output.append(" ")
            i += 2
            while i < len(sql):
                if sql[i] == "*" and i + 1 < len(sql) and sql[i + 1] == "/":
                    i += 2
                    break
                i += 1
            continue

        output.append(char)
        i += 1

    return "".join(output)


def split_sql_script(sql: str) -> list[str]:
    """
    Split an Exasol SQL script into executable statements.

    The splitter is lexical, not a full SQL parser. It treats semicolons as
    statement terminators except inside string literals, quoted identifiers,
    line comments, block comments, and Exasol script bodies. Exasol script
    bodies are entered after ``CREATE ... SCRIPT ... (...) ... AS`` and are
    terminated by a standalone ``/`` line.
    """
    statements: list[str] = []
    current: list[str] = []
    word: list[str] = []
    state = ScanState.NORMAL
    script_header = ScriptHeaderState.NONE
    saw_script_signature = False
    line_start = True
    i = 0

    def flush_statement() -> None:
        statement = "".join(current).strip()
        if statement and strip_comments(statement).strip():
            statements.append(statement)
        current.clear()

    def finish_word() -> bool:
        nonlocal script_header

        if not word:
            return False

        upper_word = "".join(word).upper()
        word.clear()

        if script_header == ScriptHeaderState.NONE:
            if upper_word == "CREATE":
                script_header = ScriptHeaderState.SAW_CREATE
        elif script_header == ScriptHeaderState.SAW_CREATE:
            if upper_word == "SCRIPT":
                script_header = ScriptHeaderState.SAW_SCRIPT
            elif upper_word not in SCRIPT_HEADER_WORDS:
                script_header = ScriptHeaderState.NONE
        else:
            if saw_script_signature and upper_word == "AS":
                return True

        return False

    def slash_terminates_script_body(position: int) -> bool:
        if not line_start or sql[position] != "/":
            return False

        lookahead = position + 1
        while lookahead < len(sql) and sql[lookahead] in " \t":
            lookahead += 1

        if lookahead == len(sql) or sql[lookahead] == "\n":
            return True

        return sql[lookahead] == "\r" and (
            lookahead + 1 == len(sql) or sql[lookahead + 1] == "\n"
        )

    while i < len(sql):
        char = sql[i]
        next_char = sql[i + 1] if i + 1 < len(sql) else None

        if state == ScanState.NORMAL:
            if char.isalnum() or char == "_":
                word.append(char)
                current.append(char)
                i += 1
                continue

            enter_script_body = finish_word()
            if enter_script_body:
                script_header = ScriptHeaderState.NONE
                saw_script_signature = False
                state = ScanState.SCRIPT_BODY
                current.append(char)
                line_start = char in " \t\n"
                i += 1
                continue

            if script_header == ScriptHeaderState.SAW_SCRIPT and char == "(":
                saw_script_signature = True

            if char == "'":
                current.append(char)
                state = ScanState.SINGLE_QUOTE
                i += 1
            elif char == '"':
                current.append(char)
                state = ScanState.DOUBLE_QUOTE
                i += 1
            elif char == "-" and next_char == "-":
                current.append(char)
                current.append(next_char)
                state = ScanState.LINE_COMMENT
                i += 2
            elif char == "/" and next_char == "*":
                current.append(char)
                current.append(next_char)
                state = ScanState.BLOCK_COMMENT
                i += 2
            elif char == ";":
                script_header = ScriptHeaderState.NONE
                saw_script_signature = False
                flush_statement()
                i += 1
            else:
                current.append(char)
                i += 1

        elif state == ScanState.SCRIPT_BODY:
            if slash_terminates_script_body(i):
                if current and current[-1] == "\n":
                    current.pop()
                flush_statement()
                script_header = ScriptHeaderState.NONE
                saw_script_signature = False
                state = ScanState.NORMAL
                i += 1
                while i < len(sql) and sql[i] in " \t":
                    i += 1
                if i < len(sql) and sql[i] == "\r":
                    i += 1
            else:
                current.append(char)
                if char == "\n":
                    line_start = True
                elif char not in " \t":
                    line_start = False
                i += 1

        elif state == ScanState.SINGLE_QUOTE:
            current.append(char)
            if char == "'":
                state = ScanState.NORMAL
            i += 1

        elif state == ScanState.DOUBLE_QUOTE:
            current.append(char)
            if char == '"':
                state = ScanState.NORMAL
            i += 1

        elif state == ScanState.LINE_COMMENT:
            current.append(char)
            if char == "\n":
                state = ScanState.NORMAL
            i += 1

        else:
            current.append(char)
            if char == "*" and next_char == "/":
                current.append(next_char)
                state = ScanState.NORMAL
                i += 2
            else:
                i += 1

    finish_word()
    flush_statement()
    return statements
