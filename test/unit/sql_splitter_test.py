from inspect import cleandoc

import pytest

from pyexasol._sql_splitter import (
    split_sql_script,
    strip_comments,
)


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        pytest.param("", [], id="empty"),
        pytest.param(" \n\t ", [], id="whitespace"),
        pytest.param("-- comment only\n/* and block */", [], id="comment_only"),
        pytest.param("SELECT 1", ["SELECT 1"], id="single_without_semicolon"),
        pytest.param("SELECT 1;", ["SELECT 1"], id="single_with_semicolon"),
        pytest.param("SELECT 1; SELECT 2;", ["SELECT 1", "SELECT 2"], id="two"),
        pytest.param(
            "SELECT 'a; b' AS value; SELECT 2",
            ["SELECT 'a; b' AS value", "SELECT 2"],
            id="single_quoted_semicolon",
        ),
        pytest.param(
            'SELECT "col;name" FROM "schema;name"."table;name"; SELECT 2',
            ['SELECT "col;name" FROM "schema;name"."table;name"', "SELECT 2"],
            id="quoted_identifier_semicolon",
        ),
        pytest.param(
            "SELECT 'a'';b' AS value; SELECT 2",
            ["SELECT 'a'';b' AS value", "SELECT 2"],
            id="escaped_single_quote",
        ),
        pytest.param(
            'SELECT "a"";b" FROM table; SELECT 2',
            ['SELECT "a"";b" FROM table', "SELECT 2"],
            id="escaped_double_quote",
        ),
        pytest.param(
            "SELECT 1 -- a; b; c\n; SELECT 2",
            ["SELECT 1 -- a; b; c", "SELECT 2"],
            id="line_comment_semicolon",
        ),
        pytest.param(
            "SELECT /* a; b */ 1; SELECT 2",
            ["SELECT /* a; b */ 1", "SELECT 2"],
            id="block_comment_semicolon",
        ),
        pytest.param(
            "SELECT /* unterminated; block */ 1; SELECT 2",
            ["SELECT /* unterminated; block */ 1", "SELECT 2"],
            id="terminated_block_comment",
        ),
        pytest.param(
            "SELECT /* unterminated; block",
            ["SELECT /* unterminated; block"],
            id="unterminated_block_comment",
        ),
        pytest.param(
            "CREATE TABLE script AS SELECT 1; SELECT 2",
            ["CREATE TABLE script AS SELECT 1", "SELECT 2"],
            id="create_table_named_script",
        ),
        pytest.param(
            "CREATE TABLE script AS SELECT (1) AS x; SELECT 2",
            ["CREATE TABLE script AS SELECT (1) AS x", "SELECT 2"],
            id="create_table_named_script_with_parenthesized_expression",
        ),
    ],
)
def test_split_sql_script(sql, expected):
    assert split_sql_script(sql) == expected


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        pytest.param(
            cleandoc("""
                CREATE OR REPLACE PYTHON3 ADAPTER SCRIPT schema.name() AS
                  x = 1;
                  y = 2;
                /
                SELECT 1;
                """),
            [
                cleandoc("""
                    CREATE OR REPLACE PYTHON3 ADAPTER SCRIPT schema.name() AS
                      x = 1;
                      y = 2;
                    """),
                "SELECT 1",
            ],
            id="adapter_script",
        ),
        pytest.param(
            "CREATE LUA SCALAR SCRIPT schema.name() RETURNS INT AS\nreturn 1;\n/  \n",
            [
                "CREATE LUA SCALAR SCRIPT schema.name() RETURNS INT AS\nreturn 1;",
            ],
            id="script_terminator_with_trailing_spaces",
        ),
        pytest.param(
            "CREATE LUA SCALAR SCRIPT schema.name() RETURNS INT AS\r\nreturn 1;\r\n/\r\nSELECT 1;",
            [
                "CREATE LUA SCALAR SCRIPT schema.name() RETURNS INT AS\r\nreturn 1;",
                "SELECT 1",
            ],
            id="script_terminator_with_crlf",
        ),
        pytest.param(
            "CREATE LUA SCALAR SCRIPT schema.name() RETURNS INT AS\n   /",
            [
                "CREATE LUA SCALAR SCRIPT schema.name() RETURNS INT AS",
            ],
            id="indented_script_terminator_without_body",
        ),
        pytest.param(
            "CREATE LUA SCALAR SCRIPT schema.name() RETURNS INT AS\nreturn 1;",
            [
                "CREATE LUA SCALAR SCRIPT schema.name() RETURNS INT AS\nreturn 1;",
            ],
            id="script_body_until_eof",
        ),
        pytest.param(
            "CREATE LUA SCALAR SCRIPT schema.name() RETURNS VARCHAR(10) AS\nreturn '/';\n/\n",
            [
                "CREATE LUA SCALAR SCRIPT schema.name() RETURNS VARCHAR(10) AS\nreturn '/';",
            ],
            id="slash_not_at_line_start",
        ),
        pytest.param(
            "CREATE SCRIPT function_lib AS\nx = 1;\ny = 2;\n/\nSELECT 1;",
            [
                "CREATE SCRIPT function_lib AS\nx = 1;\ny = 2;",
                "SELECT 1",
            ],
            id="script_without_parameter_list",
        ),
        pytest.param(
            "CREATE JAVA ADAPTER SCRIPT my_script AS\n%jar /buckets/jdbc.jar;\n/\nSELECT 1;",
            [
                "CREATE JAVA ADAPTER SCRIPT my_script AS\n%jar /buckets/jdbc.jar;",
                "SELECT 1",
            ],
            id="adapter_script_without_parameter_list",
        ),
        pytest.param(
            "CREATE PYTHON3 PREPROCESSOR SCRIPT my_script AS\nprint('x;')\n/\nSELECT 1;",
            [
                "CREATE PYTHON3 PREPROCESSOR SCRIPT my_script AS\nprint('x;')",
                "SELECT 1",
            ],
            id="preprocessor_script",
        ),
        pytest.param(
            "CREATE MY_PYTHON SCALAR SCRIPT my_script AS\nprint('x;')\n/\nSELECT 1;",
            [
                "CREATE MY_PYTHON SCALAR SCRIPT my_script AS\nprint('x;')",
                "SELECT 1",
            ],
            id="custom_script_language_alias",
        ),
    ],
)
def test_split_sql_script_with_exasol_script_bodies(sql, expected):
    assert split_sql_script(sql) == expected


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        pytest.param("SELECT 1 -- comment", "SELECT 1  ", id="line_comment"),
        pytest.param("SELECT /* comment */ 1", "SELECT   1", id="block_comment"),
        pytest.param("SELECT /**/ 1", "SELECT   1", id="empty_block_comment"),
        pytest.param(
            "SELECT '/* not comment */'", "SELECT '/* not comment */'", id="single"
        ),
        pytest.param('SELECT "-- not comment"', 'SELECT "-- not comment"', id="double"),
        pytest.param("SELECT /* unterminated", "SELECT  ", id="unterminated_block"),
    ],
)
def test_strip_comments(sql, expected):
    assert strip_comments(sql) == expected
