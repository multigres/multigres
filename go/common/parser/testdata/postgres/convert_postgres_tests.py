#!/usr/bin/env python3
"""
Script to convert PostgreSQL test files to JSON format for the parser tests.

IMPORTANT: Before running this script, you need to copy PostgreSQL test files:
1. Copy test files from PostgreSQL source: src/test/regress/sql/*.sql
2. Place them in: ./go/parser/testdata/postgres-tests/
3. Run this script from the postgres directory: python3 convert_postgres_tests.py

This script:
- Reads .sql files from the postgres-tests directory (one level up)
- Handles psql variable substitution (e.g., :datoid, :main_filenode)
- Creates corresponding .json files in the current directory
- Filters out psql-specific commands that aren't valid SQL

Usage:
    cd <home>/multigres/go/parser/testdata/postgres
    python3 convert_postgres_tests.py
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional


def extract_sql_statements(content: str) -> List[str]:
    """
    Extract SQL statements from PostgreSQL test file content.
    Handles multi-line statements and removes comments.
    Properly handles quoted function bodies in CREATE FUNCTION statements.
    """
    statements = []
    lines = content.split("\n")
    current_statement = []
    in_statement = False
    in_function_body = False
    function_quote_style = None  # 'single' or 'dollar'
    dollar_quote_tag = None  # For tracking dollar quote tags like $func$ or $$

    for line in lines:
        # Strip whitespace
        line = line.strip()

        # Skip empty lines when not in a statement or function body
        if not line and not in_statement and not in_function_body:
            continue

        # Skip comment-only lines when not in a function body
        if line.startswith("--") and not in_function_body and not in_statement:
            continue

        # Remove inline comments (but preserve strings with --)
        clean_line = remove_inline_comments(line)
        if not clean_line and not in_statement and not in_function_body:
            continue

        # Add line to current statement
        if clean_line or in_function_body:
            current_statement.append(clean_line)
            in_statement = True

        # Check if we're starting a function body or DO block
        if not in_function_body and in_statement:
            stmt_so_far = " ".join(current_statement)
            # Look for CREATE [OR REPLACE] FUNCTION/PROCEDURE ... AS followed by a quote
            if re.search(
                r"create\s+(or\s+replace\s+)?(function|procedure)",
                stmt_so_far,
                re.IGNORECASE,
            ):
                # Check for single-quoted function body
                if re.search(r"\s+as\s+\'\s*$", stmt_so_far, re.IGNORECASE):
                    # Single-quoted function body starts
                    in_function_body = True
                    function_quote_style = "single"
                # Check for dollar-quoted function body (including on the same line)
                elif re.search(r"\s+as\s+(\$\w*\$)", stmt_so_far, re.IGNORECASE):
                    # Dollar-quoted function body starts
                    match = re.search(r"\s+as\s+(\$\w*\$)", stmt_so_far, re.IGNORECASE)
                    if match:
                        dollar_quote_tag = match.group(1)
                    in_function_body = True
                    function_quote_style = "dollar"
            # Look for DO blocks with dollar quotes
            elif re.search(r"^do\s+(\$\w*\$)", stmt_so_far, re.IGNORECASE):
                # DO block starts
                match = re.search(r"^do\s+(\$\w*\$)", stmt_so_far, re.IGNORECASE)
                if match:
                    dollar_quote_tag = match.group(1)
                in_function_body = True
                function_quote_style = "dollar"
            # Look for DO blocks with single quotes
            elif re.search(r"^do\s+\'\s*$", stmt_so_far, re.IGNORECASE):
                # DO block with single quote starts
                in_function_body = True
                function_quote_style = "single"

        # Check if we're ending a function body or DO block
        if in_function_body:
            stmt_so_far = " ".join(current_statement).lower()
            is_do_block = stmt_so_far.startswith("do ")

            if function_quote_style == "single":
                if is_do_block:
                    # For DO blocks, just look for closing quote (with optional semicolon)
                    if re.match(r"^'\s*;?\s*$", clean_line):
                        in_function_body = False
                        function_quote_style = None
                else:
                    # For functions, look for closing quote followed by
                    # language/immutable/volatile/etc or semicolon. The closing quote might be at
                    # the start of line OR after END; on the same line
                    if re.match(
                        r"^'\s*(language|immutable|volatile|stable|strict|security|cost|rows|;)",
                        clean_line,
                        re.IGNORECASE,
                    ) or re.search(
                        r";\s*'\s*(language|immutable|volatile|stable|strict|security|cost|rows|;)",
                        clean_line,
                        re.IGNORECASE,
                    ):
                        in_function_body = False
                        function_quote_style = None

                # Check if statement ends after the definition
                if not in_function_body and clean_line.rstrip().endswith(";"):
                    if current_statement:
                        stmt = " ".join(current_statement).strip()
                        stmt = remove_psql_metacommands(stmt)
                        if stmt and not is_ignored_statement(stmt):
                            processed_stmt = handle_parameterized_queries(stmt)
                            if processed_stmt:
                                statements.append(processed_stmt)
                    current_statement = []
                    in_statement = False

            elif function_quote_style == "dollar":
                # Look for the matching dollar quote tag
                if dollar_quote_tag and dollar_quote_tag in clean_line:
                    if is_do_block:
                        # For DO blocks, just check for the closing tag (possibly with semicolon)
                        pattern = re.escape(dollar_quote_tag) + r"\s*;?\s*$"
                        if re.search(pattern, clean_line):
                            in_function_body = False
                            function_quote_style = None
                            dollar_quote_tag = None
                    else:
                        # For functions, check if closing tag is followed by language keyword or
                        # semicolon
                        pattern = (
                            re.escape(dollar_quote_tag)
                            + r"\s*(language|immutable|volatile|stable|strict|security|cost|rows|;)"
                        )
                        if re.search(pattern, clean_line, re.IGNORECASE):
                            in_function_body = False
                            function_quote_style = None
                            dollar_quote_tag = None

                    # Check if statement ends after the definition
                    if not in_function_body and clean_line.rstrip().endswith(";"):
                        if current_statement:
                            stmt = " ".join(current_statement).strip()
                            stmt = remove_psql_metacommands(stmt)
                            if stmt and not is_ignored_statement(stmt):
                                processed_stmt = handle_parameterized_queries(stmt)
                                if processed_stmt:
                                    statements.append(processed_stmt)
                        current_statement = []
                        in_statement = False

        # Check if statement ends with semicolon (but ONLY when not inside function body)
        if not in_function_body and in_statement:
            line_ends_statement = (
                clean_line.rstrip().endswith(";")
                or "\\gset" in clean_line
                or "\\gexec" in clean_line
                or "\\crosstabview" in clean_line
            )

            if line_ends_statement:
                if current_statement:
                    stmt = " ".join(current_statement).strip()
                    # Remove psql meta-commands from the end of statements
                    stmt = remove_psql_metacommands(stmt)
                    if stmt and not is_ignored_statement(stmt):
                        # Handle parameterized queries
                        processed_stmt = handle_parameterized_queries(stmt)
                        if processed_stmt:
                            statements.append(processed_stmt)
                current_statement = []
                in_statement = False

                # If there's content after the meta-command on the same line, start a new statement
                if (
                    "\\gset" in clean_line
                    or "\\gexec" in clean_line
                    or "\\crosstabview" in clean_line
                ):
                    remaining_content = extract_content_after_metacommand(clean_line)
                    if remaining_content:
                        current_statement = [remaining_content]
                        in_statement = True

    # Handle case where last statement doesn't end with semicolon
    if current_statement:
        stmt = " ".join(current_statement).strip()
        if stmt and not is_ignored_statement(stmt):
            processed_stmt = handle_parameterized_queries(stmt)
            if processed_stmt:
                statements.append(processed_stmt)

    return statements


def remove_inline_comments(line: str) -> str:
    """
    Remove inline comments while preserving -- inside string literals.
    """
    result = []
    i = 0
    in_single_quote = False
    in_double_quote = False

    while i < len(line):
        char = line[i]

        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            result.append(char)
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            result.append(char)
        elif char == "-" and i + 1 < len(line) and line[i + 1] == "-":
            if not in_single_quote and not in_double_quote:
                # Found comment outside of quotes, stop here
                break
            result.append(char)
        else:
            result.append(char)

        i += 1

    return "".join(result).strip()


def remove_psql_metacommands(stmt: str) -> str:
    """
    Remove psql meta-commands from the end of SQL statements.
    """
    # Remove common psql meta-commands
    metacommands = ["\\gset", "\\gexec", "\\crosstabview"]

    for cmd in metacommands:
        if cmd in stmt:
            # Split on the meta-command and take only the SQL part
            parts = stmt.split(cmd)
            stmt = parts[0].strip()
            break

    return stmt


def extract_content_after_metacommand(line: str) -> str:
    """
    Extract any SQL content that appears after a psql meta-command on the same line.
    """
    metacommands = ["\\gset", "\\gexec", "\\crosstabview"]

    for cmd in metacommands:
        if cmd in line:
            parts = line.split(cmd, 1)  # Split only on first occurrence
            if len(parts) > 1:
                remaining = parts[1].strip()
                # Return the remaining content if it looks like SQL
                if remaining and not remaining.startswith("\\"):
                    return remaining

    return ""


def is_ignored_statement(stmt: str) -> bool:
    """
    Check if a statement should be ignored (only psql meta-commands).
    """
    stmt_upper = stmt.upper().strip()

    # Only ignore psql meta-commands (backslash commands)
    ignore_patterns = [
        r"^\\.*",  # psql commands like \d, \dt, etc.
    ]

    for pattern in ignore_patterns:
        if re.match(pattern, stmt_upper):
            return True

    return False


def handle_parameterized_queries(stmt: str) -> Optional[str]:
    """
    Handle PostgreSQL parameterized queries by replacing psql variables with placeholder values.
    Returns None if the statement should be skipped.
    """
    # Skip statements that are primarily psql variable references or conditional statements
    if (
        stmt.strip().startswith("\\if")
        or stmt.strip().startswith("\\set")
        or stmt.strip().startswith("\\echo")
    ):
        return None

    # Check if the statement contains psql variables (colon syntax)
    if ":" not in stmt:
        return stmt

    # Find all psql variables in the format :variable_name
    # Use negative lookbehind to avoid matching :: (type cast operator)
    psql_var_pattern = r"(?<!:):\w+"

    # Helper function to check if a position is inside square brackets
    def is_inside_brackets(text, pos):
        """Check if position is inside square brackets [...]"""
        bracket_depth = 0
        for i in range(pos):
            if text[i] == "[":
                bracket_depth += 1
            elif text[i] == "]":
                bracket_depth -= 1
        return bracket_depth > 0

    # Helper function to check if a position is inside quotes
    def is_inside_quotes(text, pos):
        """Check if position is inside quoted strings (single or double quotes)"""
        in_single_quote = False
        in_double_quote = False
        i = 0

        while i < pos:
            char = text[i]
            if char == "'" and not in_double_quote:
                # Check for escaped quote
                if i > 0 and text[i - 1] == "\\":
                    i += 1
                    continue
                # Check for doubled single quote
                if i + 1 < len(text) and text[i + 1] == "'":
                    i += 2  # Skip the doubled quote
                    continue
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                # Check for escaped quote
                if i > 0 and text[i - 1] == "\\":
                    i += 1
                    continue
                in_double_quote = not in_double_quote
            i += 1

        return in_single_quote or in_double_quote

    # Helper function to check if a colon match is part of a time pattern
    def is_time_pattern(text, pos):
        """Check if colon at position is part of a time pattern like HH:MM or HH:MM:SS"""
        # Look backwards for digits before the colon
        before_digits = ""
        i = pos - 1
        while i >= 0 and text[i].isdigit():
            before_digits = text[i] + before_digits
            i -= 1

        # Look forward for digits after the colon
        after_digits = ""
        match_end = pos + len(re.match(r":\w+", text[pos:]).group())
        j = pos + 1
        while j < len(text) and j < match_end and text[j].isdigit():
            after_digits += text[j]
            j += 1

        # Check if this looks like a time pattern
        # Before: 1-4 digits (hours/minutes), After: 1-2 digits (minutes/seconds)
        if before_digits and after_digits:
            if len(before_digits) <= 4 and len(after_digits) <= 2:
                # Additional check: make sure the after_digits is exactly what was matched
                variable_name = re.match(r":\w+", text[pos:]).group()[
                    1:
                ]  # Remove the colon
                return variable_name == after_digits

        return False

    # More careful processing - only replace actual psql variables
    matches = []
    for match in re.finditer(psql_var_pattern, stmt):
        pos = match.start()

        # Skip if this is part of a :: operator
        if pos > 0 and stmt[pos - 1] == ":":
            continue
        if pos < len(stmt) - 1 and stmt[pos + 1] == ":":
            continue

        # Skip if this is inside array brackets [...]
        if is_inside_brackets(stmt, pos):
            continue

        # Skip if this is inside quoted strings (single or double quotes)
        if is_inside_quotes(stmt, pos):
            continue

        # Skip if this is part of a time pattern (like 360:00, 12:34:56)
        if is_time_pattern(stmt, pos):
            continue

        matches.append(match.group())

    variables = matches

    if not variables:
        return stmt

    # Replace common psql variables with reasonable placeholder values
    var_replacements = {
        ":datoid": "12345",  # database OID
        ":main_filenode": "16384",  # file node ID
        ":toast_filenode": "16385",  # toast file node ID
        ":committed": "100",  # transaction ID
        ":rolledback": "101",  # transaction ID
        ":inprogress": "102",  # transaction ID
        ":segment_size": "16777216",  # WAL segment size
        ":segsize": "16777216",  # segment size
        ":libdir": "'/usr/lib/postgresql'",  # library directory (quoted)
        ":dlsuffix": "'.so'",  # dynamic library suffix (quoted)
        ":abs_srcdir": "'/tmp'",  # absolute source directory (quoted)
        ":abs_builddir": "'/tmp'",  # absolute build directory (quoted)
        ":skip_test": "false",  # skip test flag
        ":dboid": "1",  # database OID
        ":ON_ERROR_ROLLBACK": "'off'",  # psql setting (quoted)
        ":AUTOCOMMIT": "'on'",  # psql setting (quoted)
        ":ERROR": "''",  # error message (empty string)
        ":SQLSTATE": "'00000'",  # SQL state (quoted)
        ":ROW_COUNT": "0",  # row count
        ":LAST_ERROR_MESSAGE": "''",  # last error message (empty string)
        ":LAST_ERROR_SQLSTATE": "'00000'",  # last error SQL state (quoted)
        ":filename": "'/tmp/data.txt'",  # filename placeholder
        ":regresslib": "'/usr/lib/postgresql/regress.so'",  # regress library
        ":toast_oid": "16385",  # toast OID
        ":g_out_file": "'/tmp/output.txt'",  # output file
        ":o_out_file": "'/tmp/output.txt'",  # output file
    }

    processed_stmt = stmt

    for var in variables:
        var_lower = var.lower()

        # Use predefined replacement if available
        if var_lower in var_replacements:
            processed_stmt = processed_stmt.replace(
                var, str(var_replacements[var_lower])
            )
        else:
            # For unknown variables, try to infer a reasonable value based on common patterns
            if "_oid" in var_lower or "oid" in var_lower:
                processed_stmt = processed_stmt.replace(var, "12345")
            elif "_filenode" in var_lower or "filenode" in var_lower:
                processed_stmt = processed_stmt.replace(var, "16384")
            elif "_before" in var_lower or "_after" in var_lower:
                processed_stmt = processed_stmt.replace(var, "1000")
            elif var_lower.startswith(":stats_test_"):
                processed_stmt = processed_stmt.replace(var, "12345")
            elif "size" in var_lower:
                processed_stmt = processed_stmt.replace(var, "8192")
            elif "file" in var_lower and not (
                "filenode" in var_lower or "oid" in var_lower
            ):
                processed_stmt = processed_stmt.replace(var, "'/tmp/data.txt'")
            else:
                # For completely unknown variables, use a generic placeholder
                processed_stmt = processed_stmt.replace(var, "1")

    return processed_stmt


def remove_duplicates(statements: List[str]) -> List[str]:
    """
    Remove duplicate statements while preserving order.
    """
    seen = set()
    unique_statements = []

    for stmt in statements:
        # Normalize whitespace for comparison
        normalized = " ".join(stmt.split())
        if normalized.lower() not in seen:
            seen.add(normalized.lower())
            unique_statements.append(stmt)

    return unique_statements


def create_test_cases(statements: List[str], filename: str) -> List[Dict]:
    """
    Convert SQL statements to test case format.
    """
    test_cases = []

    for i, stmt in enumerate(statements):
        # Remove trailing semicolon for cleaner test output
        clean_stmt = stmt.rstrip(";").strip()
        test_case = {"comment": f"{filename} - Statement {i + 1}", "query": clean_stmt}
        test_cases.append(test_case)

    return test_cases


def convert_sql_file(sql_path: Path, output_dir: Path):
    """
    Convert a single SQL file to JSON format.
    """
    print(f"Converting {sql_path.name}...")

    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            content = f.read()
    except UnicodeDecodeError:
        # Try with different encoding if UTF-8 fails
        with open(sql_path, "r", encoding="latin1") as f:
            content = f.read()

    # Extract SQL statements
    statements = extract_sql_statements(content)

    # Remove duplicates
    unique_statements = remove_duplicates(statements)

    # Create test cases
    test_cases = create_test_cases(unique_statements, sql_path.stem)

    # Write JSON file
    json_filename = sql_path.stem + ".json"
    json_path = output_dir / json_filename

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(test_cases, f, indent=2)

    print(f"  Generated {json_filename} with {len(test_cases)} test cases")
    return len(test_cases)


def main():
    """
    Main function to convert all PostgreSQL test files.

    Expects to be run from the postgres directory with test files in ../postgres-tests/
    """
    # Define paths relative to current script location
    script_dir = Path(__file__).parent.resolve()
    sql_dir = (
        script_dir.parent / "postgres-tests"
    )  # Go up one level to find postgres-tests
    output_dir = script_dir  # Output to current directory (postgres)

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all SQL files
    sql_files = list(sql_dir.glob("*.sql"))

    if not sql_files:
        print(f"No SQL files found in {sql_dir}")
        print("\nPlease ensure PostgreSQL test files are copied to:")
        print(f"  {sql_dir}")
        print("\nFrom PostgreSQL source:")
        print("  src/test/regress/sql/*.sql")
        return

    print(f"Found {len(sql_files)} SQL files to convert")
    print(f"Source directory: {sql_dir}")
    print(f"Output directory: {output_dir}")
    print("-" * 50)

    total_test_cases = 0
    converted_files = 0

    for sql_file in sorted(sql_files):
        try:
            test_case_count = convert_sql_file(sql_file, output_dir)
            total_test_cases += test_case_count
            converted_files += 1
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"  ERROR converting {sql_file.name}: {e}")

    print("-" * 50)
    print("Conversion complete!")
    print(f"  Files converted: {converted_files}/{len(sql_files)}")
    print(f"  Total test cases: {total_test_cases}")
    print(f"  Output directory: {output_dir}")


if __name__ == "__main__":
    main()
