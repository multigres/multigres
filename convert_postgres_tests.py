#!/usr/bin/env python3
"""
Script to convert PostgreSQL test files to JSON format for the parser tests.
This script reads .sql files from postgres-tests directory and creates
corresponding .json files in the postgres directory.
"""

import os
import json
import re
from pathlib import Path
from typing import List, Dict, Set

def extract_sql_statements(content: str) -> List[str]:
    """
    Extract SQL statements from PostgreSQL test file content.
    Handles multi-line statements and removes comments.
    """
    statements = []
    lines = content.split('\n')
    current_statement = []
    in_statement = False
    
    for line in lines:
        # Strip whitespace
        line = line.strip()
        
        # Skip empty lines
        if not line:
            continue
            
        # Skip comment-only lines
        if line.startswith('--'):
            continue
            
        # Remove inline comments (but preserve strings with --)
        # Simple approach: remove -- only if not inside quotes
        clean_line = remove_inline_comments(line)
        if not clean_line:
            continue
            
        current_statement.append(clean_line)
        in_statement = True
        
        # Check if statement ends with semicolon or contains psql meta-commands that terminate statements
        line_ends_statement = (clean_line.rstrip().endswith(';') or 
                             '\\gset' in clean_line or 
                             '\\gexec' in clean_line or
                             '\\crosstabview' in clean_line)
        
        if line_ends_statement:
            if current_statement:
                stmt = ' '.join(current_statement).strip()
                # Remove psql meta-commands from the end of statements
                stmt = remove_psql_metacommands(stmt)
                if stmt and not is_ignored_statement(stmt):
                    statements.append(stmt)
            current_statement = []
            in_statement = False
            
            # If there's content after the meta-command on the same line, start a new statement
            if '\\gset' in clean_line or '\\gexec' in clean_line or '\\crosstabview' in clean_line:
                remaining_content = extract_content_after_metacommand(clean_line)
                if remaining_content:
                    current_statement = [remaining_content]
                    in_statement = True
    
    # Handle case where last statement doesn't end with semicolon
    if current_statement:
        stmt = ' '.join(current_statement).strip()
        if stmt and not is_ignored_statement(stmt):
            statements.append(stmt)
    
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
        elif char == '-' and i + 1 < len(line) and line[i + 1] == '-':
            if not in_single_quote and not in_double_quote:
                # Found comment outside of quotes, stop here
                break
            else:
                result.append(char)
        else:
            result.append(char)
        
        i += 1
    
    return ''.join(result).strip()

def remove_psql_metacommands(stmt: str) -> str:
    """
    Remove psql meta-commands from the end of SQL statements.
    """
    # Remove common psql meta-commands
    metacommands = ['\\gset', '\\gexec', '\\crosstabview']
    
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
    metacommands = ['\\gset', '\\gexec', '\\crosstabview']
    
    for cmd in metacommands:
        if cmd in line:
            parts = line.split(cmd, 1)  # Split only on first occurrence
            if len(parts) > 1:
                remaining = parts[1].strip()
                # Return the remaining content if it looks like SQL
                if remaining and not remaining.startswith('\\'):
                    return remaining
    
    return ''

def is_ignored_statement(stmt: str) -> bool:
    """
    Check if a statement should be ignored (only psql meta-commands).
    """
    stmt_upper = stmt.upper().strip()
    
    # Only ignore psql meta-commands (backslash commands)
    ignore_patterns = [
        r'^\\.*',  # psql commands like \d, \dt, etc.
    ]
    
    for pattern in ignore_patterns:
        if re.match(pattern, stmt_upper):
            return True
    
    return False

def remove_duplicates(statements: List[str]) -> List[str]:
    """
    Remove duplicate statements while preserving order.
    """
    seen = set()
    unique_statements = []
    
    for stmt in statements:
        # Normalize whitespace for comparison
        normalized = ' '.join(stmt.split())
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
        clean_stmt = stmt.rstrip(';').strip()
        test_case = {
            "comment": f"{filename} - Statement {i + 1}",
            "query": clean_stmt
        }
        test_cases.append(test_case)
    
    return test_cases

def convert_sql_file(sql_path: Path, output_dir: Path):
    """
    Convert a single SQL file to JSON format.
    """
    print(f"Converting {sql_path.name}...")
    
    try:
        with open(sql_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        # Try with different encoding if UTF-8 fails
        with open(sql_path, 'r', encoding='latin1') as f:
            content = f.read()
    
    # Extract SQL statements
    statements = extract_sql_statements(content)
    
    # Remove duplicates
    unique_statements = remove_duplicates(statements)
    
    # Create test cases
    test_cases = create_test_cases(unique_statements, sql_path.stem)
    
    # Write JSON file
    json_filename = sql_path.stem + '.json'
    json_path = output_dir / json_filename
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(test_cases, f, indent=2)
    
    print(f"  Generated {json_filename} with {len(test_cases)} test cases")
    return len(test_cases)

def main():
    """
    Main function to convert all PostgreSQL test files.
    """
    # Define paths
    base_dir = Path("/Users/manangupta/multigres")
    sql_dir = base_dir / "go/parser/testdata/postgres-tests"
    output_dir = base_dir / "go/parser/testdata/postgres"
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all SQL files
    sql_files = list(sql_dir.glob("*.sql"))
    
    if not sql_files:
        print(f"No SQL files found in {sql_dir}")
        return
    
    print(f"Found {len(sql_files)} SQL files to convert")
    print("-" * 50)
    
    total_test_cases = 0
    converted_files = 0
    
    for sql_file in sorted(sql_files):
        try:
            test_case_count = convert_sql_file(sql_file, output_dir)
            total_test_cases += test_case_count
            converted_files += 1
        except Exception as e:
            print(f"  ERROR converting {sql_file.name}: {e}")
    
    print("-" * 50)
    print(f"Conversion complete!")
    print(f"  Files converted: {converted_files}/{len(sql_files)}")
    print(f"  Total test cases: {total_test_cases}")
    print(f"  Output directory: {output_dir}")

if __name__ == "__main__":
    main()