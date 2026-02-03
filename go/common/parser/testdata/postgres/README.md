# PostgreSQL Test Files

This directory contains JSON test files converted from PostgreSQL's regression test suite for use with the multigres parser.

## Directory Structure

- `*.json` - Converted test files containing SQL queries from PostgreSQL's test suite
- `convert_postgres_tests.py` - Script to convert PostgreSQL .sql test files to JSON format

## Setup Instructions

To regenerate these test files from PostgreSQL source:

1. **Get PostgreSQL test files**:

   ```bash
   # Clone PostgreSQL repository (if not already done)
   git clone https://github.com/postgres/postgres.git

   # Copy test files to postgres-tests directory (one level up)
   cp postgres/src/test/regress/sql/*.sql ../postgres-tests/
   ```

2. **Run the conversion script**:
   ```bash
   cd <home>/multigres/go/common/parser/testdata/postgres
   python3 convert_postgres_tests.py
   ```

## What the Conversion Script Does

The `convert_postgres_tests.py` script:

- Reads `.sql` files from `../postgres-tests/` directory
- Extracts SQL statements, removing psql-specific commands
- Handles psql variable substitution (e.g., `:datoid`, `:main_filenode`)
- Replaces psql variables with appropriate placeholder values
- Generates JSON test files in the current directory

## psql Variable Handling

PostgreSQL test files use psql variables (e.g., `:datoid`, `:committed`) that are substituted at runtime. The conversion script replaces these with sensible placeholder values:

- `:datoid` → `12345` (database OID)
- `:main_filenode` → `16384` (file node ID)
- `:committed` → `100` (transaction ID)
- And many others...

This ensures the generated SQL is valid PostgreSQL syntax that can be parsed directly.

## Updating Test Files

When PostgreSQL releases new versions with updated tests:

1. Copy the new test files to `../postgres-tests/`
2. Run `python3 convert_postgres_tests.py`
3. Commit the updated JSON files

## Notes

- The script automatically filters out psql meta-commands (e.g., `\d`, `\set`)
- Duplicate queries are removed while preserving order
- Each test case includes a comment indicating its source file and statement number
