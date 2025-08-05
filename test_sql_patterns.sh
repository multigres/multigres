#!/bin/bash

# Test SQL patterns through both lexers
cd /Users/manangupta/multigres

# SQL patterns to test
patterns=(
    "SELECT \$1::text"
    "SELECT 123_456"
    "SELECT U&'\\0061\\0062\\0063'"
    "SELECT /*+ hint */ * FROM t"
    "SELECT ARRAY[1,2,3]"
    "SELECT t.* FROM table t"
    "SELECT \$\$dollar quote\$\$"
    "SELECT 'string' 'concatenation'"
    "SELECT e'\\x41'"
    "SELECT 1.23e-10"
)

echo "Testing SQL patterns through both lexers"
echo "========================================"

for i in "${!patterns[@]}"; do
    pattern="${patterns[$i]}"
    echo ""
    echo "Pattern $((i+1)): $pattern"
    echo "----------------------------------------"
    
    echo "PostgreSQL lexer output:"
    ../postgres/complete_lexer_test "$pattern" 2>&1 || echo "ERROR: PostgreSQL lexer failed"
    
    echo ""
    echo "Go lexer output:"
    go run test_lexer.go "$pattern" 2>&1 || echo "ERROR: Go lexer failed"
    echo ""
done