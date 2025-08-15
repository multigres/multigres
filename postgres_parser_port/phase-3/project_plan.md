# Phase 3: Grammar & Parsing Implementation Plan

## Overview
Phase 3 implements the PostgreSQL grammar using goyacc, building on the completed AST nodes (Phase 1.5) and lexer (Phase 2). This phase also includes **SQL deparsing** capability to enable round-trip compatibility.

**Total Grammar Rules**: 727 rules from PostgreSQL's gram.y
**Implementation Strategy**: 10 sub-phases (3A-3J) with incremental complexity
**Deparsing**: Every parsed construct must support `SqlString()` for SQL generation

## Prerequisites Completed
- ✅ **Phase 1.5**: All 265 PostgreSQL AST nodes implemented
- ✅ **Phase 2**: Complete lexer with all PostgreSQL token types
- ✅ **Foundation**: Thread-safe context, build system, test framework
- ✅ **Deparsing Infrastructure**: Node.SqlString() interface and utilities

## Deparsing Strategy
Every AST node implements `SqlString() string` method for SQL generation:
- **Round-trip compatibility**: SQL → AST → SQL
- **Test coverage**: Parse tests paired with deparse tests
- **Default implementation**: Panic with helpful error message
- **Utility functions**: Identifier quoting, formatting helpers
- **PostgreSQL compliance**: Matches PostgreSQL's SQL output format

## Sub-Phase Breakdown

### Phase 3A: Grammar Foundation & Infrastructure
**Target**: ~20 foundational rules
**Duration**: 1-2 sessions
**Goals**:
- Set up goyacc integration with our lexer
- Port basic grammar structure from PostgreSQL
- Implement token declarations and precedence rules
- Create parser-lexer interface
- Basic statement routing (parse_toplevel, stmtmulti, stmt)
- **Deparsing**: Implement `SqlString()` for basic foundation nodes

**Key Rules**:
- parse_toplevel
- stmtmulti
- toplevel_stmt
- stmt
- opt_single_name
- opt_qualified_name
- opt_drop_behavior
- opt_with

### Phase 3B: Basic Expression Grammar
**Target**: ~40 expression rules
**Duration**: 1-2 sessions
**Goals**:
- Simple expressions (literals, identifiers, operators)
- Basic arithmetic and logical operations
- Column references and parameters
- Type casting expressions

**Key Rules**:
- a_expr
- b_expr
- c_expr
- AexprConst
- func_expr
- func_application
- func_name
- columnref
- indirection
- opt_indirection

### Phase 3C: SELECT Statement Core
**Target**: ~35 SELECT-related rules
**Duration**: 1-2 sessions
**Goals**:
- Basic SELECT structure
- FROM clause with single tables
- WHERE clause with simple conditions
- Basic target list

**Key Rules**:
- SelectStmt
- simple_select
- select_no_parens
- select_clause
- from_clause
- from_list
- where_clause
- target_list
- target_el

### Phase 3D: JOIN & Table References
**Target**: ~45 JOIN/table rules
**Duration**: 2 sessions
**Goals**:
- All JOIN types (INNER, LEFT, RIGHT, FULL, CROSS)
- Table aliases and qualified names
- Subqueries in FROM clause
- Common Table Expressions (WITH)

**Key Rules**:
- joined_table
- join_type
- join_qual
- join_outer
- table_ref
- relation_expr
- relation_expr_list
- with_clause
- common_table_expr

### Phase 3E: Data Manipulation (DML)
**Target**: ~50 DML rules
**Duration**: 2 sessions
**Goals**:
- INSERT with VALUES and SELECT
- UPDATE with SET and WHERE
- DELETE statements
- MERGE statements (PostgreSQL 15+)
- RETURNING clauses

**Key Rules**:
- InsertStmt
- insert_rest
- insert_column_list
- UpdateStmt
- set_clause_list
- DeleteStmt
- MergeStmt
- merge_when_clause
- returning_clause

### Phase 3F: Basic DDL - Tables & Indexes
**Target**: ~80 DDL rules
**Duration**: 2-3 sessions
**Goals**:
- CREATE TABLE with full column definitions
- Constraints (PRIMARY KEY, FOREIGN KEY, CHECK, UNIQUE)
- ALTER TABLE operations
- CREATE/DROP INDEX

**Key Rules**:
- CreateStmt
- OptTableElementList
- TableElement
- columnDef
- ColConstraint
- ColConstraintElem
- TableConstraint
- ConstraintElem
- AlterTableStmt
- alter_table_cmd
- IndexStmt

### Phase 3G: Advanced DDL
**Target**: ~100 advanced DDL rules
**Duration**: 3 sessions
**Goals**:
- CREATE VIEW, MATERIALIZED VIEW
- CREATE FUNCTION, PROCEDURE
- CREATE TRIGGER
- Schema, domain, type definitions
- Sequences and extensions

**Key Rules**:
- CreateFunctionStmt
- func_args_list
- func_return
- CreateTrigStmt
- trigger_events
- CreateSchemaStmt
- CreateDomainStmt
- CreateTypeStmt
- ViewStmt
- CreateSeqStmt

### Phase 3H: Advanced SELECT Features
**Target**: ~60 advanced query rules
**Duration**: 2 sessions
**Goals**:
- GROUP BY, HAVING
- ORDER BY, LIMIT, OFFSET
- Window functions and OVER clauses
- UNION, INTERSECT, EXCEPT
- Aggregates and DISTINCT

**Key Rules**:
- group_clause
- having_clause
- window_clause
- window_definition
- over_clause
- sortby_list
- sortby
- limit_clause
- for_locking_clause
- select_limit

### Phase 3I: Transaction & Administrative
**Target**: ~80 admin rules
**Duration**: 2 sessions
**Goals**:
- Transaction control (BEGIN, COMMIT, ROLLBACK, SAVEPOINT)
- Security (GRANT, REVOKE, CREATE ROLE)
- Configuration (SET, SHOW)
- Maintenance (VACUUM, ANALYZE, EXPLAIN)

**Key Rules**:
- TransactionStmt
- CreateRoleStmt
- AlterRoleStmt
- GrantStmt
- GrantRoleStmt
- VariableSetStmt
- VariableShowStmt
- ExplainStmt
- VacuumStmt
- AnalyzeStmt

### Phase 3J: PostgreSQL-Specific & Edge Cases
**Target**: ~200+ remaining rules
**Duration**: 3-4 sessions
**Goals**:
- COPY statements
- LISTEN/NOTIFY
- Cursors (DECLARE, FETCH, MOVE)
- Prepared statements (PREPARE, EXECUTE)
- Table inheritance
- Partitioning syntax
- All remaining specialized rules

**Key Rules**:
- CopyStmt
- copy_options
- ListenStmt
- NotifyStmt
- DeclareCursorStmt
- FetchStmt
- PrepareStmt
- ExecuteStmt
- partition_spec
- PartitionBoundSpec

## Implementation Guidelines

### For Each Sub-Phase:
1. **Start with rule stubs**: Define rules returning placeholder AST nodes
2. **Implement incrementally**: Get basic cases working before edge cases
3. **Test continuously**: Write tests for each rule as implemented
4. **Verify compatibility**: Compare with PostgreSQL's gram.y
5. **Update checklist**: Mark completed rules in grammar_rules_checklist.md

### Parser-Lexer Integration Pattern:
```go
// In postgres.y
%{
package parser

import (
    "github.com/multigres/parser/ast"
    "github.com/multigres/parser/lexer"
)

// Parser state passed through yylex
type parserState struct {
    lex    *lexer.Lexer
    result ast.Node
    errors []error
}
%}
```

### Testing Strategy:
1. **Unit tests**: Test individual grammar rules
2. **Integration tests**: Test complete SQL statements
3. **PostgreSQL compatibility**: Use PostgreSQL test cases
4. **Error cases**: Test syntax error handling
5. **Performance**: Benchmark against requirements

## Success Criteria

### Per Sub-Phase:
- All targeted grammar rules implemented
- Tests passing for implemented functionality
- No reduce/reduce conflicts
- Shift/reduce conflicts documented and justified
- Performance within acceptable bounds

### Overall Phase 3:
- [ ] All 727 grammar rules ported
- [ ] 100% PostgreSQL SQL syntax support
- [ ] Thread-safe concurrent parsing
- [ ] Integration with Phase 2 lexer
- [ ] Comprehensive test coverage
- [ ] Performance benchmarks met

## Risk Mitigation

### Potential Challenges:
1. **Grammar conflicts**: Use precedence declarations, refactor rules if needed
2. **Complex rules**: Break down into smaller sub-rules
3. **Performance issues**: Profile and optimize hot paths
4. **Compatibility gaps**: Reference PostgreSQL source, add tests

### Mitigation Strategies:
- Regular testing against PostgreSQL
- Incremental implementation
- Clear documentation of decisions
- Session handoff notes in progress.md

## Next Session Starting Point

**Start with Phase 3A** - Grammar Foundation & Infrastructure:
1. Set up goyacc build integration
2. Create postgres.y with basic structure
3. Port initial 20 foundational rules
4. Connect lexer to parser
5. Implement basic statement routing

See `progress.md` for detailed session tracking and `grammar_rules_checklist.md` for rule completion status.