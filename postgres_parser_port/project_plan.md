# PostgreSQL Parser Port to Go - Master Engineering Plan

## Overview

Port the PostgreSQL parser from C to Go for the Multigres project, creating a thread-safe, maintainable parser that produces identical AST output to the original PostgreSQL parser.

## Project Structure

```
multigres/
├── go/
│   ├── parser/                  # Core parser package  
│   │   ├── lexer/              # Lexical analysis (scan.l port)
│   │   ├── grammar/            # Grammar rules (gram.y port with goyacc)
│   │   ├── ast/                # PostgreSQL AST node definitions
│   │   ├── analysis/           # Semantic analysis (analyze.c port)
│   │   ├── context/            # Parser context (thread-safe state)
│   │   ├── keywords/           # SQL keywords and tokens
│   │   └── generate.go         # Code generation directives
│   ├── sqlast/                 # SQL AST utilities and helpers
│   └── internal/
│       ├── testutils/          # Testing utilities
│       └── generators/         # Code generation tools
├── go.mod
├── go.sum
├── Makefile                    # Build automation including parser generation
└── tests/
    ├── lexer/                  # Lexer tests
    ├── parser/                 # Parser tests  
    ├── integration/            # End-to-end tests
    └── postgres_compat/        # PostgreSQL compatibility tests
```

## Development Phases

### Phase 1: Foundation (Estimated: 2-3 weeks) ✅ COMPLETED

**Goals**: Set up project structure and core infrastructure

**Tasks**:
1. **Project Setup** ✅
   - Create Go module in `go/` directory
   - Set up Makefile with parser generation rules
   - Configure CI/CD pipeline
   - Create basic directory structure

2. **Keywords & Tokens** ✅
   - Port PostgreSQL keywords from `src/common/keywords.c`
   - Create token definitions in `go/parser/keywords/`
   - Establish token constants and lookup functions

3. **Basic AST Framework** ✅
   - Analyze PostgreSQL AST node system architecture
   - Define Go base interfaces and node traversal system
   - Implement core node types (Query, SelectStmt, basic expressions)
   - Create foundation for full AST implementation

4. **Parser Context** ✅
   - Design thread-safe context struct in `go/parser/context/`
   - Replace all global state with context fields
   - Create context creation and management functions

**Deliverables**:
- Working Go module with basic structure ✅
- Basic AST node framework (foundation only) ✅
- Thread-safe parser context system ✅
- Comprehensive test framework setup ✅

### Phase 1.5: Complete AST Implementation (Estimated: 2-3 weeks)

**Goals**: Implement all PostgreSQL AST node types for complete compatibility

**Critical Discovery**: PostgreSQL has 535 AST struct definitions across multiple header files. Phase 1 only implemented ~15 basic nodes. Phase 1.5 will complete the full AST system.

**Tasks**:
1. **Parse Node Analysis**
   - Complete analysis of PostgreSQL node structure
   - Catalog all 196 structs in `parsenodes.h` (statements, clauses)
   - Catalog all 64 structs in `primnodes.h` (expressions, primitives)
   - Analyze dependencies and inheritance relationships

2. **Statement Node Implementation**
   - Port all DDL statements (CREATE, ALTER, DROP variants)
   - Port all DML statements (INSERT, UPDATE, DELETE variants)
   - Port all utility statements (VACUUM, ANALYZE, EXPLAIN, etc.)
   - Port all transaction and session statements

3. **Expression Node Implementation**
   - Port all expression types (FuncExpr, OpExpr, BoolExpr, etc.)
   - Port all literal types (constants, parameters, arrays)
   - Port all reference types (column refs, function refs)
   - Port all advanced expressions (CASE, COALESCE, sublinks)

4. **Support Node Implementation**
   - Port value nodes from `value.h`
   - Port miscellaneous nodes from `miscnodes.h`
   - Port list and container structures
   - Complete node type hierarchy

5. **Enhanced Node System**
   - Update node traversal to handle all node types
   - Implement proper node visitor patterns
   - Add node serialization/deserialization capabilities
   - Create node construction utilities

**Deliverables**:
- Complete PostgreSQL AST compatibility (all 535 node types)
- Updated node traversal system supporting all types
- Comprehensive AST test suite covering all nodes
- Performance benchmarks for AST operations
- Complete PostgreSQL source traceability

### Phase 2: Lexer (Estimated: 2-3 weeks)

**Goals**: Implement lexical analysis without Flex dependency

**Tasks**:
1. **Port scan.l to Go**
   - Recreate lexical analysis in `go/parser/lexer/`
   - Implement state machine for token recognition
   - Handle PostgreSQL-specific lexical rules

2. **Token Generation**
   - Implement PostgreSQL-compatible token stream
   - Support for all PostgreSQL token types
   - Proper handling of operators, literals, identifiers

3. **String Handling**
   - Port scansup.c functionality for escape handling
   - Unicode and encoding support
   - String literal processing

4. **Error Handling**
   - Thread-safe error reporting with source locations
   - Detailed error messages matching PostgreSQL
   - Error recovery mechanisms

**Deliverables**:
- Complete lexer implementation
- Comprehensive lexer test suite
- Error handling system
- Performance benchmarks

### Phase 3: Grammar & Parsing (Estimated: 4-5 weeks)

**Goals**: Generate parser using goyacc following Vitess patterns

**Tasks**:
1. **Port gram.y**
   - Convert PostgreSQL grammar to `go/parser/grammar/postgres.y`
   - Adapt grammar rules for goyacc compatibility
   - Maintain semantic equivalence with original

2. **Goyacc Integration**
   - Follow Vitess pattern with `generate.go` and Makefile rules
   - Custom goyacc configuration and build process
   - Generate parser code with proper Go conventions

3. **Parse Tree Construction**
   - Build "raw" parse trees matching PostgreSQL structure
   - Implement all PostgreSQL statement types
   - Ensure AST node creation matches original

4. **Location Tracking**
   - Implement source position tracking for error messages
   - Line and column number propagation
   - Error location reporting

**Deliverables**:
- Working goyacc-generated parser
- Complete grammar rule coverage
- Parse tree construction for all statement types
- Location tracking system

### Phase 4: Semantic Analysis (Estimated: 4-5 weeks)

**Goals**: Port semantic analysis and query transformation

**Tasks**:
1. **Port analyze.c**
   - Top-level semantic analysis in `go/parser/analysis/`
   - Query tree transformation and validation
   - Type checking and resolution

2. **Expression Analysis**
   - Port parse_expr.c, parse_func.c, parse_oper.c
   - Expression type resolution and coercion
   - Function and operator handling

3. **Clause Handling**
   - Port parse_clause.c, parse_target.c
   - WHERE, ORDER BY, GROUP BY clause processing
   - Target list analysis and expansion

4. **Type System**
   - Port parse_type.c, parse_coerce.c
   - Type coercion and compatibility rules
   - Data type handling and validation

5. **Advanced Features**
   - Port parse_cte.c, parse_merge.c, etc.
   - Common Table Expressions
   - MERGE statement support
   - Other advanced SQL features

**Deliverables**:
- Complete semantic analysis system
- Type checking and coercion
- All PostgreSQL clause types supported
- Advanced SQL feature support

### Phase 5: Testing & Validation (Estimated: 3-4 weeks)

**Goals**: Comprehensive testing and PostgreSQL compatibility validation

**Tasks**:
1. **Unit Tests**
   - Test each component individually using Go test framework
   - Mock components for isolated testing
   - Code coverage analysis

2. **PostgreSQL Test Port**
   - Port relevant tests from `src/test/` to `tests/postgres_compat/`
   - Adapt PostgreSQL regression tests for Go
   - Cross-reference test results with PostgreSQL

3. **Compatibility Testing**
   - Parse real PostgreSQL queries and validate AST equivalence
   - Test against multiple PostgreSQL versions
   - Verify error message compatibility

4. **Integration Tests**
   - End-to-end parser testing in `tests/integration/`
   - Performance testing and benchmarking
   - Memory usage analysis

5. **Fuzzing**
   - Generate random SQL for robustness testing
   - Edge case discovery and handling
   - Crash resistance validation

**Deliverables**:
- Comprehensive test suite
- PostgreSQL compatibility validation
- Performance benchmarks
- Fuzzing test results

## Technical Requirements

### Thread Safety Strategy
- **Parser Context**: All state in `ParserContext` struct passed between functions
- **No Global Variables**: Replace all static/global state with context fields
- **Immutable AST**: Once created, AST nodes are read-only
- **Concurrent Safe**: Multiple parser instances can run simultaneously in different goroutines

### Build System (Following Vitess Pattern)
- **Makefile**: Rules for parser generation, testing, linting
- **Code Generation**: Use `//go:generate` directives like Vitess sqlparser
- **Goyacc Integration**: Custom goyacc usage following `go/vt/sqlparser/generate.go`
- **CI Integration**: Validate generated parser matches committed version

### Verification Strategy
1. **Test Equivalence**: Same SQL input produces equivalent AST output vs PostgreSQL
2. **Round-trip Testing**: Parse → Serialize → Parse should be identical  
3. **PostgreSQL Regression Tests**: Port and run PostgreSQL's parser tests
4. **Error Message Compatibility**: Match PostgreSQL error messages and source locations
5. **Compatibility Matrix**: Test against different PostgreSQL versions

## Success Criteria

- [ ] Parse all PostgreSQL syntax supported by original parser
- [ ] Thread-safe: multiple goroutines can parse concurrently
- [ ] Compatibility: 100% test compatibility with PostgreSQL regression tests
- [ ] Maintainable: Clear Go idioms, comprehensive documentation
- [ ] Generated Code: Reproducible parser generation using Makefile
- [ ] Performance: Reasonable performance compared to original (not a primary goal)

## Key Technical Decisions

### Use Goyacc (Not Hand-Written Parser)
- **Rationale**: Use PostgreSQL's grammar as-is for compatibility
- **Pattern**: Follow Vitess tooling approach (goyacc + build system)
- **Benefit**: PostgreSQL grammar accuracy with Go tooling benefits

### All Code Under `go/` Directory
- **Rationale**: Follow Vitess project structure conventions
- **Benefit**: Clear separation of Go code from other project components
- **Standard**: Aligns with Go community practices for large projects

### Thread-Safe by Design
- **Rationale**: Critical requirement for concurrent use in production
- **Implementation**: Explicit context passing, no global state
- **Benefit**: Enables parallel parsing in different goroutines

### Test-Driven Compatibility
- **Rationale**: Must produce identical results to PostgreSQL parser
- **Strategy**: Port PostgreSQL regression tests and validate AST equivalence
- **Goal**: 100% compatibility with PostgreSQL parsing behavior

## Risk Mitigation

### Complexity of PostgreSQL Grammar
- **Risk**: PostgreSQL grammar is very complex with many edge cases
- **Mitigation**: Incremental development, extensive testing, phase-by-phase validation

### Thread Safety Implementation
- **Risk**: Difficult to ensure complete elimination of global state
- **Mitigation**: Systematic analysis of original code, careful context design

### Goyacc Limitations
- **Risk**: Goyacc may not support all PostgreSQL grammar features
- **Mitigation**: Study Vitess implementation, custom modifications if needed

### Testing Completeness
- **Risk**: May miss edge cases not covered by standard tests
- **Mitigation**: Fuzzing, real-world query testing, multiple PostgreSQL versions