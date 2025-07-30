# Technical Decisions - PostgreSQL Parser Port

This document tracks major technical decisions made during the PostgreSQL parser port project and the rationale behind them.

---

## Core Architecture Decisions

### Use Goyacc (Not Hand-Written Parser)
**Decision**: Use PostgreSQL's grammar with goyacc for parser generation  
**Rationale**: 
- Maintain compatibility with PostgreSQL grammar structure
- Follow established Vitess tooling patterns 
- Leverage existing PostgreSQL grammar accuracy
- Enable easier maintenance and updates

**Alternatives Considered**: Hand-written recursive descent parser  
**Trade-offs**: More complex build system but higher grammar fidelity

---

### All Go Code Under `go/` Directory  
**Decision**: Place all Go implementation under `multigres/go/` directory structure  
**Rationale**:
- Follow Vitess project structure conventions
- Clear separation of Go code from other project components
- Align with Go community practices for large projects

**Implementation**: 
- `go/parser/` - Core parser package
- `go/sqlast/` - SQL AST utilities
- `go/internal/` - Internal utilities and generators

---

### Thread-Safe Design with Explicit Context
**Decision**: Eliminate all global state, use explicit context passing  
**Rationale**:
- Critical requirement for concurrent use in production
- PostgreSQL parser relies heavily on global state that must be eliminated
- Enable parallel parsing in different goroutines

**Implementation**: 
- `ParserContext` struct contains all parser state
- No global variables or static state
- Context passed explicitly to all functions
- Immutable AST nodes after creation

---

### Test-Driven PostgreSQL Compatibility  
**Decision**: Achieve 100% compatibility with PostgreSQL parsing behavior  
**Rationale**:
- Must produce identical results to PostgreSQL parser
- Enable validation against PostgreSQL regression tests
- Ensure correctness for production use

**Strategy**:
- Port PostgreSQL regression tests where applicable
- Validate AST equivalence with PostgreSQL output
- Match error messages and source locations
- Test against multiple PostgreSQL versions

---

## AST Implementation Decisions

### Complete PostgreSQL Source Traceability
**Decision**: Every AST node includes exact PostgreSQL source file and line references  
**Rationale**:
- Enable maintainers to understand PostgreSQL origins
- Facilitate updates when PostgreSQL changes
- Provide debugging and verification capabilities
- Ensure implementation accuracy

**Format**: `// Ported from postgres/src/include/nodes/file.h:line-range`  
**Verification**: All references validated against actual PostgreSQL source in Session 004

---

### Interface-Based Node System
**Decision**: Use Go interfaces (Node, Statement, Expression, Value) for AST nodes  
**Rationale**:
- Type safety and compile-time checking
- Clean separation of node categories  
- Enable visitor pattern implementations
- Support future AST traversal and analysis needs

**Implementation**:
```go
type Node interface {
    NodeTag() NodeTag
    Location() int
    String() string
}

type Statement interface {
    Node
    StatementType() string
}

type Expression interface {
    Node  
    ExpressionType() string
}
```

---

### PostgreSQL OID Compatibility
**Decision**: Implement complete PostgreSQL Object ID (OID) compatibility  
**Rationale**:
- Essential for operator and function resolution
- Required for type system compatibility
- Enables proper semantic analysis
- Matches PostgreSQL behavior exactly

**Implementation**: 
- `Oid` type matching PostgreSQL definition
- All function, operator, and type OIDs preserved
- Compatible with PostgreSQL catalog system

---

## Implementation Strategy Decisions

### Priority-Based AST Implementation  
**Decision**: Implement AST nodes in priority order rather than file-by-file  
**Made In**: Session 004 gap analysis  
**Rationale**:
- Focus on essential functionality first
- Enable incremental parser development
- Provide early value from partial implementation

**Priority Categories**:
1. **High Priority**: Essential query execution nodes (TargetEntry, FromExpr, JoinExpr)
2. **Medium Priority**: Common SQL features (type coercion, advanced expressions)
3. **Low Priority**: Advanced PostgreSQL features (JSON/XML, replication, policies)

---

### Phase-Focused Documentation Structure
**Decision**: Organize documentation by current phase rather than session-by-session logs  
**Made In**: Session 004 restructuring  
**Rationale**:
- Eliminate contradictory information between status files
- Focus on current work rather than historical sessions
- Reduce maintenance overhead
- Provide clear roadmap for contributors

**Implementation**:
- Single `status.md` file for project-wide status
- Phase documentation integrated into main status file
- Removed `sessions/` directory to reduce complexity
- Phase 1.5 completed and documentation archived

---

## Quality Assurance Decisions

### 100% Test Coverage Requirement
**Decision**: Maintain 100% test coverage for all implemented AST nodes  
**Rationale**:
- Ensure correctness of PostgreSQL compatibility  
- Enable confident refactoring and updates
- Catch regressions early
- Validate thread safety

**Implementation**: 
- Unit tests for every AST node type
- Constructor function tests
- Interface implementation validation
- Integration tests for AST traversal

---

### Standardized Testing Framework
**Decision**: Use `github.com/stretchr/testify/assert` for all Go test assertions  
**Made In**: Session 007 testing cleanup

---

### Accurate Progress Tracking  
**Decision**: Report honest completion percentages based on actual node counts  
**Made In**: Session 004 gap analysis  
**Rationale**:
- Previously inflated claims (175+ nodes) undermined project credibility
- Accurate status essential for planning and resource allocation  
- Clear understanding of remaining work needed

**Corrected Status**: 30% complete (~70-80 of 265 total nodes) rather than claimed "Phase 1.5 Complete"

---

## Build System Decisions

### Makefile-Based Build System
**Decision**: Use comprehensive Makefile for build automation  
**Rationale**:
- Follow Vitess patterns for parser generation
- Support multiple development workflows
- Enable CI/CD integration  
- Provide reproducible builds

**Features**:
- Parser generation rules
- Testing automation  
- Development targets
- PostgreSQL compatibility validation

---

## Future Technical Decisions

### Documentation Needed
- [ ] Lexer implementation strategy (hand-coded vs generated)
- [ ] Token buffering and performance optimization
- [ ] Unicode and encoding support requirements
- [ ] Error recovery and continuation strategies
- [ ] Integration patterns with goyacc parser generator

These decisions will be made and documented as Phase 1.5 completes and Phase 2 (Lexer) begins.

---

## Decision Review Process

**Major Decisions**: Document in this file with rationale and alternatives considered  
**Minor Decisions**: Document in relevant implementation files or phase documentation  
**Review Trigger**: When changing established patterns or making architectural changes
**Update Responsibility**: Lead implementer or session owner should update this document

This document serves as institutional memory for the project's technical evolution.