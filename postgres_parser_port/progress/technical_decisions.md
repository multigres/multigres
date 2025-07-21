# Technical Decisions - PostgreSQL Parser Port

This document records all major technical decisions made during the project, with rationale and implications.

## Parser Generation Technology

### Decision: Use Goyacc (Not Hand-Written Parser)
**Date**: 2025-07-18 (Session 001)  
**Rationale**: 
- PostgreSQL grammar is extremely complex (~600 rules)
- Hand-written parser would be error-prone and hard to maintain
- We want to use PostgreSQL's grammar as-is to ensure compatibility
- Vitess successfully demonstrates goyacc tooling approach for complex grammars

**Implementation**:
- Port PostgreSQL's `gram.y` to `postgres.y` for goyacc compilation
- Follow Vitess *tooling pattern* with `generate.go` and Makefile rules
- Use `//go:generate` directives for code generation
- Keep PostgreSQL grammar rules intact, only adapt for Go compilation

**Implications**:
- Must learn goyacc nuances and limitations
- Build system complexity for parser generation
- Dependency on goyacc tool chain
- Grammar comes from PostgreSQL, not Vitess (critical difference)

---

## Project Structure

### Decision: All Go Code Under `go/` Directory
**Date**: 2025-07-18 (Session 001)  
**Rationale**:
- Follows Vitess project structure conventions
- Clear separation of Go code from other project components  
- Aligns with Go community practices for large projects
- Enables proper Go module organization

**Implementation**:
```
multigres/go/
├── parser/       # Core parser package
├── sqlast/       # AST utilities  
└── internal/     # Internal packages
```

**Implications**:
- Import paths will be `github.com/multigres/go/parser/...`
- Build scripts must account for `go/` subdirectory
- Documentation must clearly explain structure

---

## Thread Safety

### Decision: Explicit Context Passing (No Global State)
**Date**: 2025-07-18 (Session 001)  
**Rationale**:
- Critical requirement for concurrent use in production
- PostgreSQL parser has extensive global state that must be eliminated
- Go best practices favor explicit context passing
- Enables multiple parser instances in different goroutines

**Implementation**:
- Create `ParserContext` struct containing all parser state
- Pass context explicitly to all parsing functions
- Replace global variables with context fields
- Make AST nodes immutable after creation

**Implications**:
- Significant refactoring of PostgreSQL parsing logic
- Function signatures will change extensively
- Need systematic analysis to find all global state
- Testing must validate concurrent safety

---

## Compatibility Strategy

### Decision: Test-Driven PostgreSQL Compatibility
**Date**: 2025-07-18 (Session 001)  
**Rationale**:
- Must produce identical results to PostgreSQL parser
- Regression tests provide comprehensive coverage
- Automated validation prevents compatibility drift
- Builds confidence in correctness

**Implementation**:
- Port PostgreSQL regression tests to Go
- Create AST comparison utilities
- Validate error messages and locations match
- Test against multiple PostgreSQL versions

**Implications**:
- Large test suite to maintain
- Complex AST comparison logic needed
- Must track PostgreSQL version compatibility
- Performance testing secondary to correctness

---

## Error Handling

### Decision: Go Error Conventions with Source Locations
**Date**: 2025-07-18 (Session 001)  
**Rationale**:
- Follow Go idioms for error handling
- Must preserve PostgreSQL error message compatibility
- Source location tracking essential for debugging
- Thread-safe error reporting required

**Implementation**:
- Use Go error interface, not panics for parse errors
- Wrap errors with source location information
- Match PostgreSQL error message text and codes
- Context-aware error reporting

**Implications**:
- All parsing functions return `(result, error)`
- Error message compatibility testing required
- Source location tracking adds complexity
- Error recovery mechanisms needed

---

## Build System

### Decision: Makefile with Parser Generation Validation
**Date**: 2025-07-18 (Session 001)  
**Rationale**:
- Follow Vitess pattern for build automation
- Parser generation must be reproducible
- CI/CD integration requires validation
- Developer workflow simplification

**Implementation**:
- Makefile rules for `make parser` generation
- Validation that generated code matches committed version
- `//go:generate` directives for code generation
- Integration with Go toolchain

**Implications**:
- Build system complexity increases
- Developers must run `make parser` after grammar changes
- CI must validate parser generation
- Tool dependencies (goyacc) must be managed

---

## AST Design

### Decision: Immutable AST Nodes with Interface Hierarchy
**Date**: 2025-07-18 (Session 001)  
**Rationale**:
- Thread safety requires immutable data structures
- PostgreSQL has complex AST node hierarchy
- Go interfaces enable type safety and extensibility
- Facilitates testing and debugging

**Implementation**:
- Define Go interfaces for AST node types
- Struct implementations for concrete nodes
- Immutable fields (no setters after creation)
- Type-safe node traversal and manipulation

**Implications**:
- Large number of Go types to define
- Interface design complexity
- Memory efficiency considerations
- Visitor pattern implementation needed

---

## Module Organization

### Decision: Single Go Module with Multiple Packages
**Date**: 2025-07-18 (Session 001)  
**Rationale**:
- Simplifies dependency management
- Enables internal package usage
- Follows Go best practices for related functionality
- Easier to version and release

**Implementation**:
- Single `go.mod` in `multigres/go/`
- Multiple packages under `parser/` namespace
- Internal packages for implementation details
- Clear public API boundaries

**Implications**:
- Import path management across packages
- API design must be carefully planned
- Testing across package boundaries
- Documentation organization

---

## Testing Strategy

### Decision: Multi-Level Testing with PostgreSQL Validation
**Date**: 2025-07-18 (Session 001)  
**Rationale**:
- Complex system requires comprehensive testing
- Unit tests for individual components
- Integration tests for end-to-end functionality
- Compatibility tests for PostgreSQL equivalence

**Implementation**:
- Unit tests for lexer, parser, AST components
- Integration tests for complete parsing workflows
- PostgreSQL regression test port
- Fuzzing for robustness testing
- Performance benchmarks

**Implications**:
- Large test suite maintenance overhead
- Test data management complexity
- CI/CD pipeline must run all test levels
- Test result analysis and debugging tools needed

---

## Source Code Traceability

### Decision: PostgreSQL Source References in All Ported Code
**Date**: 2025-07-21 (Session 002)  
**Rationale**:
- Essential for code verification and maintenance
- Enables easy cross-reference between Go and PostgreSQL implementations
- Facilitates debugging and understanding of ported logic
- Supports code reviews and validation processes

**Implementation**:
- Every ported function/struct includes comment: `// Ported from postgres/path/file.c:line`
- Include original PostgreSQL function/variable names in comments
- Reference specific PostgreSQL version/commit being ported
- Maintain mapping documentation between Go and PostgreSQL code

**Implications**:
- Increased comment overhead in codebase
- Need to track PostgreSQL source locations accurately
- Documentation maintenance when PostgreSQL updates
- Code review process must verify source references

---

## Testing Framework

### Decision: Testify/Stretchr for Test Assertions
**Date**: 2025-07-21 (Session 002)  
**Rationale**:
- More readable test assertions than standard Go testing
- Better error messages for failed assertions
- Widely adopted in Go community
- Supports test suites and setup/teardown patterns

**Implementation**:
- Use `github.com/stretchr/testify/assert` for assertions
- Use `github.com/stretchr/testify/require` for test-stopping assertions
- Use `github.com/stretchr/testify/suite` for complex test organization
- Standard Go testing package for test discovery and running

**Implications**:
- Additional dependency on testify package
- Team needs familiarity with testify patterns
- More expressive but slightly different test patterns
- Better test maintenance and readability

---

## Test Data Strategy

### Decision: PostgreSQL-Driven Test Cases
**Date**: 2025-07-21 (Session 002)  
**Rationale**:
- Tests must verify compatibility with actual PostgreSQL behavior
- Use PostgreSQL's own test cases as source of truth
- Automated validation against PostgreSQL regression tests
- Reduces risk of missing edge cases or behaviors

**Implementation**:
- Parse and convert PostgreSQL regression tests to Go test format
- Create utilities to read PostgreSQL test files directly
- Implement AST comparison between Go parser and PostgreSQL output
- Test against multiple PostgreSQL versions when possible

**Implications**:
- Complex test data parsing and conversion logic needed
- Dependency on PostgreSQL test file formats
- Large test suite derived from PostgreSQL tests
- Need tooling to sync with PostgreSQL test updates

---

## Future Decisions Needed

### Open Technical Questions:
- [ ] Specific error code mapping strategy (PostgreSQL → Go)
- [ ] Memory optimization techniques for large ASTs
- [ ] Incremental parsing support (future enhancement)
- [ ] Plugin architecture for custom analysis (future)
- [ ] Performance optimization strategies
- [ ] Debugging and tooling integration

### Decision Process:
- Document all major decisions in this file
- Include rationale, implementation, and implications
- Update as project progresses and new decisions made
- Reference from session logs and progress tracking