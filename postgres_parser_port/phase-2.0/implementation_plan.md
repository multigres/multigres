# Phase 2.0: PostgreSQL Lexer Implementation Plan

**Phase Duration**: 9-11 development sessions (45-55 days estimated effort)  
**Prerequisites**: Phase 1.5 Complete AST Implementation ✅  
**Goal**: Implement a thread-safe, PostgreSQL-compatible lexer in Go without Flex dependency

---

## Phase 2 Overview

Phase 2 implements the lexical analysis component of the PostgreSQL parser, porting the sophisticated PostgreSQL lexer (scan.l) from C to Go. This involves creating a hand-written lexer that maintains 100% compatibility with PostgreSQL's token stream while providing thread-safety and Go idioms.

### Key Challenges
- **Complexity**: PostgreSQL's lexer uses 12 exclusive states with complex transitions
- **String Processing**: Multiple string formats (standard, extended, dollar-quoted, Unicode)
- **Thread Safety**: Eliminate all global state from C implementation
- **Performance**: Maintain PostgreSQL's no-backtrack optimization strategy

### Success Criteria
1. **100% Token Compatibility**: Identical token stream output to PostgreSQL
2. **Thread Safety**: Multiple concurrent lexer instances without interference
3. **Performance**: Within 20% of PostgreSQL lexer performance
4. **Maintainability**: Clean Go code with comprehensive test coverage

---

## Implementation Phases (9 Sessions)

### Phase 2A: Foundation & Token System
**Session Duration**: 1 session  
**Estimated Effort**: 4-5 days

#### Goals
- Establish lexer package structure
- Implement core token type system
- Create thread-safe lexer context
- Set up testing framework

#### Deliverables
- `go/parser/lexer/tokens.go` - Complete token type definitions
- `go/parser/lexer/lexer.go` - Basic lexer structure and context
- `go/parser/lexer/lexer_test.go` - Initial test framework
- Token type compatibility with PostgreSQL validated

#### Key Tasks
1. **Token Type System**
   - Port all PostgreSQL token constants (IDENT, SCONST, ICONST, etc.)
   - Implement token value union equivalent to `core_YYSTYPE`
   - Create token position tracking structures

2. **Lexer Context Design**
   - Port `core_yy_extra_type` structure to Go
   - Implement thread-safe state management with config variables
   - Design buffer management system
   - Eliminate global state (backslash_quote, escape_string_warning, standard_conforming_strings)

3. **Basic Testing Infrastructure**
   - Token creation and validation tests
   - Context initialization tests
   - Thread-safety validation framework

### Phase 2B: Basic Lexer Engine
**Session Duration**: 1-2 sessions  
**Estimated Effort**: 5-7 days

#### Goals
- Implement core character-by-character scanning
- Handle basic token types (identifiers, whitespace, operators)
- Establish state machine foundation

#### Deliverables
- Core lexer scanning loop
- Basic identifier and operator recognition
- Whitespace and comment handling (single-line)
- Initial state machine framework

#### Key Tasks
1. **Scanner Core**
   - Character-by-character input processing
   - Input buffer management and refill logic
   - Basic tokenization dispatch system

2. **Simple Tokens**
   - Identifier recognition with case handling
   - Single-character operators and punctuation
   - Whitespace tokenization and skipping

3. **State Machine Foundation**
   - Basic state enumeration and management
   - State transition framework
   - Initial state (default scanning mode)

### Phase 2C: String Literal System
**Session Duration**: 1-2 sessions  
**Estimated Effort**: 6-8 days

#### Goals
- Implement PostgreSQL's comprehensive string literal support
- Handle all string formats and escape processing
- Implement state-based string parsing

#### Deliverables
- `go/parser/lexer/strings.go` - Complete string processing system
- Support for all PostgreSQL string formats
- Comprehensive string literal testing

#### Key Tasks
1. **Standard SQL Strings**
   - Single-quoted strings with quote doubling (`'don''t'`)
   - String concatenation across whitespace
   - Multi-line string support

2. **Extended Strings**
   - Backslash escape sequences (`E'...'`)
   - Unicode escape sequences (`\uXXXX`, `\UXXXXXXXX`)
   - Octal and hexadecimal escape sequences

3. **Dollar-Quoted Strings**
   - Arbitrary tag parsing (`$tag$...$tag$`)
   - Nested dollar quoting support
   - Tag validation and matching

4. **String State Management**
   - Implement string-specific lexer states (xq, xe, xdolq, etc.)
   - State transition logic for string contexts
   - Error handling for unterminated strings

### Phase 2D: Numeric Literals & Bit Strings
**Session Duration**: 1 session  
**Estimated Effort**: 3-4 days

#### Goals
- Implement all PostgreSQL numeric literal formats
- Add bit string and hex string support
- Handle numeric parsing edge cases

#### Deliverables
- Complete numeric literal recognition
- Bit string and byte string support
- Numeric literal testing suite

#### Key Tasks
1. **Numeric Formats**
   - Integer literals (decimal, hexadecimal, octal, binary)
   - Floating-point literals with scientific notation
   - Numeric literal validation and range checking

2. **Special Literals**
   - Bit string literals (`B'101010'`)
   - Hexadecimal byte strings (`X'deadbeef'`)
   - Bit string state management

3. **Edge Case Handling**
   - Overflow detection and reporting
   - Invalid format error handling
   - PostgreSQL-compatible numeric parsing

### Phase 2E: Comments & Advanced Scanning
**Session Duration**: 1 session  
**Estimated Effort**: 4-5 days

#### Goals
- Implement PostgreSQL comment support
- Handle delimited identifiers
- Add advanced scanning features

#### Deliverables
- Complete comment parsing (single-line and multi-line)
- Delimited identifier support (`"identifier"`)
- Advanced scanning state management

#### Key Tasks
1. **Comment System**
   - Single-line comments (`--` to end of line)
   - Multi-line comments (`/* ... */`) with arbitrary nesting depth
   - Comment state management (xc state) with depth tracking (xcdepth)

2. **Delimited Identifiers**
   - Double-quoted identifiers with case preservation
   - Escape handling within delimited identifiers
   - Unicode support in identifiers

3. **Advanced Features**
   - Parameter placeholder recognition (`$1`, `$2`, etc.)
   - Type cast operator (`::`)
   - Array subscript operators

### Phase 2F: Error Handling & Recovery
**Session Duration**: 1 session  
**Estimated Effort**: 3-4 days

#### Goals
- Implement comprehensive error handling
- Add source position tracking
- Create error recovery mechanisms

#### Deliverables
- `go/parser/lexer/errors.go` - Complete error handling system
- Source position tracking for all tokens
- Error recovery and reporting mechanisms

#### Key Tasks
1. **Error Reporting**
   - Thread-safe error collection and reporting
   - Source position tracking (line, column, byte offset)
   - Error context and suggestion system

2. **Error Recovery**
   - Graceful handling of invalid input
   - Recovery strategies for different error types
   - Error message compatibility with PostgreSQL

3. **Position Tracking**
   - Accurate line and column tracking
   - Unicode-aware position calculation
   - Token span information for error reporting

### Phase 2G: Keyword Integration & Optimization
**Session Duration**: 1 session  
**Estimated Effort**: 3-4 days

#### Goals
- Integrate with existing keyword system
- Implement performance optimizations
- Add keyword context sensitivity

#### Deliverables
- `go/parser/lexer/keywords.go` - Enhanced keyword integration
- Performance optimization for hot paths
- Keyword lookup optimization

#### Key Tasks
1. **Keyword Integration**
   - Interface with existing keyword lookup system
   - Context-sensitive keyword recognition
   - Reserved word handling

2. **Performance Optimization**
   - Optimize identifier recognition hot path
   - Buffer management optimization
   - Memory allocation reduction

3. **Advanced Features**
   - Keyword case normalization
   - Perfect hash integration for keyword lookup
   - Context-dependent keyword vs identifier resolution

### Phase 2H: Testing & Validation
**Session Duration**: 1 session  
**Estimated Effort**: 5-6 days

#### Goals
- Comprehensive test suite development
- PostgreSQL compatibility validation
- Performance benchmarking

#### Deliverables
- Complete test suite with 100% coverage
- PostgreSQL compatibility validation
- Performance benchmarks and optimization

#### Key Tasks
1. **Comprehensive Testing**
   - Unit tests for all lexer components
   - Integration tests with AST system
   - Edge case and error condition testing

2. **Compatibility Validation**
   - Token stream comparison with PostgreSQL
   - Real-world SQL file processing tests
   - Regression test suite

3. **Performance Benchmarking**
   - Lexer performance vs PostgreSQL baseline
   - Memory usage profiling
   - Concurrent usage stress testing

### Phase 2I: Advanced Unicode & Edge Cases
**Session Duration**: 1 session  
**Estimated Effort**: 6-8 days

#### Goals
- Implement advanced Unicode processing features
- Handle complex edge cases and state transitions
- Complete sophisticated string processing requirements

#### Deliverables
- `go/parser/lexer/unicode.go` - Advanced Unicode processing system
- UTF-16 surrogate pair handling system
- Quote continuation and complex whitespace processing
- Complete edge case handling and validation

#### Key Tasks
1. **Unicode Surrogate Pair Processing**
   - Implement `<xeu>` state for UTF-16 surrogate pairs
   - Handle Unicode escape sequences in extended strings
   - Multi-byte character boundary detection and handling
   - Unicode validation and error reporting

2. **Quote Continuation System**
   - Complex multi-line string whitespace processing rules
   - String continuation across whitespace and comments
   - State management for quote stop detection (`<xqs>` state)
   - PostgreSQL-compatible whitespace handling

3. **Advanced Edge Case Handling**
   - Operator parsing with embedded comment starts
   - Complex state transitions between lexer modes
   - Error recovery in sophisticated string contexts
   - No-backtrack constraint validation across all states

4. **Performance Critical Path Optimization**
   - Hot path optimization for complex string processing
   - Memory allocation reduction in Unicode handling
   - State transition performance optimization
   - Concurrent usage optimization with complex states

---

## Implementation Files Structure

```
go/parser/lexer/
├── lexer.go          # Core lexer engine and state machine
├── tokens.go         # Token type definitions and constants  
├── strings.go        # String literal processing system
├── keywords.go       # Keyword lookup integration
├── errors.go         # Error handling and position tracking
├── context.go        # Thread-safe lexer context management
├── optimize.go       # Performance optimization utilities
├── unicode.go        # Advanced Unicode processing (surrogate pairs)
└── lexer_test.go     # Comprehensive test suite
```

## Integration Points

### With Existing Codebase
- **AST System**: Lexer will produce tokens for existing AST construction
- **Keywords**: Integrate with existing keyword lookup infrastructure
- **Parser Context**: Use existing thread-safe context framework
- **Testing**: Build on existing test patterns and utilities

### With Future Phases
- **Phase 3 (Grammar)**: Lexer will feed token stream to goyacc-generated parser
- **Phase 4 (Semantic Analysis)**: Error reporting integration for semantic errors
- **Phase 5 (Testing)**: Lexer forms foundation for end-to-end parser testing

## Key Technical Decisions

### Hand-Written vs Generated Lexer
**Decision**: Implement hand-written lexer in Go rather than using flex/goyacc lexer generator

**Rationale**:
- Thread safety: Easier to eliminate global state in hand-written code
- Go idioms: Can use proper Go error handling and memory management
- Maintainability: Easier to debug and modify than generated code
- Performance: Can optimize hot paths specifically for Go runtime

### State Machine Design
**Decision**: Use explicit state enumeration with switch-based dispatch

**Rationale**:
- Follows Vitess pattern successfully used in production
- Easier to debug than table-driven state machines
- Better performance in Go due to compiler optimizations
- Maintains PostgreSQL's exclusive state semantics

### Buffer Management
**Decision**: Use growable slices for token buffers with pre-allocation

**Rationale**:
- Leverages Go's built-in slice growth patterns
- Reduces memory allocations in common cases
- Thread-safe without explicit synchronization
- Easy integration with Go garbage collector

## Risk Mitigation

### Complexity Risk
**Risk**: PostgreSQL lexer is extremely complex with many edge cases
**Mitigation**: Incremental implementation with continuous PostgreSQL compatibility testing

### Performance Risk  
**Risk**: Hand-written Go lexer may be slower than optimized C flex lexer
**Mitigation**: Profile and optimize hot paths, maintain no-backtrack property

### Compatibility Risk
**Risk**: Subtle differences in token stream could break parser compatibility
**Mitigation**: Comprehensive token stream comparison testing at each phase

### Thread Safety Risk
**Risk**: Missing global state elimination could cause race conditions
**Mitigation**: Systematic analysis of C code, stress testing with concurrent lexers

## Success Metrics

1. **Functional**: 100% token stream compatibility with PostgreSQL (including Unicode edge cases)
2. **Performance**: Within 20% of PostgreSQL lexer speed on representative workloads with no-backtrack design  
3. **Quality**: 100% test coverage with comprehensive edge case handling
4. **Thread Safety**: Pass stress tests with multiple concurrent lexer instances (no global state)
5. **Maintainability**: Clean, documented Go code following project patterns
6. **Unicode Compliance**: Full UTF-16 surrogate pair support and multi-byte character handling

---

## Phase 2 Completion Criteria

Phase 2.0 will be considered complete when:

1. ✅ All PostgreSQL token types are recognized correctly
2. ✅ All PostgreSQL string formats are supported (standard, extended, dollar-quoted)
3. ✅ All numeric literal formats work identically to PostgreSQL
4. ✅ Comment handling matches PostgreSQL behavior exactly  
5. ✅ Error messages and position tracking are compatible
6. ✅ Thread safety is verified through stress testing (no global state)
7. ✅ Performance is within acceptable range of PostgreSQL baseline (no-backtrack design)
8. ✅ Integration with existing AST system is working
9. ✅ Comprehensive test suite passes with 100% coverage
10. ✅ Unicode edge cases are handled correctly (surrogate pairs, multi-byte boundaries)
11. ✅ Documentation is complete and accurate

**The successful completion of Phase 2.0 will provide a production-ready lexer that serves as the foundation for Phase 3 (grammar and parsing) while maintaining full PostgreSQL compatibility and thread safety.**