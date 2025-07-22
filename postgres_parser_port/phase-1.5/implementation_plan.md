# Phase 1.5 Implementation Plan - Complete AST Node Implementation

**Goal**: Implement remaining 185+ PostgreSQL AST nodes for 100% compatibility  
**Strategy**: Priority-based implementation across 3-4 focused sessions  
**Success Criteria**: All 265 PostgreSQL AST nodes implemented with accurate source references and complete test coverage

---

## Implementation Roadmap

### Session 1: Essential Query Execution Nodes
**Target**: 20-25 critical nodes for basic SQL functionality  
**Estimated Effort**: 1 session (2-3 hours)  
**Priority**: 🔴 Critical - Blocks basic parser functionality

#### Core Query Structures
1. **TargetEntry** (`primnodes.h:1726-1745`)
   - Target list entries in SELECT statements
   - Critical for query execution planning
   - Fields: expr, resname, sortgroupref, resorigtbl, etc.

2. **FromExpr** (`primnodes.h:1877-1884`)
   - FROM clause representation  
   - Essential for table references and joins
   - Fields: fromlist, quals

3. **JoinExpr** (`primnodes.h:1803-1827`)
   - JOIN operations (INNER, LEFT, RIGHT, FULL, etc.)
   - Core SQL functionality
   - Fields: jointype, isNatural, larg, rarg, usingClause, quals, etc.

#### Subquery Support
4. **SubPlan** (`primnodes.h:1067-1105`)
   - Execution-level subquery representation
   - Essential for subquery processing
   - Fields: subLinkType, testexpr, paramIds, plan_id, etc.

5. **AlternativeSubPlan** (`primnodes.h:1113-1123`)
   - Alternative subquery execution strategies
   - Performance optimization support
   - Fields: subplans

#### Modern SQL Features
6. **CommonTableExpr** (`parsenodes.h:1706-1717`)
   - WITH clause (CTE) support
   - Increasingly common in modern SQL
   - Fields: ctename, aliascolnames, ctematerialized, ctequery, etc.

7. **WindowClause** (`parsenodes.h:571-583`)
   - Window function clauses
   - Essential for analytical queries
   - Fields: name, refname, partitionClause, orderClause, etc.

#### Essential Clause Support  
8. **SortGroupClause** (`parsenodes.h:1563-1576`)
   - ORDER BY and GROUP BY representation
   - Core SQL functionality
   - Fields: tleSortGroupRef, eqop, sortop, nulls_first, etc.

9. **RowMarkClause** (`parsenodes.h:1585-1595`)
   - FOR UPDATE/SHARE locking
   - Concurrency control support
   - Fields: rti, strength, waitPolicy, pushedDown

10. **OnConflictExpr** (`primnodes.h:2046-2061`)
    - INSERT...ON CONFLICT support
    - Important PostgreSQL-specific feature
    - Fields: action, arbiterElems, arbiterWhere, onConflictSet, etc.

#### Expected Deliverables:
- **New AST file**: `query_execution_nodes.go` (~1000+ lines)
- **Test file**: `query_execution_nodes_test.go` (~500+ lines) 
- **Integration**: Update existing AST traversal for new nodes
- **Documentation**: PostgreSQL source references for all nodes

---

### Session 2: Type System and Advanced Expressions  
**Target**: 25-30 nodes for type handling and complex expressions  
**Estimated Effort**: 1 session (2-3 hours)  
**Priority**: 🟡 Important - Enables advanced SQL features

#### Type Coercion System
1. **RelabelType** (`primnodes.h:1632-1646`)
   - Type casting operations
   - Essential for PostgreSQL's type system
   - Fields: arg, resulttype, resulttypmod, resultcollid, etc.

2. **CoerceViaIO** (`primnodes.h:1668-1679`)
   - Type coercion through I/O functions
   - Common type conversion mechanism
   - Fields: arg, resulttype, coerceformat

3. **ArrayCoerceExpr** (`primnodes.h:1681-1703`)
   - Array type coercion
   - Essential for array operations
   - Fields: arg, elemfuncid, resulttype, etc.

4. **ConvertRowtypeExpr** (`primnodes.h:1705-1713`)
   - Row type conversion
   - Record type handling
   - Fields: arg, resulttype, convertformat

#### Advanced Expression Handling
5. **FieldSelect** (`primnodes.h:1408-1421`)
   - Record field access (record.field)
   - Important for composite types
   - Fields: arg, fieldnum, resulttype, resulttypmod

6. **FieldStore** (`primnodes.h:1428-1441`)
   - Record field assignment
   - UPDATE operations on composite types
   - Fields: arg, newvals, fieldnums, resulttype

7. **SubscriptingRef** (`primnodes.h:597-621`)
   - Array/JSON subscripting (arr[1], json['key'])
   - Modern PostgreSQL indexing
   - Fields: refcontainertype, refelemtype, reftypmod, etc.

#### Boolean and Null Testing
8. **NullTest** (`primnodes.h:1787-1797`)
   - IS NULL/IS NOT NULL tests
   - Fundamental SQL operations
   - Fields: arg, nulltesttype, argisrow

9. **BooleanTest** (`primnodes.h:1805-1815`)
   - IS TRUE/IS FALSE/IS UNKNOWN tests
   - Boolean logic support
   - Fields: arg, booltesttype

#### Additional Expression Types
10. **CoerceToDomain** (`primnodes.h:1715-1730`)
    - Domain type coercion
    - PostgreSQL domain support
    - Fields: arg, resulttype, coercionformat

#### Expected Deliverables:
- **New AST file**: `type_coercion_nodes.go` (~800+ lines)
- **Test file**: `type_coercion_nodes_test.go` (~400+ lines)
- **Enhanced type system**: Integration with existing OID handling

---

### Session 3: DDL and Administrative Statements
**Target**: 35-40 nodes for data definition and administration  
**Estimated Effort**: 1 session (3-4 hours)  
**Priority**: 🟡 Important - Enables complete DDL support

#### Advanced DDL Commands
1. **AlterTableCmd** (`parsenodes.h:2205-2225`)
   - Individual ALTER TABLE operations
   - Core DDL functionality
   - Fields: subtype, name, num, newowner, def, etc.

2. **ColumnDef** (`parsenodes.h:801-829`)
   - Column definitions in CREATE TABLE
   - Essential DDL support
   - Fields: colname, typeName, inhcount, constraints, etc.

3. **TableLikeClause** (`parsenodes.h:831-841`)
   - LIKE clauses in CREATE TABLE
   - Table inheritance support
   - Fields: relation, options

#### Partitioning Support
4. **PartitionSpec** (`parsenodes.h:942-950`)
   - Table partitioning specifications
   - Modern PostgreSQL feature
   - Fields: strategy, partParams

5. **PartitionBoundSpec** (`parsenodes.h:952-966`)
   - Partition boundary specifications
   - Partitioning implementation
   - Fields: strategy, is_default, modulus, etc.

#### Index and Constraint Management
6. **IndexElem** (verify if exists, enhance if needed)
   - Index element specifications
   - May need enhancement for advanced features

7. **StatsElem** (`parsenodes.h:3176-3186`)
   - Statistics element specifications
   - Extended statistics support
   - Fields: name, expr

#### Foreign Data Wrapper Support
8. **CreateForeignServerStmt** (`parsenodes.h:2820-2829`)
9. **CreateForeignTableStmt** (`parsenodes.h:2690-2701`)
10. **CreateUserMappingStmt** (`parsenodes.h:2831-2839`)

#### Expected Deliverables:
- **Enhanced DDL file**: Add ~1000+ lines to existing `ddl_statements.go`
- **Test expansion**: Add ~500+ lines to `ddl_statements_test.go`
- **Complete DDL coverage**: Support all major PostgreSQL DDL operations

---

### Session 4: Advanced Features and Completion
**Target**: Remaining 100+ nodes for complete PostgreSQL coverage  
**Estimated Effort**: 1-2 sessions (3-5 hours)  
**Priority**: 🟢 Enhancement - Enables advanced PostgreSQL features

#### JSON/XML Support
1. **JsonExpr** (`primnodes.h:1900-1935`)
2. **JsonConstructorExpr** (`primnodes.h:1937-1953`) 
3. **JsonIsPredicate** (`primnodes.h:1955-1964`)
4. **XmlExpr** (`primnodes.h:1559-1583`)

#### Modern PostgreSQL Statements
5. **MergeStmt** (`parsenodes.h:2103-2119`)
   - PostgreSQL 15+ MERGE statement
   - Advanced DML operation
   
6. **CallStmt** (`parsenodes.h:3489-3497`)
   - CALL statement for procedures
   - Procedure support

#### Logical Replication Support
7. **CreatePublicationStmt** (`parsenodes.h:4070-4080`)
8. **CreateSubscriptionStmt** (`parsenodes.h:4099-4113`)
9. **AlterSubscriptionStmt** (`parsenodes.h:4115-4124`)

#### Query Planning Infrastructure
10. **PlaceHolderVar** (`primnodes.h:297-309`)
11. **PlannedStmt** (if needed for completeness)

#### Specialized Functions
12. **SQLValueFunction** (`primnodes.h:1538-1549`)
13. **GroupingFunc** (`primnodes.h:688-700`)
14. **MinMaxExpr** (`primnodes.h:1516-1528`)

#### Expected Deliverables:
- **New AST files**: `advanced_features.go`, `json_xml_nodes.go`
- **Complete PostgreSQL coverage**: All 265 nodes implemented
- **Full test coverage**: 100% test coverage maintained
- **Integration validation**: All nodes work with existing AST system

---

## Implementation Standards

### Code Quality Requirements
1. **PostgreSQL Source Accuracy**
   - Every struct must include exact PostgreSQL source reference
   - Format: `// Ported from postgres/src/include/nodes/file.h:line-range`
   - Verify line numbers against actual PostgreSQL source

2. **Interface Compliance**
   - All nodes implement appropriate interfaces (Node, Statement, Expression, Value)
   - Consistent constructor patterns (New* functions)
   - Proper String() methods for debugging

3. **Test Coverage**
   - Unit tests for every new node type
   - Constructor function tests
   - Interface implementation tests
   - Integration tests with existing AST

4. **Documentation**
   - Clear comments explaining node purpose
   - Usage examples for complex nodes
   - Integration notes for dependent nodes

### Performance Considerations
1. **Memory Efficiency**
   - Proper struct field ordering
   - Minimize memory allocations
   - Use appropriate Go types

2. **Concurrent Safety**
   - All nodes immutable after creation
   - No global state
   - Thread-safe operations

### Testing Strategy
1. **Unit Tests** (~3,000+ new lines of tests)
   - Test every field of every new node
   - Verify constructor behavior
   - Test edge cases and error conditions

2. **Integration Tests**
   - AST traversal with new nodes
   - Node visitor pattern compatibility
   - Serialization/deserialization if needed

3. **Regression Tests**
   - Ensure existing functionality remains working
   - Performance benchmarks for AST operations
   - Memory usage validation

---

## Success Metrics

### Completion Criteria
- [ ] **265 total nodes implemented** (100% PostgreSQL AST coverage)
- [ ] **All PostgreSQL source references accurate** (verified against source)
- [ ] **100% test coverage maintained** (all new nodes tested)
- [ ] **Interface compliance** (all nodes implement required interfaces)
- [ ] **Performance validation** (AST operations remain efficient)

### Quality Gates
- [ ] **Build system integration** (make dev-test passes)
- [ ] **Concurrent safety verification** (thread safety tests pass)
- [ ] **Documentation completeness** (all major nodes documented)
- [ ] **Code review readiness** (follows Go standards and project patterns)

### Exit Criteria for Phase 1.5
- [ ] **Complete AST implementation** (all PostgreSQL nodes available)
- [ ] **Accurate source traceability** (every node has correct PostgreSQL references)
- [ ] **Comprehensive testing** (100% pass rate maintained)
- [ ] **Integration validation** (AST traversal works with all nodes)
- [ ] **Documentation updated** (status files reflect 100% completion)

**Upon completion of Phase 1.5, the project will be ready for Phase 2 (Lexer Implementation) with complete AST foundation supporting all PostgreSQL syntax.**