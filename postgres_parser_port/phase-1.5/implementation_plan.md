# Phase 1.5 Implementation Plan - Parser-Essential AST Nodes

**Goal**: Implement all PostgreSQL AST nodes required for parsing functionality  
**Strategy**: 8-phase implementation covering 166 parser-essential nodes  
**Success Criteria**: Complete PostgreSQL parsing capability with full syntax support

**IMPORTANT**: The `Expr` base interface already exists in `/go/parser/ast/expressions.go` and does not need to be reimplemented.

---

## Implementation Scope

### Parser-Essential Nodes: 166 Total
Based on ast_structs_checklist.md, focusing on nodes required for parsing:

- **Parse Tree Nodes** (parsenodes.h): ~130 missing nodes
- **Primitive Expression Nodes** (primnodes.h): ~36 missing nodes (excluding existing Expr interface)

### Explicitly Excluded (Not Needed for Parsing):
- **Query Planning** (plannodes.h): 66 nodes - Query planner infrastructure
- **Execution Engine** (execnodes.h): 80 nodes - Runtime execution infrastructure  
- **Path Optimization** (pathnodes.h): 81 nodes - Query optimization infrastructure
- **Support Infrastructure**: ~40 nodes - Memory management, extensions, etc.

---

## Phase Implementation Plan

### Phase 1A: Core Parse Infrastructure ✅ COMPLETED
**Target**: 25 nodes - Essential parsing foundation  
**Priority**: ✅ Complete - Lexer/parser integration enabled  
**Estimated Effort**: 2 sessions
**Status**: All 25 nodes implemented in `parse_infrastructure.go` with comprehensive tests

#### Core Expression Parsing (15 nodes)
- **RawStmt** - Raw statement wrapper (`parsenodes.h:2017`)
- **A_Expr** - Generic expression node (`parsenodes.h:329`)
- **A_Const** - Constant value (`parsenodes.h:357`)
- **ParamRef** - Parameter reference (`parsenodes.h:301`)
- **TypeCast** - Type casting (`parsenodes.h:370`)
- **FuncCall** - Function call (`parsenodes.h:423`)
- **A_Star** - Asterisk (*) (`parsenodes.h:445`)
- **A_Indices** - Array indices (`parsenodes.h:456`) 
- **A_Indirection** - Indirection (field access) (`parsenodes.h:479`)
- **A_ArrayExpr** - Array expression (`parsenodes.h:489`)
- **CollateClause** - COLLATE clause (`parsenodes.h:381`)
- **TypeName** - Type specification (`parsenodes.h:265`)
- **ColumnDef** - Column definition (`parsenodes.h:723`)
- **WithClause** - WITH clause (`parsenodes.h:1592`)
- **MultiAssignRef** - Multi-assignment reference (`parsenodes.h:532`)

#### Support Infrastructure (10 nodes)
- **WindowDef** - Window definition (`parsenodes.h:561`)
- **SortBy** - Sort specification (`parsenodes.h:543`)
- **GroupingSet** - Grouping set (`parsenodes.h:1506`)
- **LockingClause** - Locking clause (FOR UPDATE, etc.) (`parsenodes.h:831`)
- **XmlSerialize** - XML serialization (`parsenodes.h:842`)
- **PartitionElem** - Partition element (`parsenodes.h:860`)
- **TableSampleClause** - Table sample clause (`parsenodes.h:1344`)
- **ObjectWithArgs** - Object with arguments (`parsenodes.h:2524`)
- **SinglePartitionSpec** - Single partition specification (`parsenodes.h:945`)
- **PartitionCmd** - Partition command (`parsenodes.h:953`)

**Deliverables**: ✅ COMPLETED
- ✅ New AST file: `parse_infrastructure.go` (821 lines) - All 25 nodes implemented
- ✅ Test file: `parse_infrastructure_test.go` (718 lines) - Comprehensive test coverage
- ✅ Updated node tag system with 25 new parsing infrastructure nodes
- ✅ Removed placeholder structs and restored original naming (ColumnDef, WithClause, etc.)

---

### Phase 1B: Advanced SQL Statements  
**Target**: 20 nodes - Advanced DML and statement support  
**Priority**: 🔴 Critical - Core SQL functionality  
**Status**: ⚠️ **PARTIALLY COMPLETED** - 8 nodes completed, 12 remaining
**Estimated Effort**: 1-2 sessions

#### Advanced DML Statements (8 nodes)
- **MergeStmt** - MERGE statement (`parsenodes.h:2084`)
- **SetOperationStmt** - UNION/INTERSECT/EXCEPT (`parsenodes.h:2185`)
- **ReturnStmt** - RETURN statement (`parsenodes.h:2210`)
- **PLAssignStmt** - PL assignment statement (`parsenodes.h:2224`)
- **OnConflictClause** - ON CONFLICT clause (`parsenodes.h:1621`)
- **InferClause** - Inference clause (`parsenodes.h:1606`)
- **WithCheckOption** - WITH CHECK OPTION (`parsenodes.h:1368`)
- **MergeWhenClause** - WHEN clause in MERGE (`parsenodes.h:1717`)

#### Additional Statement Support (12 nodes)
- **TruncateStmt** - TRUNCATE statement (`parsenodes.h:3240`)
- **CommentStmt** - COMMENT statement (`parsenodes.h:3252`)
- **RenameStmt** - RENAME operations (`parsenodes.h:3525`)
- **AlterOwnerStmt** - ALTER OWNER (`parsenodes.h:3571`)
- **RuleStmt** - CREATE RULE (`parsenodes.h:3606`)
- ✅ **LoadStmt** - LOAD statement (`parsenodes.h:3755`) - COMPLETED
- ✅ **ClusterStmt** - CLUSTER statement (`parsenodes.h:3822`) - COMPLETED
- **LockStmt** - LOCK statement (`parsenodes.h:3942`)
- ✅ **CheckPointStmt** - CHECKPOINT statement (`parsenodes.h:3914`) - COMPLETED
- ✅ **DiscardStmt** - DISCARD statement (`parsenodes.h:3932`) - COMPLETED
- ✅ **NotifyStmt** - NOTIFY statement (`parsenodes.h:3622`) - COMPLETED
- ✅ **ListenStmt** - LISTEN statement (`parsenodes.h:3633`) - COMPLETED
- ✅ **UnlistenStmt** - UNLISTEN statement (`parsenodes.h:3643`) - COMPLETED

**Deliverables**:
- New AST file: `advanced_statements.go` (~600 lines)
- Test file: `advanced_statements_test.go` (~500 lines)

---

### Phase 1C: DDL Creation Statements
**Target**: 25 nodes - Complete DDL creation support  
**Priority**: 🟡 High - Advanced PostgreSQL DDL features  
**Estimated Effort**: 2 sessions

#### Core DDL Creation (15 nodes)
- **CreateFunctionStmt** - CREATE FUNCTION (`parsenodes.h:3427`)
- **CreateSeqStmt** - CREATE SEQUENCE (`parsenodes.h:3117`)
- **CreateOpClassStmt** - CREATE OPERATOR CLASS (`parsenodes.h:3169`)
- **CreateOpFamilyStmt** - CREATE OPERATOR FAMILY (`parsenodes.h:3201`)
- **CreateCastStmt** - CREATE CAST (`parsenodes.h:4002`)
- **CreateConversionStmt** - CREATE CONVERSION (`parsenodes.h:3988`)
- **CreateTransformStmt** - CREATE TRANSFORM (`parsenodes.h:4016`)
- **DefineStmt** - DEFINE statement (`parsenodes.h:3140`)
- **DeclareCursorStmt** - DECLARE CURSOR (`parsenodes.h:3293`)
- **FetchStmt** - FETCH statement (`parsenodes.h:3328`)
- **ClosePortalStmt** - CLOSE statement (`parsenodes.h:3305`)
- And additional DDL creation nodes...

**Deliverables**:
- Enhanced DDL file: Add ~800 lines to existing `ddl_statements.go`
- Test expansion: Add ~600 lines to `ddl_statements_test.go`

---

### Phase 1D: Range & Table Infrastructure
**Target**: 25 nodes - FROM clause and JOIN support  
**Priority**: 🟡 High - Essential for complex queries  
**Estimated Effort**: 2 sessions

#### Range Table System (15 nodes)
- **RangeTblEntry** - Range table entry (`parsenodes.h:1038`)
- **RangeSubselect** - Subquery in FROM (`parsenodes.h:615`)
- **RangeFunction** - Function in FROM (`parsenodes.h:637`)
- **RangeTableFunc** - Table function (`parsenodes.h:655`)
- **RangeTableFuncCol** - Table function column (`parsenodes.h:673`)
- **RangeTableSample** - TABLESAMPLE clause (`parsenodes.h:695`)
- **RangeTblFunction** - Range table function (`parsenodes.h:1317`)
- **RTEPermissionInfo** - Permission info for RTE (`parsenodes.h:1286`)
- And additional range/table infrastructure...

**Deliverables**:
- New AST file: `table_range_nodes.go` (~700 lines)
- Test file: `table_range_nodes_test.go` (~550 lines)

---

### Phase 1E: JSON Parse Tree Support
**Target**: 20 nodes - Modern PostgreSQL JSON functionality  
**Priority**: 🟡 High - Modern SQL features  
**Estimated Effort**: 2 sessions

#### JSON Parse Infrastructure (20 nodes)
- **JsonOutput** - JSON output specification (`parsenodes.h:1751`)
- **JsonArgument** - JSON function argument (`parsenodes.h:1762`)
- **JsonFuncExpr** - JSON function expression (`parsenodes.h:1785`)
- **JsonTable** - JSON_TABLE (`parsenodes.h:1821`)
- **JsonTablePathSpec** - JSON table path specification (`parsenodes.h:1807`)
- **JsonTableColumn** - JSON table column (`parsenodes.h:1851`)
- **JsonKeyValue** - JSON key-value pair (`parsenodes.h:1872`)
- **JsonParseExpr** - JSON_PARSE expression (`parsenodes.h:1883`)
- **JsonScalarExpr** - JSON scalar expression (`parsenodes.h:1896`)
- **JsonSerializeExpr** - JSON_SERIALIZE expression (`parsenodes.h:1908`)
- **JsonObjectConstructor** - JSON object constructor (`parsenodes.h:1920`)
- **JsonArrayConstructor** - JSON array constructor (`parsenodes.h:1934`)
- **JsonArrayQueryConstructor** - JSON array query constructor (`parsenodes.h:1947`)
- **JsonAggConstructor** - JSON aggregate constructor (`parsenodes.h:1962`)
- **JsonObjectAgg** - JSON_OBJECTAGG (`parsenodes.h:1976`)
- **JsonArrayAgg** - JSON_ARRAYAGG (`parsenodes.h:1989`)
- And additional JSON parsing nodes...

**Deliverables**:
- New AST file: `json_parse_nodes.go` (~650 lines)
- Test file: `json_parse_nodes_test.go` (~500 lines)

---

### Phase 1F: Primitive Expression Completion Part 1
**Target**: 20 nodes - Core primitive expressions  
**Priority**: 🟡 High - Expression evaluation infrastructure  
**Estimated Effort**: 2 sessions

#### Core Expression Infrastructure (20 nodes)
Note: `Expr` base interface already exists and does not need reimplementation.

- **GroupingFunc** - GROUPING function (`primnodes.h:537`)
- **WindowFuncRunCondition** - Window function run condition (`primnodes.h:596`)
- **MergeSupportFunc** - Merge support function (`primnodes.h:628`)
- **NamedArgExpr** - Named argument expression (`primnodes.h:787`)
- **CaseTestExpr** - CASE test expression (`primnodes.h:1352`)
- **MinMaxExpr** - MIN/MAX expression (`primnodes.h:1506`)
- **RowCompareExpr** - Row comparison (`primnodes.h:1463`)
- **SQLValueFunction** - SQL value function (`primnodes.h:1553`)
- **XmlExpr** - XML expression (`primnodes.h:1596`)
- **MergeAction** - MERGE action (`primnodes.h:2003`)
- **RangeTblRef** - Range table reference (`primnodes.h:2243`)
- **TableFunc** - Table function (`primnodes.h:109`)
- **IntoClause** - INTO clause (`primnodes.h:158`)
- And additional primitive expression nodes...

**Deliverables**:
- Enhanced expressions file: Add ~600 lines to existing `expressions.go`
- Test expansion: Add ~500 lines to `expressions_test.go`

---

### Phase 1G: JSON Primitive Expressions
**Target**: 16 nodes - JSON expression evaluation  
**Priority**: 🟢 Medium - Complete JSON support  
**Estimated Effort**: 1-2 sessions

#### JSON Primitive Infrastructure (16 nodes)
- **JsonFormat** - JSON format specification (`primnodes.h:1648`)
- **JsonReturning** - JSON RETURNING clause (`primnodes.h:1660`)
- **JsonValueExpr** - JSON value expression (`primnodes.h:1680`)
- **JsonConstructorExpr** - JSON constructor expression (`primnodes.h:1703`)
- **JsonIsPredicate** - JSON IS predicate (`primnodes.h:1732`)
- **JsonBehavior** - JSON behavior specification (`primnodes.h:1786`)
- **JsonExpr** - JSON expression (`primnodes.h:1813`)
- **JsonTablePath** - JSON table path (`primnodes.h:1867`)
- **JsonTablePlan** - JSON table plan (`primnodes.h:1882`)
- **JsonTablePathScan** - JSON table path scan (`primnodes.h:1893`)
- **JsonTableSiblingJoin** - JSON table sibling join (`primnodes.h:1923`)
- And additional JSON primitive expressions...

**Deliverables**:
- New AST file: `json_expressions.go` (~550 lines)
- Test file: `json_expressions_test.go` (~450 lines)

---

### Phase 1H: Final Parser Infrastructure
**Target**: 15+ remaining nodes - Complete parser readiness  
**Priority**: 🟢 Medium - Final infrastructure completion  
**Estimated Effort**: 1-2 sessions

#### Remaining Essential Nodes
All remaining parser-essential nodes from parsenodes.h and primnodes.h that haven't been covered in previous phases.

**Deliverables**:
- Complete any remaining nodes needed for full parsing capability
- Final integration testing and validation
- Documentation updates

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
   - Note: `Expr` interface already exists, use existing implementation

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
1. **Unit Tests** (~4,000 new lines of tests)
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

### Phase Completion Criteria
Each phase must meet:
- [ ] **All target nodes implemented** with accurate PostgreSQL source references
- [ ] **100% test coverage maintained** for all new nodes
- [ ] **Interface compliance verified** (all nodes implement required interfaces)
- [ ] **Build system integration** (make dev-test passes)
- [ ] **Performance validation** (AST operations remain efficient)

### Overall Success Criteria
- [ ] **166 parser-essential nodes implemented** (Complete parsing capability)
- [ ] **All PostgreSQL syntax supported** for parsing
- [ ] **JSON, XML, and DDL support complete** as requested
- [ ] **Integration with existing AST system verified**
- [ ] **Documentation updated** (all references accurate)

### Parser Readiness Validation
- [ ] **Lexer integration ready** (all parse tree nodes available)
- [ ] **Grammar development enabled** (complete syntax tree support)
- [ ] **Semantic analysis supported** (expression evaluation infrastructure)
- [ ] **PostgreSQL compatibility verified** (all syntax constructs supported)

---

## Dependencies and Sequencing

### Critical Path
**Phases 1A-1B** must be completed first as they provide core parsing infrastructure that other phases depend on.

### Parallel Development Opportunities
- **Phases 1C-1D** can be developed in parallel after 1A-1B completion
- **Phases 1E-1G** (JSON support) can be developed independently
- **Phase 1H** requires completion of all previous phases

### Integration Points
- All phases integrate with existing AST system in `/go/parser/ast/`
- Node tag system requires updates with each phase
- Test framework expansion with each phase

---

## Post-Phase 1.5 Readiness

Upon completion of all 8 phases:

### Immediate Capabilities
- **Complete PostgreSQL parsing** for all supported syntax
- **Lexer development enabled** with full parse tree support
- **Grammar implementation ready** with complete AST coverage
- **Semantic analysis foundation** with expression evaluation

### Future Development Paths
The excluded subsystems can be implemented as separate major projects:
- **Query Planning System** (66 plannodes.h nodes) - Separate phase
- **Execution Engine** (80 execnodes.h nodes) - Separate phase  
- **Path Optimization** (81 pathnodes.h nodes) - Separate phase

**Phase 1.5 completion provides complete foundation for PostgreSQL parser development, enabling Phase 2 (Lexer Implementation) and Phase 3 (Grammar/Parsing) with full AST support for all PostgreSQL syntax.**

---

## Progress Tracking

### Source of Truth
**All progress tracking is maintained in `ast_structs_checklist.md`** which serves as the definitive source of implementation status. This file contains:
- Complete inventory of all 456 PostgreSQL AST nodes
- Current implementation status (123 nodes completed, 333 remaining)
- Accurate PostgreSQL source references for each node
- Clear marking of implemented vs. missing nodes
- ✅ Phase 1A: 25 core parse infrastructure nodes completed (47% of parser-essential nodes)

### Recommended Practice
- **Update checkboxes in `ast_structs_checklist.md`** as nodes are implemented
- **Mark nodes as `[x]`** when implementation and testing are complete
- **Use implementation_plan.md** for phase planning and organization
- **No separate progress tracking file needed** as all status information is maintained in ast_structs_checklist.md

This approach eliminates duplication and ensures a single, accurate source of truth for project status.