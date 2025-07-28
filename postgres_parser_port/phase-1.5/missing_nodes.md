# Missing PostgreSQL AST Nodes - Complete Inventory

**Total Missing**: 260 nodes out of 456 total PostgreSQL AST nodes
**Current Implementation**: 196 nodes (43.0% complete)
**Target**: 100% PostgreSQL AST compatibility
**Latest**: ✅ Phase 1G completed - 11 JSON primitive expression nodes implemented in `json_parse_nodes.go`
**Previous**: ✅ Phase 1C completed - 15 DDL creation statement nodes implemented in `ddl_creation_statements.go`

This inventory is based on the comprehensive ast_structs_checklist.md which tracks all PostgreSQL node structures across parsenodes.h, primnodes.h, plannodes.h, execnodes.h, pathnodes.h, and supporting files.

---

## High Priority - Essential Missing Parse Nodes

### Core Parse Tree Structures (parsenodes.h)
These nodes are essential for basic SQL parsing functionality:

**Statement Nodes:**
- [x] **RawStmt** - Raw statement wrapper (`src/include/nodes/parsenodes.h:2017`) ✅ Phase 1A
- [x] **MergeStmt** - MERGE statement (`src/include/nodes/parsenodes.h:2084`) ✅ Phase 1B
- [x] **SetOperationStmt** - UNION/INTERSECT/EXCEPT (`src/include/nodes/parsenodes.h:2185`) ✅ Phase 1B
- [x] **ReturnStmt** - RETURN statement (`src/include/nodes/parsenodes.h:2210`) ✅ Phase 1B
- [x] **PLAssignStmt** - PL assignment statement (`src/include/nodes/parsenodes.h:2224`) ✅ Phase 1B

**Expression Nodes:**
- [x] **A_Expr** - Generic expression node (`src/include/nodes/parsenodes.h:329`) ✅ Phase 1A
- [x] **A_Const** - Constant value (`src/include/nodes/parsenodes.h:357`) ✅ Phase 1A
- [x] **ParamRef** - Parameter reference (`src/include/nodes/parsenodes.h:301`) ✅ Phase 1A
- [x] **TypeCast** - Type casting (`src/include/nodes/parsenodes.h:370`) ✅ Phase 1A
- [x] **FuncCall** - Function call (`src/include/nodes/parsenodes.h:423`) ✅ Phase 1A
- [x] **A_Star** - Asterisk (*) (`src/include/nodes/parsenodes.h:445`) ✅ Phase 1A
- [x] **A_Indices** - Array indices (`src/include/nodes/parsenodes.h:456`) ✅ Phase 1A
- [x] **A_Indirection** - Indirection (field access) (`src/include/nodes/parsenodes.h:479`) ✅ Phase 1A
- [x] **A_ArrayExpr** - Array expression (`src/include/nodes/parsenodes.h:489`) ✅ Phase 1A

**DDL Missing Nodes:**
- [x] **CreateFunctionStmt** - CREATE FUNCTION (`src/include/nodes/parsenodes.h:3427`) ✅ Phase 1C
- [x] **CreateSeqStmt** - CREATE SEQUENCE (`src/include/nodes/parsenodes.h:3117`) ✅ Phase 1C
- [x] **CreateOpClassStmt** - CREATE OPERATOR CLASS (`src/include/nodes/parsenodes.h:3169`) ✅ Phase 1C
- [x] **CreateOpFamilyStmt** - CREATE OPERATOR FAMILY (`src/include/nodes/parsenodes.h:3201`) ✅ Phase 1C
- [x] **CreateCastStmt** - CREATE CAST (`src/include/nodes/parsenodes.h:4002`) ✅ Phase 1C
- [x] **CreateConversionStmt** - CREATE CONVERSION (`src/include/nodes/parsenodes.h:3988`) ✅ Phase 1C
- [x] **CreateTransformStmt** - CREATE TRANSFORM (`src/include/nodes/parsenodes.h:4016`) ✅ Phase 1C
- [x] **DefineStmt** - DEFINE statement (`src/include/nodes/parsenodes.h:3140`) ✅ Phase 1C
- [x] **CreateEnumStmt** - CREATE TYPE ... AS ENUM (`src/include/nodes/parsenodes.h:3696`) ✅ Phase 1C
- [x] **CreateRangeStmt** - CREATE TYPE ... AS RANGE (`src/include/nodes/parsenodes.h:3707`) ✅ Phase 1C
- [x] **CreateStatsStmt** - CREATE STATISTICS (`src/include/nodes/parsenodes.h:3384`) ✅ Phase 1C
- [x] **CreatePLangStmt** - CREATE LANGUAGE (`src/include/nodes/parsenodes.h:3054`) ✅ Phase 1C
- [x] **TruncateStmt** - TRUNCATE statement (`src/include/nodes/parsenodes.h:3240`) ✅ Phase 1B
- [x] **CommentStmt** - COMMENT statement (`src/include/nodes/parsenodes.h:3252`) ✅ Phase 1B
- [x] **RenameStmt** - RENAME operations (`src/include/nodes/parsenodes.h:3525`) ✅ Phase 1B
- [x] **AlterOwnerStmt** - ALTER OWNER (`src/include/nodes/parsenodes.h:3571`) ✅ Phase 1B
- [x] **RuleStmt** - CREATE RULE (`src/include/nodes/parsenodes.h:3606`) ✅ Phase 1B

---

## Medium Priority - Core Missing Primitive Nodes

### Expression System (primnodes.h)
These nodes are essential for expression evaluation:

**Base Expression Types:**
- [x] **GroupingFunc** - GROUPING function (`src/include/nodes/primnodes.h:537`) ✅ Phase 1F
- [x] **WindowFuncRunCondition** - Window function run condition (`src/include/nodes/primnodes.h:596`) ✅ Phase 1F
- [x] **MergeSupportFunc** - Merge support function (`src/include/nodes/primnodes.h:628`) ✅ Phase 1F
- [x] **NamedArgExpr** - Named argument expression (`src/include/nodes/primnodes.h:787`) ✅ Phase 1F

**Control Flow and Test Nodes:**
- [x] **CaseTestExpr** - CASE test expression (`src/include/nodes/primnodes.h:1352`) ✅ Phase 1F
- [x] **MinMaxExpr** - MIN/MAX expression (`src/include/nodes/primnodes.h:1506`) ✅ Phase 1F
- [x] **RowCompareExpr** - Row comparison (`src/include/nodes/primnodes.h:1463`) ✅ Phase 1F
- [x] **SQLValueFunction** - SQL value function (`src/include/nodes/primnodes.h:1553`) ✅ Phase 1F
- [x] **XmlExpr** - XML expression (`src/include/nodes/primnodes.h:1596`) ✅ Phase 1F
- [x] **MergeAction** - MERGE action (`src/include/nodes/primnodes.h:2003`) ✅ Phase 1F

### Missing Table and Range Nodes
**Range Table Support:**
- [x] **RangeTblEntry** - Range table entry (`src/include/nodes/parsenodes.h:1038`) ✅ Phase 1D
- [x] **RangeSubselect** - Subquery in FROM (`src/include/nodes/parsenodes.h:615`) ✅ Phase 1D
- [x] **RangeFunction** - Function in FROM (`src/include/nodes/parsenodes.h:637`) ✅ Phase 1D
- [x] **RangeTableFunc** - Table function (`src/include/nodes/parsenodes.h:655`) ✅ Phase 1D
- [x] **RangeTableFuncCol** - Table function column (`src/include/nodes/parsenodes.h:673`) ✅ Phase 1D
- [x] **RangeTableSample** - TABLESAMPLE clause (`src/include/nodes/parsenodes.h:695`) ✅ Phase 1D
- [x] **RangeTblFunction** - Range table function (`src/include/nodes/parsenodes.h:1317`) ✅ Phase 1D
- [x] **RTEPermissionInfo** - Permission info for RTE (`src/include/nodes/parsenodes.h:1286`) ✅ Phase 1D

**Column and Table Definition:**
- [x] **ColumnDef** - Column definition (`src/include/nodes/parsenodes.h:723`) ✅ Phase 1A
- [x] **TypeName** - Type specification (`src/include/nodes/parsenodes.h:265`) ✅ Phase 1A
- [x] **MultiAssignRef** - Multi-assignment reference (`src/include/nodes/parsenodes.h:532`) ✅ Phase 1A

---

## Lower Priority - Advanced and Specialized Nodes

### Complete JSON Expression System (parsenodes.h + primnodes.h) ✅ FULLY COMPLETED
**JSON Nodes (parsenodes.h):** ✅ Phase 1E Complete
- [x] **JsonOutput** - JSON output specification (`src/include/nodes/parsenodes.h:1751`) ✅ Phase 1E
- [x] **JsonArgument** - JSON function argument (`src/include/nodes/parsenodes.h:1762`) ✅ Phase 1E
- [x] **JsonFuncExpr** - JSON function expression (`src/include/nodes/parsenodes.h:1785`) ✅ Phase 1E
- [x] **JsonTable** - JSON_TABLE (`src/include/nodes/parsenodes.h:1821`) ✅ Phase 1E
- [x] **JsonTablePathSpec** - JSON table path specification (`src/include/nodes/parsenodes.h:1807`) ✅ Phase 1E
- [x] **JsonTableColumn** - JSON table column (`src/include/nodes/parsenodes.h:1851`) ✅ Phase 1E
- [x] **JsonKeyValue** - JSON key-value pair (`src/include/nodes/parsenodes.h:1872`) ✅ Phase 1E
- [x] **JsonParseExpr** - JSON_PARSE expression (`src/include/nodes/parsenodes.h:1883`) ✅ Phase 1E
- [x] **JsonScalarExpr** - JSON scalar expression (`src/include/nodes/parsenodes.h:1896`) ✅ Phase 1E
- [x] **JsonSerializeExpr** - JSON_SERIALIZE expression (`src/include/nodes/parsenodes.h:1908`) ✅ Phase 1E
- [x] **JsonObjectConstructor** - JSON object constructor (`src/include/nodes/parsenodes.h:1920`) ✅ Phase 1E
- [x] **JsonArrayConstructor** - JSON array constructor (`src/include/nodes/parsenodes.h:1934`) ✅ Phase 1E
- [x] **JsonArrayQueryConstructor** - JSON array query constructor (`src/include/nodes/parsenodes.h:1947`) ✅ Phase 1E
- [x] **JsonAggConstructor** - JSON aggregate constructor (`src/include/nodes/parsenodes.h:1962`) ✅ Phase 1E
- [x] **JsonObjectAgg** - JSON_OBJECTAGG (`src/include/nodes/parsenodes.h:1976`) ✅ Phase 1E
- [x] **JsonArrayAgg** - JSON_ARRAYAGG (`src/include/nodes/parsenodes.h:1989`) ✅ Phase 1E

**JSON Nodes (primnodes.h):** ✅ Phase 1G Complete
- [x] **JsonFormat** - JSON format specification (`src/include/nodes/primnodes.h:1648`) ✅ Phase 1E
- [x] **JsonReturning** - JSON RETURNING clause (`src/include/nodes/primnodes.h:1660`) ✅ Phase 1E
- [x] **JsonValueExpr** - JSON value expression (`src/include/nodes/primnodes.h:1680`) ✅ Phase 1E
- [x] **JsonBehavior** - JSON behavior specification (`src/include/nodes/primnodes.h:1786`) ✅ Phase 1E
- [x] **JsonConstructorExpr** - JSON constructor expression (`src/include/nodes/primnodes.h:1703`) ✅ Phase 1G
- [x] **JsonIsPredicate** - JSON IS predicate (`src/include/nodes/primnodes.h:1732`) ✅ Phase 1G
- [x] **JsonExpr** - JSON expression (`src/include/nodes/primnodes.h:1813`) ✅ Phase 1G
- [x] **JsonTablePath** - JSON table path (`src/include/nodes/primnodes.h:1867`) ✅ Phase 1G
- [x] **JsonTablePlan** - JSON table plan (`src/include/nodes/primnodes.h:1882`) ✅ Phase 1G
- [x] **JsonTablePathScan** - JSON table path scan (`src/include/nodes/primnodes.h:1893`) ✅ Phase 1G
- [x] **JsonTableSiblingJoin** - JSON table sibling join (`src/include/nodes/primnodes.h:1923`) ✅ Phase 1G

### Missing Utility Statements (parsenodes.h)
- [x] **LoadStmt** - LOAD statement (`src/include/nodes/parsenodes.h:3755`) ✅ COMPLETED
- [x] **ClusterStmt** - CLUSTER statement (`src/include/nodes/parsenodes.h:3822`) ✅ COMPLETED
- [x] **LockStmt** - LOCK statement (`src/include/nodes/parsenodes.h:3942`) ✅ Phase 1B
- [x] **DeclareCursorStmt** - DECLARE CURSOR (`src/include/nodes/parsenodes.h:3293`) ✅ Phase 1C
- [x] **FetchStmt** - FETCH statement (`src/include/nodes/parsenodes.h:3328`) ✅ Phase 1C
- [x] **ClosePortalStmt** - CLOSE statement (`src/include/nodes/parsenodes.h:3305`) ✅ Phase 1C
- [x] **NotifyStmt** - NOTIFY statement (`src/include/nodes/parsenodes.h:3622`) ✅ COMPLETED
- [x] **ListenStmt** - LISTEN statement (`src/include/nodes/parsenodes.h:3633`) ✅ COMPLETED
- [x] **UnlistenStmt** - UNLISTEN statement (`src/include/nodes/parsenodes.h:3643`) ✅ COMPLETED
- [x] **CheckPointStmt** - CHECKPOINT statement (`src/include/nodes/parsenodes.h:3914`) ✅ COMPLETED
- [x] **DiscardStmt** - DISCARD statement (`src/include/nodes/parsenodes.h:3932`) ✅ COMPLETED

### Window and Grouping Support (parsenodes.h)
- [x] **WindowDef** - Window definition (`src/include/nodes/parsenodes.h:561`) ✅ Phase 1A
- [x] **SortBy** - Sort specification (`src/include/nodes/parsenodes.h:543`) ✅ Phase 1A
- [x] **GroupingSet** - Grouping set (`src/include/nodes/parsenodes.h:1506`) ✅ Phase 1A
- [x] **WithClause** - WITH clause (`src/include/nodes/parsenodes.h:1592`) ✅ Phase 1A
- [x] **OnConflictClause** - ON CONFLICT clause (`src/include/nodes/parsenodes.h:1621`) ✅ Phase 1B
- [x] **InferClause** - Inference clause (`src/include/nodes/parsenodes.h:1606`) ✅ Phase 1B
- [x] **WithCheckOption** - WITH CHECK OPTION (`src/include/nodes/parsenodes.h:1368`) ✅ Phase 1B
- [x] **MergeWhenClause** - WHEN clause in MERGE (`src/include/nodes/parsenodes.h:1717`) ✅ Phase 1B

### Miscellaneous Parse Nodes (parsenodes.h)
- [x] **LockingClause** - Locking clause (FOR UPDATE, etc.) (`src/include/nodes/parsenodes.h:831`) ✅ Phase 1A
- [x] **XmlSerialize** - XML serialization (`src/include/nodes/parsenodes.h:842`) ✅ Phase 1A
- [x] **PartitionElem** - Partition element (`src/include/nodes/parsenodes.h:860`) ✅ Phase 1A
- [x] **SinglePartitionSpec** - Single partition specification (`src/include/nodes/parsenodes.h:945`) ✅ Phase 1A
- [x] **PartitionCmd** - Partition command (`src/include/nodes/parsenodes.h:953`) ✅ Phase 1A
- [x] **TableSampleClause** - Table sample clause (`src/include/nodes/parsenodes.h:1344`) ✅ Phase 1A
- [x] **ObjectWithArgs** - Object with arguments (`src/include/nodes/parsenodes.h:2524`) ✅ Phase 1A

---

## Massive Missing Infrastructure - Plan and Execution Nodes

### Plan Node Structures (plannodes.h) - ALL MISSING
**Core Plan Nodes (66 nodes):**
- [ ] **PlannedStmt** - Planned statement (`src/include/nodes/plannodes.h:46`)
- [ ] **Plan** - Base plan node (`src/include/nodes/plannodes.h:119`)
- [ ] **Result** - Result plan (`src/include/nodes/plannodes.h:196`)
- [ ] **ProjectSet** - Project set plan (`src/include/nodes/plannodes.h:208`)
- [ ] **ModifyTable** - Modify table plan (`src/include/nodes/plannodes.h:229`)
- [ ] All scan nodes (SeqScan, IndexScan, BitmapHeapScan, etc.)
- [ ] All join nodes (NestLoop, MergeJoin, HashJoin, etc.)
- [ ] All set operation nodes (Append, MergeAppend, etc.)
- [ ] All sorting/grouping nodes (Sort, Group, Agg, WindowAgg, etc.)

### Execution Node Structures (execnodes.h) - ALL MISSING
**Core Execution State (80+ nodes):**
- [ ] **ExprState** - Expression state (`src/include/nodes/execnodes.h:78`)
- [ ] **PlanState** - Plan state (`src/include/nodes/execnodes.h:1115`)
- [ ] **EState** - Executor state (`src/include/nodes/execnodes.h:623`)
- [ ] All scan states (SeqScanState, IndexScanState, etc.)
- [ ] All join states (NestLoopState, MergeJoinState, etc.)
- [ ] All aggregate states (AggState, WindowAggState, etc.)

### Path Node Structures (pathnodes.h) - ALL MISSING
**Path Planning (80+ nodes):**
- [ ] **Path** - Base path node (`src/include/nodes/pathnodes.h:1621`)
- [ ] **IndexPath** - Index path (`src/include/nodes/pathnodes.h:1709`)
- [ ] **JoinPath** - Base join path (`src/include/nodes/pathnodes.h:2065`)
- [ ] **PlannerGlobal** - Global planner info (`src/include/nodes/pathnodes.h:95`)
- [ ] **PlannerInfo** - Planner info (`src/include/nodes/pathnodes.h:191`)
- [ ] **RelOptInfo** - Relation optimization info (`src/include/nodes/pathnodes.h:853`)
- [ ] All path types (MaterialPath, SortPath, AggPath, etc.)

### Additional Missing Nodes
**Miscellaneous Support Nodes:**
- [ ] **List** - Generic list (`src/include/nodes/pg_list.h:53`)
- [ ] **MemoryContextData** - Memory context (`src/include/nodes/memnodes.h:117`)
- [ ] **LocationLen** - Location and length (`src/include/nodes/queryjumble.h:22`)
- [ ] **ExtensibleNode** - Extensible node (`src/include/nodes/extensible.h:32`)
- [ ] **SubscriptRoutines** - Subscripting routines (`src/include/nodes/subscripting.h:158`)
- [ ] All replication nodes (12 nodes from replnodes.h)
- [ ] All support request nodes (7 nodes from supportnodes.h)

---

## Implementation Status by Source File

### parsenodes.h Progress
- **Total structs**: 196 nodes  
- **Implemented**: 67 nodes (34%)
- **Missing**: 129 nodes (66%)
- **Categories**: Parse tree nodes, DDL/DML statements, expressions

### primnodes.h Progress
- **Total structs**: 64 nodes
- **Implemented**: 51 nodes (80%)
- **Missing**: 13 nodes (20%)
- **Categories**: Primitive expressions, type coercion, execution support

### plannodes.h Progress
- **Total structs**: 66 nodes
- **Implemented**: 0 nodes (0%)
- **Missing**: 66 nodes (100%)
- **Categories**: Query plan representation

### execnodes.h Progress
- **Total structs**: 80 nodes
- **Implemented**: 0 nodes (0%)
- **Missing**: 80 nodes (100%)
- **Categories**: Query execution state

### pathnodes.h Progress
- **Total structs**: 81 nodes
- **Implemented**: 0 nodes (0%)
- **Missing**: 81 nodes (100%)
- **Categories**: Query planning and optimization

### value.h Progress
- **Total structs**: 5 nodes
- **Implemented**: 5 nodes (100%) ✅
- **Missing**: 0 nodes

### Other Files Progress
- **Total structs**: 41 nodes (misc support)
- **Implemented**: 6 nodes (15%)
- **Missing**: 35 nodes (85%)

---

## Massive Scope Reality Check

### The True Scale of Missing Work
**291 missing nodes** across multiple PostgreSQL subsystems:

1. **Parse Tree Completion** (129 missing parsenodes.h)
   - Core parsing expressions and statements
   - Essential for lexer/parser integration

2. **Primitive Expression System** (26 missing primnodes.h)
   - Advanced type system and expression evaluation
   - Critical for semantic analysis

3. **Query Planning Infrastructure** (66 missing plannodes.h)
   - Complete query planning system
   - Required for query optimization

4. **Execution Engine** (80 missing execnodes.h)
   - Query execution state management
   - Required for query execution

5. **Path Optimization** (81 missing pathnodes.h)
   - Query path planning and optimization
   - Required for efficient query execution

6. **Support Infrastructure** (40+ missing misc nodes)
   - Memory management, lists, extensions
   - Required for system integration

### Revised Implementation Strategy
**This is a much larger undertaking than originally estimated.**

**Phase 1.5a - Complete Parse Tree (130 nodes)**
- Focus only on parsenodes.h completion
- Essential for basic parsing functionality
- Enables lexer/parser development

**Phase 1.5b - Complete Primitive Expressions (37 nodes)**
- Complete primnodes.h implementation
- Required for expression evaluation
- Enables semantic analysis

**Phase 1.5c - Planning Infrastructure (66 nodes)**
- Implement plannodes.h nodes
- Required for query planning
- Major undertaking on its own

**Phase 1.5d - Execution Infrastructure (80 nodes)****
- Implement execnodes.h nodes
- Required for query execution
- Major undertaking on its own

**Phase 1.5e - Path Optimization (81 nodes)**
- Implement pathnodes.h nodes
- Required for optimization
- Major undertaking on its own

### Reality Assessment
**Current 21% completion means we have completed basic infrastructure only. The remaining 79% includes entire subsystems that are each major projects in themselves.**

**Recommendation**: Focus Phase 1.5 on completing just the parse tree (parsenodes.h) to enable lexer/parser development. The planning and execution nodes can be separate phases as they're not needed for basic parsing.**
