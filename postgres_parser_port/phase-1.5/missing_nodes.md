# Missing PostgreSQL AST Nodes - Complete Inventory

**Total Missing**: 358 nodes out of 456 total PostgreSQL AST nodes
**Current Implementation**: 98 nodes (21% complete)
**Target**: 100% PostgreSQL AST compatibility

This inventory is based on the comprehensive ast_structs_checklist.md which tracks all PostgreSQL node structures across parsenodes.h, primnodes.h, plannodes.h, execnodes.h, pathnodes.h, and supporting files.

---

## High Priority - Essential Missing Parse Nodes

### Core Parse Tree Structures (parsenodes.h)
These nodes are essential for basic SQL parsing functionality:

**Statement Nodes:**
- [ ] **RawStmt** - Raw statement wrapper (`src/include/nodes/parsenodes.h:2017`)
- [ ] **MergeStmt** - MERGE statement (`src/include/nodes/parsenodes.h:2084`)
- [ ] **SetOperationStmt** - UNION/INTERSECT/EXCEPT (`src/include/nodes/parsenodes.h:2185`)
- [ ] **ReturnStmt** - RETURN statement (`src/include/nodes/parsenodes.h:2210`)
- [ ] **PLAssignStmt** - PL assignment statement (`src/include/nodes/parsenodes.h:2224`)

**Expression Nodes:**
- [ ] **A_Expr** - Generic expression node (`src/include/nodes/parsenodes.h:329`)
- [ ] **A_Const** - Constant value (`src/include/nodes/parsenodes.h:357`)
- [ ] **ParamRef** - Parameter reference (`src/include/nodes/parsenodes.h:301`)
- [ ] **TypeCast** - Type casting (`src/include/nodes/parsenodes.h:370`)
- [ ] **FuncCall** - Function call (`src/include/nodes/parsenodes.h:423`)
- [ ] **A_Star** - Asterisk (*) (`src/include/nodes/parsenodes.h:445`)
- [ ] **A_Indices** - Array indices (`src/include/nodes/parsenodes.h:456`)
- [ ] **A_Indirection** - Indirection (field access) (`src/include/nodes/parsenodes.h:479`)
- [ ] **A_ArrayExpr** - Array expression (`src/include/nodes/parsenodes.h:489`)

**DDL Missing Nodes:**
- [ ] **CreateFunctionStmt** - CREATE FUNCTION (`src/include/nodes/parsenodes.h:3427`)
- [ ] **CreateSeqStmt** - CREATE SEQUENCE (`src/include/nodes/parsenodes.h:3117`)
- [ ] **CreateOpClassStmt** - CREATE OPERATOR CLASS (`src/include/nodes/parsenodes.h:3169`)
- [ ] **CreateOpFamilyStmt** - CREATE OPERATOR FAMILY (`src/include/nodes/parsenodes.h:3201`)
- [ ] **CreateCastStmt** - CREATE CAST (`src/include/nodes/parsenodes.h:4002`)
- [ ] **CreateConversionStmt** - CREATE CONVERSION (`src/include/nodes/parsenodes.h:3988`)
- [ ] **CreateTransformStmt** - CREATE TRANSFORM (`src/include/nodes/parsenodes.h:4016`)
- [ ] **DefineStmt** - DEFINE statement (`src/include/nodes/parsenodes.h:3140`)
- [ ] **TruncateStmt** - TRUNCATE statement (`src/include/nodes/parsenodes.h:3240`)
- [ ] **CommentStmt** - COMMENT statement (`src/include/nodes/parsenodes.h:3252`)
- [ ] **RenameStmt** - RENAME operations (`src/include/nodes/parsenodes.h:3525`)
- [ ] **AlterOwnerStmt** - ALTER OWNER (`src/include/nodes/parsenodes.h:3571`)
- [ ] **RuleStmt** - CREATE RULE (`src/include/nodes/parsenodes.h:3606`)

---

## Medium Priority - Core Missing Primitive Nodes

### Expression System (primnodes.h)
These nodes are essential for expression evaluation:

**Base Expression Types:**
- [ ] **GroupingFunc** - GROUPING function (`src/include/nodes/primnodes.h:537`)
- [ ] **WindowFuncRunCondition** - Window function run condition (`src/include/nodes/primnodes.h:596`)
- [ ] **MergeSupportFunc** - Merge support function (`src/include/nodes/primnodes.h:628`)
- [ ] **NamedArgExpr** - Named argument expression (`src/include/nodes/primnodes.h:787`)

**Control Flow and Test Nodes:**
- [ ] **CaseTestExpr** - CASE test expression (`src/include/nodes/primnodes.h:1352`)
- [ ] **MinMaxExpr** - MIN/MAX expression (`src/include/nodes/primnodes.h:1506`)
- [ ] **RowCompareExpr** - Row comparison (`src/include/nodes/primnodes.h:1463`)
- [ ] **SQLValueFunction** - SQL value function (`src/include/nodes/primnodes.h:1553`)
- [ ] **XmlExpr** - XML expression (`src/include/nodes/primnodes.h:1596`)
- [ ] **MergeAction** - MERGE action (`src/include/nodes/primnodes.h:2003`)

### Missing Table and Range Nodes
**Range Table Support:**
- [ ] **RangeTblEntry** - Range table entry (`src/include/nodes/parsenodes.h:1038`)
- [ ] **RangeSubselect** - Subquery in FROM (`src/include/nodes/parsenodes.h:615`)
- [ ] **RangeFunction** - Function in FROM (`src/include/nodes/parsenodes.h:637`)
- [ ] **RangeTableFunc** - Table function (`src/include/nodes/parsenodes.h:655`)
- [ ] **RangeTableFuncCol** - Table function column (`src/include/nodes/parsenodes.h:673`)
- [ ] **RangeTableSample** - TABLESAMPLE clause (`src/include/nodes/parsenodes.h:695`)
- [ ] **RangeTblFunction** - Range table function (`src/include/nodes/parsenodes.h:1317`)
- [ ] **RTEPermissionInfo** - Permission info for RTE (`src/include/nodes/parsenodes.h:1286`)

**Column and Table Definition:**
- [ ] **ColumnDef** - Column definition (`src/include/nodes/parsenodes.h:723`)
- [ ] **TypeName** - Type specification (`src/include/nodes/parsenodes.h:265`)
- [ ] **MultiAssignRef** - Multi-assignment reference (`src/include/nodes/parsenodes.h:532`)

---

## Lower Priority - Advanced and Specialized Nodes

### Complete JSON Expression System (parsenodes.h + primnodes.h)
**JSON Nodes (parsenodes.h):**
- [ ] **JsonOutput** - JSON output specification (`src/include/nodes/parsenodes.h:1751`)
- [ ] **JsonArgument** - JSON function argument (`src/include/nodes/parsenodes.h:1762`)
- [ ] **JsonFuncExpr** - JSON function expression (`src/include/nodes/parsenodes.h:1785`)
- [ ] **JsonTable** - JSON_TABLE (`src/include/nodes/parsenodes.h:1821`)
- [ ] **JsonTablePathSpec** - JSON table path specification (`src/include/nodes/parsenodes.h:1807`)
- [ ] **JsonTableColumn** - JSON table column (`src/include/nodes/parsenodes.h:1851`)
- [ ] **JsonKeyValue** - JSON key-value pair (`src/include/nodes/parsenodes.h:1872`)
- [ ] **JsonParseExpr** - JSON_PARSE expression (`src/include/nodes/parsenodes.h:1883`)
- [ ] **JsonScalarExpr** - JSON scalar expression (`src/include/nodes/parsenodes.h:1896`)
- [ ] **JsonSerializeExpr** - JSON_SERIALIZE expression (`src/include/nodes/parsenodes.h:1908`)
- [ ] **JsonObjectConstructor** - JSON object constructor (`src/include/nodes/parsenodes.h:1920`)
- [ ] **JsonArrayConstructor** - JSON array constructor (`src/include/nodes/parsenodes.h:1934`)
- [ ] **JsonArrayQueryConstructor** - JSON array query constructor (`src/include/nodes/parsenodes.h:1947`)
- [ ] **JsonAggConstructor** - JSON aggregate constructor (`src/include/nodes/parsenodes.h:1962`)
- [ ] **JsonObjectAgg** - JSON_OBJECTAGG (`src/include/nodes/parsenodes.h:1976`)
- [ ] **JsonArrayAgg** - JSON_ARRAYAGG (`src/include/nodes/parsenodes.h:1989`)

**JSON Nodes (primnodes.h):**
- [ ] **JsonFormat** - JSON format specification (`src/include/nodes/primnodes.h:1648`)
- [ ] **JsonReturning** - JSON RETURNING clause (`src/include/nodes/primnodes.h:1660`)
- [ ] **JsonValueExpr** - JSON value expression (`src/include/nodes/primnodes.h:1680`)
- [ ] **JsonConstructorExpr** - JSON constructor expression (`src/include/nodes/primnodes.h:1703`)
- [ ] **JsonIsPredicate** - JSON IS predicate (`src/include/nodes/primnodes.h:1732`)
- [ ] **JsonBehavior** - JSON behavior specification (`src/include/nodes/primnodes.h:1786`)
- [ ] **JsonExpr** - JSON expression (`src/include/nodes/primnodes.h:1813`)
- [ ] **JsonTablePath** - JSON table path (`src/include/nodes/primnodes.h:1867`)
- [ ] **JsonTablePlan** - JSON table plan (`src/include/nodes/primnodes.h:1882`)
- [ ] **JsonTablePathScan** - JSON table path scan (`src/include/nodes/primnodes.h:1893`)
- [ ] **JsonTableSiblingJoin** - JSON table sibling join (`src/include/nodes/primnodes.h:1923`)

### Missing Utility Statements (parsenodes.h)
- [ ] **LoadStmt** - LOAD statement (`src/include/nodes/parsenodes.h:3755`)
- [ ] **ClusterStmt** - CLUSTER statement (`src/include/nodes/parsenodes.h:3822`)
- [ ] **LockStmt** - LOCK statement (`src/include/nodes/parsenodes.h:3942`)
- [ ] **DeclareCursorStmt** - DECLARE CURSOR (`src/include/nodes/parsenodes.h:3293`)
- [ ] **FetchStmt** - FETCH statement (`src/include/nodes/parsenodes.h:3328`)
- [ ] **ClosePortalStmt** - CLOSE statement (`src/include/nodes/parsenodes.h:3305`)
- [ ] **NotifyStmt** - NOTIFY statement (`src/include/nodes/parsenodes.h:3622`)
- [ ] **ListenStmt** - LISTEN statement (`src/include/nodes/parsenodes.h:3633`)
- [ ] **UnlistenStmt** - UNLISTEN statement (`src/include/nodes/parsenodes.h:3643`)
- [ ] **CheckPointStmt** - CHECKPOINT statement (`src/include/nodes/parsenodes.h:3914`)
- [ ] **DiscardStmt** - DISCARD statement (`src/include/nodes/parsenodes.h:3932`)

### Window and Grouping Support (parsenodes.h)
- [ ] **WindowDef** - Window definition (`src/include/nodes/parsenodes.h:561`)
- [ ] **SortBy** - Sort specification (`src/include/nodes/parsenodes.h:543`)
- [ ] **GroupingSet** - Grouping set (`src/include/nodes/parsenodes.h:1506`)
- [ ] **WithClause** - WITH clause (`src/include/nodes/parsenodes.h:1592`)
- [ ] **OnConflictClause** - ON CONFLICT clause (`src/include/nodes/parsenodes.h:1621`)
- [ ] **InferClause** - Inference clause (`src/include/nodes/parsenodes.h:1606`)
- [ ] **WithCheckOption** - WITH CHECK OPTION (`src/include/nodes/parsenodes.h:1368`)
- [ ] **MergeWhenClause** - WHEN clause in MERGE (`src/include/nodes/parsenodes.h:1717`)

### Miscellaneous Parse Nodes (parsenodes.h)
- [ ] **LockingClause** - Locking clause (FOR UPDATE, etc.) (`src/include/nodes/parsenodes.h:831`)
- [ ] **XmlSerialize** - XML serialization (`src/include/nodes/parsenodes.h:842`)
- [ ] **PartitionElem** - Partition element (`src/include/nodes/parsenodes.h:860`)
- [ ] **SinglePartitionSpec** - Single partition specification (`src/include/nodes/parsenodes.h:945`)
- [ ] **PartitionCmd** - Partition command (`src/include/nodes/parsenodes.h:953`)
- [ ] **TableSampleClause** - Table sample clause (`src/include/nodes/parsenodes.h:1344`)
- [ ] **ObjectWithArgs** - Object with arguments (`src/include/nodes/parsenodes.h:2524`)

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
- **Total structs**: 180 nodes
- **Implemented**: 50 nodes (28%)
- **Missing**: 130 nodes (72%)
- **Categories**: Parse tree nodes, DDL/DML statements, expressions

### primnodes.h Progress
- **Total structs**: 74 nodes
- **Implemented**: 37 nodes (50%)
- **Missing**: 37 nodes (50%)
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
- **Total structs**: 46 nodes (misc support)
- **Implemented**: 6 nodes (13%)
- **Missing**: 40 nodes (87%)

---

## Massive Scope Reality Check

### The True Scale of Missing Work
**358 missing nodes** across multiple PostgreSQL subsystems:

1. **Parse Tree Completion** (130 missing parsenodes.h)
   - Core parsing expressions and statements
   - Essential for lexer/parser integration

2. **Primitive Expression System** (37 missing primnodes.h)
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
