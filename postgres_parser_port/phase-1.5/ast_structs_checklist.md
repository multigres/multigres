# PostgreSQL AST Struct Implementation Checklist

This checklist tracks the implementation status of all PostgreSQL AST structs for the parser port project.

## Parse Tree Node Structures (parsenodes.h)

### Core Query Structure
- [ ] **Query** - The main structure representing a parsed SQL query (`src/include/nodes/parsenodes.h:117`)
- [ ] **RawStmt** - Raw statement wrapper (`src/include/nodes/parsenodes.h:2017`)

### Expression Nodes
- [ ] **A_Expr** - Generic expression node (`src/include/nodes/parsenodes.h:329`)
- [ ] **A_Const** - Constant value (`src/include/nodes/parsenodes.h:357`)
- [ ] **ColumnRef** - Column reference (`src/include/nodes/parsenodes.h:291`)
- [ ] **ParamRef** - Parameter reference (`src/include/nodes/parsenodes.h:301`)
- [ ] **TypeCast** - Type casting (`src/include/nodes/parsenodes.h:370`)
- [ ] **CollateClause** - COLLATE clause (`src/include/nodes/parsenodes.h:381`)
- [ ] **FuncCall** - Function call (`src/include/nodes/parsenodes.h:423`)
- [ ] **A_Star** - Asterisk (*) (`src/include/nodes/parsenodes.h:445`)
- [ ] **A_Indices** - Array indices (`src/include/nodes/parsenodes.h:456`)
- [ ] **A_Indirection** - Indirection (field access) (`src/include/nodes/parsenodes.h:479`)
- [ ] **A_ArrayExpr** - Array expression (`src/include/nodes/parsenodes.h:489`)

### Statement Nodes
- [ ] **SelectStmt** - SELECT statement (`src/include/nodes/parsenodes.h:2116`)
- [ ] **InsertStmt** - INSERT statement (`src/include/nodes/parsenodes.h:2039`)
- [ ] **DeleteStmt** - DELETE statement (`src/include/nodes/parsenodes.h:2055`)
- [ ] **UpdateStmt** - UPDATE statement (`src/include/nodes/parsenodes.h:2069`)
- [ ] **MergeStmt** - MERGE statement (`src/include/nodes/parsenodes.h:2084`)
- [ ] **SetOperationStmt** - UNION/INTERSECT/EXCEPT (`src/include/nodes/parsenodes.h:2185`)
- [ ] **ReturnStmt** - RETURN statement (`src/include/nodes/parsenodes.h:2210`)
- [ ] **PLAssignStmt** - PL assignment statement (`src/include/nodes/parsenodes.h:2224`)

### DDL Statement Nodes
- [ ] **CreateStmt** - CREATE TABLE (`src/include/nodes/parsenodes.h:2648`)
- [ ] **CreateSchemaStmt** - CREATE SCHEMA (`src/include/nodes/parsenodes.h:2320`)
- [ ] **AlterTableStmt** - ALTER TABLE (`src/include/nodes/parsenodes.h:2339`)
- [ ] **AlterTableCmd** - ALTER TABLE subcommand (`src/include/nodes/parsenodes.h:2426`)
- [ ] **CreateExtensionStmt** - CREATE EXTENSION (`src/include/nodes/parsenodes.h:2819`)
- [x] **CreateForeignServerStmt** - CREATE FOREIGN SERVER (`src/include/nodes/parsenodes.h:2870`)
- [x] **CreateForeignTableStmt** - CREATE FOREIGN TABLE (`src/include/nodes/parsenodes.h:2895`)
- [x] **CreateUserMappingStmt** - CREATE USER MAPPING (`src/include/nodes/parsenodes.h:2907`)
- [x] **CreatePolicyStmt** - CREATE POLICY (`src/include/nodes/parsenodes.h:2959`)
- [x] **AlterPolicyStmt** - ALTER POLICY (`src/include/nodes/parsenodes.h:2975`)
- [x] **CreateTrigStmt** - CREATE TRIGGER (`src/include/nodes/parsenodes.h:3001`) _(Go: CreateTriggerStmt)_
- [x] **StatsElem** - Statistics element specification (`src/include/nodes/parsenodes.h:3403`)
- [ ] **CreateFunctionStmt** - CREATE FUNCTION (`src/include/nodes/parsenodes.h:3427`)
- [ ] **CreateRoleStmt** - CREATE ROLE (`src/include/nodes/parsenodes.h:3081`)
- [ ] **CreateSeqStmt** - CREATE SEQUENCE (`src/include/nodes/parsenodes.h:3117`)
- [ ] **CreateDomainStmt** - CREATE DOMAIN (`src/include/nodes/parsenodes.h:3156`)
- [ ] **CreateOpClassStmt** - CREATE OPERATOR CLASS (`src/include/nodes/parsenodes.h:3169`)
- [ ] **CreateOpFamilyStmt** - CREATE OPERATOR FAMILY (`src/include/nodes/parsenodes.h:3201`)
- [ ] **CreateCastStmt** - CREATE CAST (`src/include/nodes/parsenodes.h:4002`)
- [ ] **CreateConversionStmt** - CREATE CONVERSION (`src/include/nodes/parsenodes.h:3988`)
- [ ] **CreateTransformStmt** - CREATE TRANSFORM (`src/include/nodes/parsenodes.h:4016`)
- [ ] **DefineStmt** - DEFINE statement (`src/include/nodes/parsenodes.h:3140`)
- [ ] **DropStmt** - DROP statement (`src/include/nodes/parsenodes.h:3226`)
- [ ] **TruncateStmt** - TRUNCATE statement (`src/include/nodes/parsenodes.h:3240`)
- [ ] **CommentStmt** - COMMENT statement (`src/include/nodes/parsenodes.h:3252`)
- [ ] **RenameStmt** - RENAME operations (`src/include/nodes/parsenodes.h:3525`)
- [ ] **AlterOwnerStmt** - ALTER OWNER (`src/include/nodes/parsenodes.h:3571`)
- [ ] **RuleStmt** - CREATE RULE (`src/include/nodes/parsenodes.h:3606`)
- [ ] **ViewStmt** - CREATE VIEW (`src/include/nodes/parsenodes.h:3740`)

### Utility Statement Nodes
- [ ] **VariableSetStmt** - SET statement (`src/include/nodes/parsenodes.h:2618`)
- [ ] **VariableShowStmt** - SHOW statement (`src/include/nodes/parsenodes.h:2631`)
- [ ] **CopyStmt** - COPY statement (`src/include/nodes/parsenodes.h:2586`)
- [ ] **GrantStmt** - GRANT statement (`src/include/nodes/parsenodes.h:2491`)
- [ ] **GrantRoleStmt** - GRANT role statement (`src/include/nodes/parsenodes.h:2556`)
- [ ] **TransactionStmt** - Transaction control (`src/include/nodes/parsenodes.h:3667`)
- [ ] **VacuumStmt** - VACUUM statement (`src/include/nodes/parsenodes.h:3837`)
- [ ] **ExplainStmt** - EXPLAIN statement (`src/include/nodes/parsenodes.h:3868`)
- [ ] **LoadStmt** - LOAD statement (`src/include/nodes/parsenodes.h:3755`)
- [ ] **ClusterStmt** - CLUSTER statement (`src/include/nodes/parsenodes.h:3822`)
- [ ] **LockStmt** - LOCK statement (`src/include/nodes/parsenodes.h:3942`)
- [ ] **PrepareStmt** - PREPARE statement (`src/include/nodes/parsenodes.h:4030`)
- [ ] **ExecuteStmt** - EXECUTE statement (`src/include/nodes/parsenodes.h:4044`)
- [ ] **DeallocateStmt** - DEALLOCATE statement (`src/include/nodes/parsenodes.h:4056`)
- [ ] **DeclareCursorStmt** - DECLARE CURSOR (`src/include/nodes/parsenodes.h:3293`)
- [ ] **FetchStmt** - FETCH statement (`src/include/nodes/parsenodes.h:3328`)
- [ ] **ClosePortalStmt** - CLOSE statement (`src/include/nodes/parsenodes.h:3305`)
- [ ] **NotifyStmt** - NOTIFY statement (`src/include/nodes/parsenodes.h:3622`)
- [ ] **ListenStmt** - LISTEN statement (`src/include/nodes/parsenodes.h:3633`)
- [ ] **UnlistenStmt** - UNLISTEN statement (`src/include/nodes/parsenodes.h:3643`)
- [ ] **CheckPointStmt** - CHECKPOINT statement (`src/include/nodes/parsenodes.h:3914`)
- [ ] **DiscardStmt** - DISCARD statement (`src/include/nodes/parsenodes.h:3932`)
- [ ] **ReindexStmt** - REINDEX statement (`src/include/nodes/parsenodes.h:3974`)

### Table and Column Definition Nodes
- [ ] **ColumnDef** - Column definition (`src/include/nodes/parsenodes.h:723`)
- [x] **TableLikeClause** - LIKE clause in CREATE TABLE (`src/include/nodes/parsenodes.h:751`)
- [ ] **IndexElem** - Index element (`src/include/nodes/parsenodes.h:780`)
- [ ] **Constraint** - Table/column constraint (`src/include/nodes/parsenodes.h:2728`)
- [ ] **DefElem** - Definition element (`src/include/nodes/parsenodes.h:811`)

### Range Table and FROM Clause Nodes
- [ ] **RangeTblEntry** - Range table entry (`src/include/nodes/parsenodes.h:1038`)
- [ ] **RangeSubselect** - Subquery in FROM (`src/include/nodes/parsenodes.h:615`)
- [ ] **RangeFunction** - Function in FROM (`src/include/nodes/parsenodes.h:637`)
- [ ] **RangeTableFunc** - Table function (`src/include/nodes/parsenodes.h:655`)
- [ ] **RangeTableFuncCol** - Table function column (`src/include/nodes/parsenodes.h:673`)
- [ ] **RangeTableSample** - TABLESAMPLE clause (`src/include/nodes/parsenodes.h:695`)
- [ ] **RangeTblFunction** - Range table function (`src/include/nodes/parsenodes.h:1317`)
- [ ] **RTEPermissionInfo** - Permission info for RTE (`src/include/nodes/parsenodes.h:1286`)

### Window and Grouping Nodes
- [ ] **WindowDef** - Window definition (`src/include/nodes/parsenodes.h:561`)
- [ ] **WindowClause** - Window clause (`src/include/nodes/parsenodes.h:1536`)
- [ ] **SortBy** - Sort specification (`src/include/nodes/parsenodes.h:543`)
- [ ] **SortGroupClause** - Sort/group clause (`src/include/nodes/parsenodes.h:1436`)
- [ ] **GroupingSet** - Grouping set (`src/include/nodes/parsenodes.h:1506`)

### CTE and WITH Clause Nodes
- [ ] **WithClause** - WITH clause (`src/include/nodes/parsenodes.h:1592`)
- [ ] **CommonTableExpr** - Common table expression (`src/include/nodes/parsenodes.h:1668`)
- [ ] **CTESearchClause** - SEARCH clause in CTE (`src/include/nodes/parsenodes.h:1643`)
- [ ] **CTECycleClause** - CYCLE clause in CTE (`src/include/nodes/parsenodes.h:1652`)

### Conflict Resolution Nodes
- [ ] **OnConflictClause** - ON CONFLICT clause (`src/include/nodes/parsenodes.h:1621`)
- [ ] **InferClause** - Inference clause (`src/include/nodes/parsenodes.h:1606`)
- [ ] **WithCheckOption** - WITH CHECK OPTION (`src/include/nodes/parsenodes.h:1368`)

### JSON Nodes
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

### Merge Statement Nodes
- [ ] **MergeWhenClause** - WHEN clause in MERGE (`src/include/nodes/parsenodes.h:1717`)

### Miscellaneous Nodes
- [ ] **TypeName** - Type specification (`src/include/nodes/parsenodes.h:265`)
- [ ] **ResTarget** - Result target (`src/include/nodes/parsenodes.h:514`)
- [ ] **MultiAssignRef** - Multi-assignment reference (`src/include/nodes/parsenodes.h:532`)
- [ ] **LockingClause** - Locking clause (FOR UPDATE, etc.) (`src/include/nodes/parsenodes.h:831`)
- [ ] **XmlSerialize** - XML serialization (`src/include/nodes/parsenodes.h:842`)
- [ ] **PartitionElem** - Partition element (`src/include/nodes/parsenodes.h:860`)
- [x] **PartitionSpec** - Partition specification (`src/include/nodes/parsenodes.h:882`)
- [x] **PartitionBoundSpec** - Partition boundary specification (`src/include/nodes/parsenodes.h:896`)
- [x] **PartitionRangeDatum** - Partition range datum (`src/include/nodes/parsenodes.h:929`)
- [ ] **SinglePartitionSpec** - Single partition specification (`src/include/nodes/parsenodes.h:945`)
- [ ] **PartitionCmd** - Partition command (`src/include/nodes/parsenodes.h:953`)
- [ ] **TableSampleClause** - Table sample clause (`src/include/nodes/parsenodes.h:1344`)
- [ ] **RowMarkClause** - Row marking clause (`src/include/nodes/parsenodes.h:1576`)
- [x] **TriggerTransition** - Trigger transition (`src/include/nodes/parsenodes.h:1737`)
- [ ] **RoleSpec** - Role specification (`src/include/nodes/parsenodes.h:401`)
- [ ] **ObjectWithArgs** - Object with arguments (`src/include/nodes/parsenodes.h:2524`)
- [ ] **AccessPriv** - Access privilege (`src/include/nodes/parsenodes.h:2540`)

## Primitive Node Structures (primnodes.h)

### Core Expression Types
- [ ] **Expr** - Base expression node (`src/include/nodes/primnodes.h:187`)
- [ ] **Var** - Variable reference (`src/include/nodes/primnodes.h:247`)
- [ ] **Const** - Constant value (`src/include/nodes/primnodes.h:306`)
- [ ] **Param** - Parameter (`src/include/nodes/primnodes.h:373`)

### Function and Operator Nodes
- [ ] **Aggref** - Aggregate function reference (`src/include/nodes/primnodes.h:439`)
- [ ] **GroupingFunc** - GROUPING function (`src/include/nodes/primnodes.h:537`)
- [ ] **WindowFunc** - Window function (`src/include/nodes/primnodes.h:563`)
- [ ] **WindowFuncRunCondition** - Window function run condition (`src/include/nodes/primnodes.h:596`)
- [ ] **MergeSupportFunc** - Merge support function (`src/include/nodes/primnodes.h:628`)
- [ ] **FuncExpr** - Function expression (`src/include/nodes/primnodes.h:746`)
- [ ] **NamedArgExpr** - Named argument expression (`src/include/nodes/primnodes.h:787`)
- [ ] **OpExpr** - Operator expression (`src/include/nodes/primnodes.h:813`)
- [ ] **ScalarArrayOpExpr** - Scalar array operator expression (`src/include/nodes/primnodes.h:893`)
- [ ] **BoolExpr** - Boolean expression (AND/OR/NOT) (`src/include/nodes/primnodes.h:934`)

### Subquery and Subplan Nodes
- [ ] **SubLink** - Subquery link (`src/include/nodes/primnodes.h:1008`)
- [ ] **SubPlan** - Subplan (`src/include/nodes/primnodes.h:1059`)
- [ ] **AlternativeSubPlan** - Alternative subplan (`src/include/nodes/primnodes.h:1108`)

### Type Coercion Nodes
- [ ] **RelabelType** - Type relabeling (`src/include/nodes/primnodes.h:1181`)
- [ ] **CoerceViaIO** - Coercion via I/O (`src/include/nodes/primnodes.h:1204`)
- [ ] **ArrayCoerceExpr** - Array coercion (`src/include/nodes/primnodes.h:1230`)
- [ ] **ConvertRowtypeExpr** - Row type conversion (`src/include/nodes/primnodes.h:1258`)
- [ ] **CollateExpr** - Collation expression (`src/include/nodes/primnodes.h:1276`)
- [ ] **CoerceToDomain** - Domain coercion (`src/include/nodes/primnodes.h:2025`)
- [ ] **CoerceToDomainValue** - Domain coercion value (`src/include/nodes/primnodes.h:2048`)

### Field and Array Operations
- [ ] **FieldSelect** - Field selection (`src/include/nodes/primnodes.h:1125`)
- [ ] **FieldStore** - Field store (`src/include/nodes/primnodes.h:1156`)
- [ ] **SubscriptingRef** - Array/subscript reference (`src/include/nodes/primnodes.h:679`)
- [ ] **ArrayExpr** - Array expression (`src/include/nodes/primnodes.h:1370`)

### Control Flow Nodes
- [ ] **CaseExpr** - CASE expression (`src/include/nodes/primnodes.h:1306`)
- [ ] **CaseWhen** - WHEN clause in CASE (`src/include/nodes/primnodes.h:1322`)
- [ ] **CaseTestExpr** - CASE test expression (`src/include/nodes/primnodes.h:1352`)
- [ ] **CoalesceExpr** - COALESCE expression (`src/include/nodes/primnodes.h:1484`)
- [ ] **MinMaxExpr** - MIN/MAX expression (`src/include/nodes/primnodes.h:1506`)

### Row and Comparison Operations
- [ ] **RowExpr** - Row expression (`src/include/nodes/primnodes.h:1408`)
- [ ] **RowCompareExpr** - Row comparison (`src/include/nodes/primnodes.h:1463`)

### Built-in Functions
- [ ] **SQLValueFunction** - SQL value function (`src/include/nodes/primnodes.h:1553`)
- [ ] **XmlExpr** - XML expression (`src/include/nodes/primnodes.h:1596`)

### JSON Expression Nodes
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

### Test and Validation Nodes
- [ ] **NullTest** - NULL test (`src/include/nodes/primnodes.h:1955`)
- [ ] **BooleanTest** - Boolean test (`src/include/nodes/primnodes.h:1979`)
- [ ] **MergeAction** - MERGE action (`src/include/nodes/primnodes.h:2003`)

### Special Value Nodes
- [ ] **SetToDefault** - SET TO DEFAULT (`src/include/nodes/primnodes.h:2068`)
- [ ] **CurrentOfExpr** - CURRENT OF expression (`src/include/nodes/primnodes.h:2094`)
- [ ] **NextValueExpr** - NEXTVAL expression (`src/include/nodes/primnodes.h:2109`)
- [ ] **InferenceElem** - Inference element (`src/include/nodes/primnodes.h:2123`)

### Target and Reference Nodes
- [ ] **TargetEntry** - Target entry (`src/include/nodes/primnodes.h:2186`)
- [ ] **RangeTblRef** - Range table reference (`src/include/nodes/primnodes.h:2243`)
- [ ] **JoinExpr** - JOIN expression (`src/include/nodes/primnodes.h:2277`)
- [ ] **FromExpr** - FROM expression (`src/include/nodes/primnodes.h:2305`)
- [ ] **OnConflictExpr** - ON CONFLICT expression (`src/include/nodes/primnodes.h:2321`)

### Utility Nodes
- [ ] **Alias** - Alias specification (`src/include/nodes/primnodes.h:47`)
- [ ] **RangeVar** - Range variable (`src/include/nodes/primnodes.h:71`)
- [ ] **TableFunc** - Table function (`src/include/nodes/primnodes.h:109`)
- [ ] **IntoClause** - INTO clause (`src/include/nodes/primnodes.h:158`)

## Plan Node Structures (plannodes.h)

### Core Plan Nodes
- [ ] **PlannedStmt** - Planned statement (`src/include/nodes/plannodes.h:46`)
- [ ] **Plan** - Base plan node (`src/include/nodes/plannodes.h:119`)
- [ ] **Result** - Result plan (`src/include/nodes/plannodes.h:196`)
- [ ] **ProjectSet** - Project set plan (`src/include/nodes/plannodes.h:208`)
- [ ] **ModifyTable** - Modify table plan (`src/include/nodes/plannodes.h:229`)

### Scan Plan Nodes
- [ ] **Scan** - Base scan plan (`src/include/nodes/plannodes.h:384`)
- [ ] **SeqScan** - Sequential scan (`src/include/nodes/plannodes.h:396`)
- [ ] **SampleScan** - Sample scan (`src/include/nodes/plannodes.h:405`)
- [ ] **IndexScan** - Index scan (`src/include/nodes/plannodes.h:449`)
- [ ] **IndexOnlyScan** - Index-only scan (`src/include/nodes/plannodes.h:492`)
- [ ] **BitmapIndexScan** - Bitmap index scan (`src/include/nodes/plannodes.h:520`)
- [ ] **BitmapHeapScan** - Bitmap heap scan (`src/include/nodes/plannodes.h:538`)
- [ ] **TidScan** - TID scan (`src/include/nodes/plannodes.h:552`)
- [ ] **TidRangeScan** - TID range scan (`src/include/nodes/plannodes.h:565`)
- [ ] **SubqueryScan** - Subquery scan (`src/include/nodes/plannodes.h:598`)
- [ ] **FunctionScan** - Function scan (`src/include/nodes/plannodes.h:609`)
- [ ] **ValuesScan** - VALUES scan (`src/include/nodes/plannodes.h:620`)
- [ ] **TableFuncScan** - Table function scan (`src/include/nodes/plannodes.h:630`)
- [ ] **CteScan** - CTE scan (`src/include/nodes/plannodes.h:640`)
- [ ] **NamedTuplestoreScan** - Named tuplestore scan (`src/include/nodes/plannodes.h:651`)
- [ ] **WorkTableScan** - Work table scan (`src/include/nodes/plannodes.h:661`)
- [ ] **ForeignScan** - Foreign scan (`src/include/nodes/plannodes.h:707`)
- [ ] **CustomScan** - Custom scan (`src/include/nodes/plannodes.h:739`)

### Join Plan Nodes
- [ ] **Join** - Base join plan (`src/include/nodes/plannodes.h:786`)
- [ ] **NestLoop** - Nested loop join (`src/include/nodes/plannodes.h:807`)
- [ ] **NestLoopParam** - Nested loop parameter (`src/include/nodes/plannodes.h:813`)
- [ ] **MergeJoin** - Merge join (`src/include/nodes/plannodes.h:833`)
- [ ] **HashJoin** - Hash join (`src/include/nodes/plannodes.h:862`)

### Set Operation Plan Nodes
- [ ] **Append** - Append plan (`src/include/nodes/plannodes.h:265`)
- [ ] **MergeAppend** - Merge append plan (`src/include/nodes/plannodes.h:287`)
- [ ] **RecursiveUnion** - Recursive union plan (`src/include/nodes/plannodes.h:325`)
- [ ] **BitmapAnd** - Bitmap AND plan (`src/include/nodes/plannodes.h:356`)
- [ ] **BitmapOr** - Bitmap OR plan (`src/include/nodes/plannodes.h:370`)

### Sorting and Grouping Plan Nodes
- [ ] **Sort** - Sort plan (`src/include/nodes/plannodes.h:931`)
- [ ] **IncrementalSort** - Incremental sort plan (`src/include/nodes/plannodes.h:955`)
- [ ] **Group** - Group plan (`src/include/nodes/plannodes.h:967`)
- [ ] **Agg** - Aggregate plan (`src/include/nodes/plannodes.h:996`)
- [ ] **WindowAgg** - Window aggregate plan (`src/include/nodes/plannodes.h:1038`)
- [ ] **Unique** - Unique plan (`src/include/nodes/plannodes.h:1112`)

### Parallelism Plan Nodes
- [ ] **Gather** - Gather plan (`src/include/nodes/plannodes.h:1140`)
- [ ] **GatherMerge** - Gather merge plan (`src/include/nodes/plannodes.h:1155`)

### Utility Plan Nodes
- [ ] **Material** - Material plan (`src/include/nodes/plannodes.h:880`)
- [ ] **Memoize** - Memoize plan (`src/include/nodes/plannodes.h:889`)
- [ ] **Hash** - Hash plan (`src/include/nodes/plannodes.h:1197`)
- [ ] **SetOp** - Set operation plan (`src/include/nodes/plannodes.h:1217`)
- [ ] **LockRows** - Lock rows plan (`src/include/nodes/plannodes.h:1256`)
- [ ] **Limit** - Limit plan (`src/include/nodes/plannodes.h:1270`)

### Plan Support Structures
- [ ] **PlanRowMark** - Plan row mark (`src/include/nodes/plannodes.h:1377`)
- [ ] **PartitionPruneInfo** - Partition pruning info (`src/include/nodes/plannodes.h:1423`)
- [ ] **PartitionedRelPruneInfo** - Partitioned relation pruning info (`src/include/nodes/plannodes.h:1449`)
- [ ] **PartitionPruneStep** - Partition pruning step (`src/include/nodes/plannodes.h:1492`)
- [ ] **PartitionPruneStepOp** - Partition pruning step operation (`src/include/nodes/plannodes.h:1527`)
- [ ] **PartitionPruneStepCombine** - Partition pruning step combine (`src/include/nodes/plannodes.h:1549`)
- [ ] **PlanInvalItem** - Plan invalidation item (`src/include/nodes/plannodes.h:1567`)

## Execution Node Structures (execnodes.h)

### Core Execution State
- [ ] **ExprState** - Expression state (`src/include/nodes/execnodes.h:78`)
- [ ] **PlanState** - Plan state (`src/include/nodes/execnodes.h:1115`)
- [ ] **EState** - Executor state (`src/include/nodes/execnodes.h:623`)

### Expression Evaluation
- [ ] **ExprContext** - Expression context (`src/include/nodes/execnodes.h:251`)
- [ ] **ExprContext_CB** - Expression context callback (`src/include/nodes/execnodes.h:221`)
- [ ] **ProjectionInfo** - Projection info (`src/include/nodes/execnodes.h:360`)
- [ ] **JunkFilter** - Junk filter (`src/include/nodes/execnodes.h:393`)

### Scan States
- [ ] **ScanState** - Base scan state (`src/include/nodes/execnodes.h:1566`)
- [ ] **SeqScanState** - Sequential scan state (`src/include/nodes/execnodes.h:1578`)
- [ ] **SampleScanState** - Sample scan state (`src/include/nodes/execnodes.h:1588`)
- [ ] **IndexScanState** - Index scan state (`src/include/nodes/execnodes.h:1653`)
- [ ] **IndexOnlyScanState** - Index-only scan state (`src/include/nodes/execnodes.h:1701`)
- [ ] **BitmapIndexScanState** - Bitmap index scan state (`src/include/nodes/execnodes.h:1738`)
- [ ] **BitmapHeapScanState** - Bitmap heap scan state (`src/include/nodes/execnodes.h:1817`)
- [ ] **TidScanState** - TID scan state (`src/include/nodes/execnodes.h:1847`)
- [ ] **TidRangeScanState** - TID range scan state (`src/include/nodes/execnodes.h:1866`)
- [ ] **SubqueryScanState** - Subquery scan state (`src/include/nodes/execnodes.h:1882`)
- [ ] **FunctionScanState** - Function scan state (`src/include/nodes/execnodes.h:1906`)
- [ ] **ValuesScanState** - VALUES scan state (`src/include/nodes/execnodes.h:1942`)
- [ ] **TableFuncScanState** - Table function scan state (`src/include/nodes/execnodes.h:1958`)
- [ ] **CteScanState** - CTE scan state (`src/include/nodes/execnodes.h:1989`)
- [ ] **NamedTuplestoreScanState** - Named tuplestore scan state (`src/include/nodes/execnodes.h:2012`)
- [ ] **WorkTableScanState** - Work table scan state (`src/include/nodes/execnodes.h:2028`)
- [ ] **ForeignScanState** - Foreign scan state (`src/include/nodes/execnodes.h:2040`)
- [ ] **CustomScanState** - Custom scan state (`src/include/nodes/execnodes.h:2066`)

### Join States
- [ ] **JoinState** - Base join state (`src/include/nodes/execnodes.h:2088`)
- [ ] **NestLoopState** - Nested loop state (`src/include/nodes/execnodes.h:2105`)
- [ ] **MergeJoinState** - Merge join state (`src/include/nodes/execnodes.h:2138`)
- [ ] **HashJoinState** - Hash join state (`src/include/nodes/execnodes.h:2191`)

### Aggregate and Window States
- [ ] **AggState** - Aggregate state (`src/include/nodes/execnodes.h:2465`)
- [ ] **WindowAggState** - Window aggregate state (`src/include/nodes/execnodes.h:2561`)
- [ ] **GroupState** - Group state (`src/include/nodes/execnodes.h:2418`)

### Utility States
- [ ] **MaterialState** - Material state (`src/include/nodes/execnodes.h:2228`)
- [ ] **MemoizeState** - Memoize state (`src/include/nodes/execnodes.h:2272`)
- [ ] **SortState** - Sort state (`src/include/nodes/execnodes.h:2334`)
- [ ] **IncrementalSortState** - Incremental sort state (`src/include/nodes/execnodes.h:2391`)
- [ ] **UniqueState** - Unique state (`src/include/nodes/execnodes.h:2658`)
- [ ] **HashState** - Hash state (`src/include/nodes/execnodes.h:2746`)
- [ ] **SetOpState** - Set operation state (`src/include/nodes/execnodes.h:2783`)
- [ ] **LockRowsState** - Lock rows state (`src/include/nodes/execnodes.h:2807`)
- [ ] **LimitState** - Limit state (`src/include/nodes/execnodes.h:2838`)
- [ ] **GatherState** - Gather state (`src/include/nodes/execnodes.h:2671`)
- [ ] **GatherMergeState** - Gather merge state (`src/include/nodes/execnodes.h:2697`)

### Set Operation States
- [ ] **AppendState** - Append state (`src/include/nodes/execnodes.h:1436`)
- [ ] **MergeAppendState** - Merge append state (`src/include/nodes/execnodes.h:1485`)
- [ ] **RecursiveUnionState** - Recursive union state (`src/include/nodes/execnodes.h:1510`)
- [ ] **BitmapAndState** - Bitmap AND state (`src/include/nodes/execnodes.h:1529`)
- [ ] **BitmapOrState** - Bitmap OR state (`src/include/nodes/execnodes.h:1540`)

### Modify Table States
- [ ] **ModifyTableState** - Modify table state (`src/include/nodes/execnodes.h:1357`)
- [ ] **ResultRelInfo** - Result relation info (`src/include/nodes/execnodes.h:450`)
- [ ] **OnConflictSetState** - ON CONFLICT SET state (`src/include/nodes/execnodes.h:407`)
- [ ] **MergeActionState** - MERGE action state (`src/include/nodes/execnodes.h:423`)

### Support Structures
- [ ] **IndexInfo** - Index info (`src/include/nodes/execnodes.h:183`)
- [ ] **ReturnSetInfo** - Return set info (`src/include/nodes/execnodes.h:330`)
- [ ] **ExecRowMark** - Execution row mark (`src/include/nodes/execnodes.h:752`)
- [ ] **ExecAuxRowMark** - Auxiliary row mark (`src/include/nodes/execnodes.h:776`)
- [ ] **TupleHashEntry** - Tuple hash entry (`src/include/nodes/execnodes.h:801`)
- [ ] **TupleHashTable** - Tuple hash table (`src/include/nodes/execnodes.h:802`)
- [ ] **TupleHashEntryData** - Tuple hash entry data (`src/include/nodes/execnodes.h:804`)
- [ ] **TupleHashTableData** - Tuple hash table data (`src/include/nodes/execnodes.h:820`)
- [ ] **EPQState** - EPQ (EvalPlanQual) state (`src/include/nodes/execnodes.h:1254`)
- [ ] **AsyncRequest** - Async request (`src/include/nodes/execnodes.h:606`)

### Expression State Structures
- [ ] **WindowFuncExprState** - Window function expression state (`src/include/nodes/execnodes.h:873`)
- [ ] **SetExprState** - Set expression state (`src/include/nodes/execnodes.h:892`)
- [ ] **SubPlanState** - Subplan state (`src/include/nodes/execnodes.h:962`)
- [ ] **DomainConstraintState** - Domain constraint state (`src/include/nodes/execnodes.h:1009`)
- [ ] **JsonExprState** - JSON expression state (`src/include/nodes/execnodes.h:1024`)

### Result and Project States
- [ ] **ResultState** - Result state (`src/include/nodes/execnodes.h:1322`)
- [ ] **ProjectSetState** - Project set state (`src/include/nodes/execnodes.h:1337`)

### Instrumentation and Statistics
- [ ] **AggregateInstrumentation** - Aggregate instrumentation (`src/include/nodes/execnodes.h:2429`)
- [ ] **HashInstrumentation** - Hash instrumentation (`src/include/nodes/execnodes.h:2723`)
- [ ] **MemoizeInstrumentation** - Memoize instrumentation (`src/include/nodes/execnodes.h:2240`)
- [ ] **IncrementalSortGroupInfo** - Incremental sort group info (`src/include/nodes/execnodes.h:2353`)
- [ ] **IncrementalSortInfo** - Incremental sort info (`src/include/nodes/execnodes.h:2363`)

### Parallel Execution Support
- [ ] **ParallelBitmapHeapState** - Parallel bitmap heap state (`src/include/nodes/execnodes.h:1786`)
- [ ] **ParallelAppendState** - Parallel append state (`src/include/nodes/execnodes.h:1438`)
- [ ] **SharedSortInfo** - Shared sort info (`src/include/nodes/execnodes.h:2324`)
- [ ] **SharedMemoizeInfo** - Shared memoize info (`src/include/nodes/execnodes.h:2259`)
- [ ] **SharedHashInfo** - Shared hash info (`src/include/nodes/execnodes.h:2736`)
- [ ] **SharedAggInfo** - Shared aggregate info (`src/include/nodes/execnodes.h:2440`)
- [ ] **SharedIncrementalSortInfo** - Shared incremental sort info (`src/include/nodes/execnodes.h:2373`)

### Per-Aggregate/Per-Group Data
- [ ] **AggStatePerAgg** - Per-aggregate data (`src/include/nodes/execnodes.h:2459`)
- [ ] **AggStatePerTrans** - Per-transition data (`src/include/nodes/execnodes.h:2460`)
- [ ] **AggStatePerGroup** - Per-group data (`src/include/nodes/execnodes.h:2461`)
- [ ] **AggStatePerPhase** - Per-phase data (`src/include/nodes/execnodes.h:2462`)
- [ ] **AggStatePerHash** - Per-hash data (`src/include/nodes/execnodes.h:2463`)
- [ ] **WindowStatePerFunc** - Per-window-function data (`src/include/nodes/execnodes.h:2546`)
- [ ] **WindowStatePerAgg** - Per-window-aggregate data (`src/include/nodes/execnodes.h:2547`)
- [ ] **SetOpStatePerGroup** - Per-group set operation data (`src/include/nodes/execnodes.h:2781`)

### Hash Join Support
- [ ] **HashJoinTuple** - Hash join tuple (`src/include/nodes/execnodes.h:2188`)
- [ ] **HashJoinTable** - Hash join table (`src/include/nodes/execnodes.h:2189`)
- [ ] **MergeJoinClause** - Merge join clause (`src/include/nodes/execnodes.h:2136`)

### Sort Support
- [ ] **PresortedKeyData** - Presorted key data (`src/include/nodes/execnodes.h:2313`)

## Path Node Structures (pathnodes.h)

### Core Path Types
- [ ] **Path** - Base path node (`src/include/nodes/pathnodes.h:1621`)
- [ ] **IndexPath** - Index path (`src/include/nodes/pathnodes.h:1709`)
- [ ] **BitmapHeapPath** - Bitmap heap path (`src/include/nodes/pathnodes.h:1784`)
- [ ] **BitmapAndPath** - Bitmap AND path (`src/include/nodes/pathnodes.h:1796`)
- [ ] **BitmapOrPath** - Bitmap OR path (`src/include/nodes/pathnodes.h:1809`)
- [ ] **TidPath** - TID path (`src/include/nodes/pathnodes.h:1823`)
- [ ] **TidRangePath** - TID range path (`src/include/nodes/pathnodes.h:1835`)
- [ ] **SubqueryScanPath** - Subquery scan path (`src/include/nodes/pathnodes.h:1849`)
- [ ] **ForeignPath** - Foreign path (`src/include/nodes/pathnodes.h:1869`)
- [ ] **CustomPath** - Custom path (`src/include/nodes/pathnodes.h:1905`)

### Join Path Types
- [ ] **JoinPath** - Base join path (`src/include/nodes/pathnodes.h:2065`)
- [ ] **NestPath** - Nested loop path (`src/include/nodes/pathnodes.h:2092`)
- [ ] **MergePath** - Merge join path (`src/include/nodes/pathnodes.h:2132`)
- [ ] **HashPath** - Hash join path (`src/include/nodes/pathnodes.h:2151`)

### Set Operation Paths
- [ ] **AppendPath** - Append path (`src/include/nodes/pathnodes.h:1931`)
- [ ] **MergeAppendPath** - Merge append path (`src/include/nodes/pathnodes.h:1955`)
- [ ] **GroupResultPath** - Group result path (`src/include/nodes/pathnodes.h:1969`)

### Utility Paths
- [ ] **MaterialPath** - Material path (`src/include/nodes/pathnodes.h:1981`)
- [ ] **MemoizePath** - Memoize path (`src/include/nodes/pathnodes.h:1992`)
- [ ] **UniquePath** - Unique path (`src/include/nodes/pathnodes.h:2027`)
- [ ] **GatherPath** - Gather path (`src/include/nodes/pathnodes.h:2041`)
- [ ] **GatherMergePath** - Gather merge path (`src/include/nodes/pathnodes.h:2053`)
- [ ] **ProjectionPath** - Projection path (`src/include/nodes/pathnodes.h:2173`)
- [ ] **ProjectSetPath** - Project set path (`src/include/nodes/pathnodes.h:2185`)
- [ ] **SortPath** - Sort path (`src/include/nodes/pathnodes.h:2199`)
- [ ] **IncrementalSortPath** - Incremental sort path (`src/include/nodes/pathnodes.h:2211`)
- [ ] **GroupPath** - Group path (`src/include/nodes/pathnodes.h:2225`)
- [ ] **UpperUniquePath** - Upper unique path (`src/include/nodes/pathnodes.h:2239`)
- [ ] **AggPath** - Aggregate path (`src/include/nodes/pathnodes.h:2253`)
- [ ] **GroupingSetsPath** - Grouping sets path (`src/include/nodes/pathnodes.h:2295`)
- [ ] **MinMaxAggPath** - Min/max aggregate path (`src/include/nodes/pathnodes.h:2308`)
- [ ] **WindowAggPath** - Window aggregate path (`src/include/nodes/pathnodes.h:2318`)
- [ ] **SetOpPath** - Set operation path (`src/include/nodes/pathnodes.h:2332`)
- [ ] **RecursiveUnionPath** - Recursive union path (`src/include/nodes/pathnodes.h:2347`)
- [ ] **LockRowsPath** - Lock rows path (`src/include/nodes/pathnodes.h:2360`)
- [ ] **ModifyTablePath** - Modify table path (`src/include/nodes/pathnodes.h:2375`)
- [ ] **LimitPath** - Limit path (`src/include/nodes/pathnodes.h:2400`)

### Planner Support Structures
- [ ] **PlannerGlobal** - Global planner info (`src/include/nodes/pathnodes.h:95`)
- [ ] **PlannerInfo** - Planner info (`src/include/nodes/pathnodes.h:191`)
- [ ] **RelOptInfo** - Relation optimization info (`src/include/nodes/pathnodes.h:853`)
- [ ] **IndexOptInfo** - Index optimization info (`src/include/nodes/pathnodes.h:1100`)
- [ ] **ForeignKeyOptInfo** - Foreign key optimization info (`src/include/nodes/pathnodes.h:1216`)
- [ ] **StatisticExtInfo** - Extended statistics info (`src/include/nodes/pathnodes.h:1266`)
- [ ] **JoinDomain** - Join domain (`src/include/nodes/pathnodes.h:1317`)
- [ ] **EquivalenceClass** - Equivalence class (`src/include/nodes/pathnodes.h:1379`)
- [ ] **EquivalenceMember** - Equivalence member (`src/include/nodes/pathnodes.h:1430`)
- [ ] **PathKey** - Path key (`src/include/nodes/pathnodes.h:1463`)
- [ ] **GroupByOrdering** - Group by ordering (`src/include/nodes/pathnodes.h:1485`)
- [ ] **PathTarget** - Path target (`src/include/nodes/pathnodes.h:1528`)
- [ ] **ParamPathInfo** - Parameterized path info (`src/include/nodes/pathnodes.h:1575`)
- [ ] **RestrictInfo** - Restriction info (`src/include/nodes/pathnodes.h:2559`)
- [ ] **PlaceHolderVar** - Placeholder variable (`src/include/nodes/pathnodes.h:2780`)
- [ ] **SpecialJoinInfo** - Special join info (`src/include/nodes/pathnodes.h:2887`)
- [ ] **OuterJoinClauseInfo** - Outer join clause info (`src/include/nodes/pathnodes.h:2920`)
- [ ] **AppendRelInfo** - Append relation info (`src/include/nodes/pathnodes.h:2959`)
- [ ] **RowIdentityVarInfo** - Row identity variable info (`src/include/nodes/pathnodes.h:3036`)
- [ ] **PlaceHolderInfo** - Placeholder info (`src/include/nodes/pathnodes.h:3074`)
- [ ] **MinMaxAggInfo** - Min/max aggregate info (`src/include/nodes/pathnodes.h:3107`)
- [ ] **PlannerParamItem** - Planner parameter item (`src/include/nodes/pathnodes.h:3185`)
- [ ] **SemiAntiJoinFactors** - Semi/anti-join factors (`src/include/nodes/pathnodes.h:3211`)
- [ ] **JoinPathExtraData** - Join path extra data (`src/include/nodes/pathnodes.h:3230`)
- [ ] **JoinCostWorkspace** - Join cost workspace (`src/include/nodes/pathnodes.h:3335`)
- [ ] **AggInfo** - Aggregate info (`src/include/nodes/pathnodes.h:3365`)
- [ ] **AggTransInfo** - Aggregate transition info (`src/include/nodes/pathnodes.h:3399`)

### Cost and Statistics
- [ ] **QualCost** - Qualifier cost (`src/include/nodes/pathnodes.h:45`)
- [ ] **AggClauseCosts** - Aggregate clause costs (`src/include/nodes/pathnodes.h:58`)
- [ ] **PartitionSchemeData** - Partition scheme data (`src/include/nodes/pathnodes.h:582`)
- [ ] **IndexClause** - Index clause (`src/include/nodes/pathnodes.h:1755`)
- [ ] **MergeScanSelCache** - Merge scan selectivity cache (`src/include/nodes/pathnodes.h:2734`)
- [ ] **GroupingSetData** - Grouping set data (`src/include/nodes/pathnodes.h:2269`)
- [ ] **RollupData** - Rollup data (`src/include/nodes/pathnodes.h:2278`)

## Miscellaneous Node Structures

### List Structures (pg_list.h)
- [ ] **List** - Generic list (`src/include/nodes/pg_list.h:53`)
- [ ] **ForEachState** - For-each iteration state (`src/include/nodes/pg_list.h:73`)
- [ ] **ForBothState** - For-both iteration state (`src/include/nodes/pg_list.h:79`)
- [ ] **ForBothCellState** - For-both cell iteration state (`src/include/nodes/pg_list.h:86`)
- [ ] **ForThreeState** - For-three iteration state (`src/include/nodes/pg_list.h:94`)
- [ ] **ForFourState** - For-four iteration state (`src/include/nodes/pg_list.h:102`)
- [ ] **ForFiveState** - For-five iteration state (`src/include/nodes/pg_list.h:111`)

### Value Nodes (value.h)
- [ ] **Integer** - Integer value (`src/include/nodes/value.h:28`)
- [ ] **Float** - Float value (`src/include/nodes/value.h:47`)
- [ ] **Boolean** - Boolean value (`src/include/nodes/value.h:55`)
- [ ] **String** - String value (`src/include/nodes/value.h:63`)
- [ ] **BitString** - Bit string value (`src/include/nodes/value.h:71`)

### Base Node (nodes.h)
- [ ] **Node** - Base node structure (`src/include/nodes/nodes.h:128`)

### Memory Management (memnodes.h)
- [ ] **MemoryContextData** - Memory context (`src/include/nodes/memnodes.h:117`)
- [ ] **MemoryContextCounters** - Memory context counters (`src/include/nodes/memnodes.h:29`)
- [ ] **MemoryContextMethods** - Memory context methods (`src/include/nodes/memnodes.h:58`)

### Query Jumbling (queryjumble.h)
- [ ] **LocationLen** - Location and length (`src/include/nodes/queryjumble.h:22`)
- [ ] **JumbleState** - Query jumble state (`src/include/nodes/queryjumble.h:32`)

### Extensible Nodes (extensible.h)
- [ ] **ExtensibleNode** - Extensible node (`src/include/nodes/extensible.h:32`)
- [ ] **ExtensibleNodeMethods** - Extensible node methods (`src/include/nodes/extensible.h:62`)
- [ ] **CustomPathMethods** - Custom path methods (`src/include/nodes/extensible.h:92`)
- [ ] **CustomScanMethods** - Custom scan methods (`src/include/nodes/extensible.h:112`)
- [ ] **CustomExecMethods** - Custom execution methods (`src/include/nodes/extensible.h:124`)

### Support Nodes (supportnodes.h)
- [ ] **SupportRequestSimplify** - Simplify support request (`src/include/nodes/supportnodes.h:64`)
- [ ] **SupportRequestSelectivity** - Selectivity support request (`src/include/nodes/supportnodes.h:91`)
- [ ] **SupportRequestCost** - Cost support request (`src/include/nodes/supportnodes.h:131`)
- [ ] **SupportRequestRows** - Rows support request (`src/include/nodes/supportnodes.h:158`)
- [ ] **SupportRequestIndexCondition** - Index condition support request (`src/include/nodes/supportnodes.h:223`)
- [ ] **SupportRequestWFuncMonotonic** - Window function monotonic support request (`src/include/nodes/supportnodes.h:290`)
- [ ] **SupportRequestOptimizeWindowClause** - Window clause optimization support request (`src/include/nodes/supportnodes.h:333`)

### Subscripting (subscripting.h)
- [ ] **SubscriptRoutines** - Subscripting routines (`src/include/nodes/subscripting.h:158`)

### Replication Nodes (replnodes.h)
- [ ] **IdentifySystemCmd** - IDENTIFY_SYSTEM command (`src/include/nodes/replnodes.h:31`)
- [ ] **BaseBackupCmd** - BASE_BACKUP command (`src/include/nodes/replnodes.h:41`)
- [ ] **CreateReplicationSlotCmd** - CREATE_REPLICATION_SLOT command (`src/include/nodes/replnodes.h:52`)
- [ ] **DropReplicationSlotCmd** - DROP_REPLICATION_SLOT command (`src/include/nodes/replnodes.h:67`)
- [ ] **AlterReplicationSlotCmd** - ALTER_REPLICATION_SLOT command (`src/include/nodes/replnodes.h:79`)
- [ ] **StartReplicationCmd** - START_REPLICATION command (`src/include/nodes/replnodes.h:91`)
- [ ] **ReadReplicationSlotCmd** - READ_REPLICATION_SLOT command (`src/include/nodes/replnodes.h:106`)
- [ ] **TimeLineHistoryCmd** - TIMELINE_HISTORY command (`src/include/nodes/replnodes.h:117`)
- [ ] **UploadManifestCmd** - UPLOAD_MANIFEST command (`src/include/nodes/replnodes.h:127`)

---

**Total: 400+ AST struct definitions**

This checklist will be used in Phase 1.5 to track implementation progress of all PostgreSQL AST structures during the parser port project.