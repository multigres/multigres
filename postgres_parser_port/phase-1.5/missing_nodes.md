# Missing PostgreSQL AST Nodes - Complete Inventory

**Total Missing**: ~185+ nodes out of 265 total PostgreSQL AST nodes  
**Current Implementation**: ~70-80 nodes (30% complete)  
**Target**: 100% PostgreSQL AST compatibility

---

## High Priority - Essential Missing Structures

### Query Execution Nodes (Critical)
These nodes are essential for basic SQL query functionality:

**From primnodes.h:**
- [ ] **TargetEntry** - Target list entries in SELECT, crucial for query execution
- [ ] **FromExpr** - FROM clause representation, needed for table joins
- [ ] **JoinExpr** - JOIN operations (INNER, LEFT, RIGHT, FULL, etc.)
- [ ] **SubPlan** - Execution-level subquery representation
- [ ] **AlternativeSubPlan** - Alternative subquery execution paths

**From parsenodes.h:**
- [ ] **CommonTableExpr** - WITH clause (CTE) support, increasingly common
- [ ] **OnConflictExpr** - INSERT...ON CONFLICT, important PostgreSQL feature
- [ ] **WindowClause** - Window function clause definitions
- [ ] **SortGroupClause** - ORDER BY and GROUP BY clause representation
- [ ] **RowMarkClause** - FOR UPDATE/SHARE locking clauses

### Type System Nodes (Important)
**From primnodes.h:**
- [ ] **RelabelType** - Type casting operations
- [ ] **CoerceViaIO** - Type coercion through I/O functions
- [ ] **ArrayCoerceExpr** - Array type coercion
- [ ] **ConvertRowtypeExpr** - Row type conversion

---

## Medium Priority - Common SQL Features

### Advanced Expression Nodes
**From primnodes.h:**
- [ ] **SubscriptingRef** - Array/JSON subscripting (arr[1], json['key'])
- [ ] **FieldSelect** - Record field access (record.field)
- [ ] **FieldStore** - Record field assignment
- [ ] **NullTest** - IS NULL/IS NOT NULL tests
- [ ] **BooleanTest** - IS TRUE/IS FALSE/IS UNKNOWN tests
- [ ] **CoerceToDomain** - Domain type coercion
- [ ] **CoerceToDomainValue** - Domain constraint checking
- [ ] **SetToDefault** - DEFAULT value expression
- [ ] **CurrentOfExpr** - CURRENT OF cursor references
- [ ] **NextValueExpr** - Sequence nextval() expressions
- [ ] **InferenceElem** - Inference elements for ON CONFLICT

### Advanced DDL Nodes
**From parsenodes.h:**
- [ ] **AlterTableCmd** - Individual ALTER TABLE command
- [ ] **AlterDatabaseStmt** - ALTER DATABASE operations
- [ ] **AlterSystemStmt** - ALTER SYSTEM operations
- [ ] **CreateUserMappingStmt** - Foreign data wrapper user mappings
- [ ] **AlterUserMappingStmt** - Alter user mappings
- [ ] **DropUserMappingStmt** - Drop user mappings
- [ ] **CreateForeignServerStmt** - Foreign server creation
- [ ] **AlterForeignServerStmt** - Foreign server alteration
- [ ] **CreateForeignTableStmt** - Foreign table creation
- [ ] **CreateTriggerStmt** - Trigger creation
- [ ] **CreateEventTriggerStmt** - Event trigger creation
- [ ] **AlterEventTriggerStmt** - Event trigger alteration
- [ ] **CreatePolicyStmt** - Row-level security policies
- [ ] **AlterPolicyStmt** - Policy alteration
- [ ] **CreateAmStmt** - Access method creation

### Table Definition Nodes
**From parsenodes.h:**
- [ ] **ColumnDef** - Column definitions in CREATE TABLE
- [ ] **TableLikeClause** - LIKE clauses in CREATE TABLE
- [ ] **PartitionSpec** - Table partitioning specifications
- [ ] **PartitionBoundSpec** - Partition bound specifications
- [ ] **PartitionRangeDatum** - Partition range values
- [ ] **PartitionCmd** - Partition-related commands

---

## Lower Priority - Advanced Features

### JSON/XML Support Nodes
**From primnodes.h:**
- [ ] **JsonExpr** - JSON path expressions
- [ ] **JsonConstructorExpr** - JSON constructor expressions
- [ ] **JsonIsPredicate** - JSON type predicates
- [ ] **JsonBehavior** - JSON behavior specifications
- [ ] **JsonReturning** - JSON RETURNING clauses
- [ ] **JsonValueExpr** - JSON_VALUE expressions
- [ ] **JsonTableExpr** - JSON_TABLE expressions
- [ ] **XmlExpr** - XML expressions and operations

### Advanced Function/Operator Nodes
**From primnodes.h:**
- [ ] **DistinctExpr** - IS DISTINCT FROM operations
- [ ] **ScalarArrayOpExpr** - scalar op ANY/ALL(array) - **May already exist**
- [ ] **MinMaxExpr** - GREATEST/LEAST expressions
- [ ] **SQLValueFunction** - SQL standard functions (CURRENT_USER, etc.)
- [ ] **GroupingFunc** - GROUPING function for ROLLUP/CUBE
- [ ] **WindowFuncRunCondition** - Window function run conditions

### Advanced Statement Nodes
**From parsenodes.h:**
- [ ] **MergeStmt** - MERGE statement (PostgreSQL 15+)
- [ ] **MergeWhenClause** - WHEN clauses in MERGE
- [ ] **CreatePublicationStmt** - Logical replication publications
- [ ] **AlterPublicationStmt** - Publication alteration
- [ ] **CreateSubscriptionStmt** - Logical replication subscriptions
- [ ] **AlterSubscriptionStmt** - Subscription alteration
- [ ] **DropSubscriptionStmt** - Subscription dropping

### Specialized Utility Nodes
**From parsenodes.h:**
- [ ] **CreateStatsStmt** - Extended statistics creation
- [ ] **AlterStatsStmt** - Statistics alteration
- [ ] **CreateConversionStmt** - Character set conversion
- [ ] **CreateCollationStmt** - Collation creation
- [ ] **AlterCollationStmt** - Collation alteration
- [ ] **CreateTransformStmt** - Transform creation
- [ ] **CallStmt** - CALL statement for procedures
- [ ] **ClosePortalStmt** - Cursor closing
- [ ] **FetchStmt** - Cursor fetching
- [ ] **CommentStmt** - COMMENT ON statements
- [ ] **SecLabelStmt** - Security label statements

---

## Supporting Infrastructure Nodes

### Query Planning Support
**From primnodes.h:**
- [ ] **PlaceHolderVar** - Query planner placeholders
- [ ] **PlaceHolderInfo** - Placeholder metadata
- [ ] **AppendRelInfo** - Append relation info
- [ ] **RowIdentityVarInfo** - Row identity information
- [ ] **PlannedStmt** - Planned statement representation

### Parse Analysis Support
**From parsenodes.h:**
- [ ] **RangeTblEntry** - Range table entries
- [ ] **RangeTblRef** - Range table references  
- [ ] **JoinType** - JOIN type enumeration
- [ ] **AggSplit** - Aggregate splitting information
- [ ] **GroupingSet** - GROUPING SETS specifications
- [ ] **WindowDef** - Window function definitions
- [ ] **SortBy** - Sort specifications
- [ ] **LockingClause** - Row locking specifications

### Constraint and Index Support
**From parsenodes.h:**
- [ ] **IndexElem** - **May already exist** - Index element specifications
- [ ] **StatsElem** - Statistics element specifications
- [ ] **CollateClause** - **May already exist** - Collation specifications
- [ ] **SortGroupClause** - Sort/group clause specifications
- [ ] **GroupingSet** - Grouping set specifications
- [ ] **WindowDef** - Window definition clauses

---

## Implementation Status by Source File

### parsenodes.h Progress
- **Total structs**: ~196
- **Implemented**: ~40-50 (estimated)
- **Missing**: ~150+ structs
- **Categories**: DDL variants, utility statements, advanced SQL features

### primnodes.h Progress  
- **Total structs**: ~64
- **Implemented**: ~25-30 (estimated)
- **Missing**: ~35+ structs
- **Categories**: Execution nodes, type coercion, specialized expressions

### value.h Progress
- **Total structs**: 5
- **Implemented**: 5 (100% complete) ✅
- **Missing**: None

---

## Implementation Recommendations

### Phase 1 - Essential Nodes (High Priority)
**Target**: ~25 nodes, enables basic query functionality
1. Start with TargetEntry, FromExpr, JoinExpr (query execution core)
2. Add SubPlan, AlternativeSubPlan (subquery support)
3. Implement CommonTableExpr, WindowClause (modern SQL features)
4. Add OnConflictExpr, SortGroupClause, RowMarkClause

### Phase 2 - Common Features (Medium Priority)  
**Target**: ~50 nodes, enables advanced SQL
1. Type coercion expressions (RelabelType, CoerceViaIO, etc.)
2. Advanced expressions (FieldSelect, SubscriptingRef, etc.)
3. DDL command variants (AlterTableCmd, etc.)
4. Constraint and index support nodes

### Phase 3 - Advanced Features (Lower Priority)
**Target**: ~110+ nodes, complete PostgreSQL coverage
1. JSON/XML expressions for modern PostgreSQL
2. Specialized statements (MERGE, replication, etc.)
3. Administrative and utility commands
4. Query planning and analysis support

### Implementation Guidelines for Each Node:
1. **Locate in PostgreSQL source** - Find exact struct definition
2. **Document source reference** - Include file:line in Go comments
3. **Map fields accurately** - Preserve PostgreSQL field names and types
4. **Implement interfaces** - Node, Statement, Expression, or Value as appropriate
5. **Add constructors** - New* functions for common usage patterns
6. **Write comprehensive tests** - Cover all fields and edge cases
7. **Integration testing** - Ensure compatibility with existing AST

**Completing these 185+ missing nodes will achieve 100% PostgreSQL AST compatibility, enabling full parser functionality.**