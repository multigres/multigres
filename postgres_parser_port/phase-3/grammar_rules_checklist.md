# PostgreSQL Grammar Rules Checklist

**Total Rules**: 727
**Status Legend**: 
- ⬜ Not Started
- 🟨 In Progress  
- ✅ Completed
- 🔄 Needs Revision

## Phase 3A: Grammar Foundation & Infrastructure (~20 rules)

### Core Structure
- ⬜ `parse_toplevel` - Top-level parser entry point
- ⬜ `stmtmulti` - Multiple statements separated by semicolons
- ⬜ `toplevel_stmt` - Top-level statement wrapper
- ⬜ `stmt` - Individual statement dispatcher

### Common Options
- ⬜ `opt_single_name` - Optional single name
- ⬜ `opt_qualified_name` - Optional qualified name
- ⬜ `opt_drop_behavior` - CASCADE/RESTRICT option
- ⬜ `opt_concurrently` - CONCURRENTLY option
- ⬜ `opt_if_exists` - IF EXISTS clause
- ⬜ `opt_if_not_exists` - IF NOT EXISTS clause
- ⬜ `opt_or_replace` - OR REPLACE option
- ⬜ `opt_with` - WITH option
- ⬜ `OptWith` - Alternative WITH option

### Names and Identifiers
- ⬜ `ColId` - Column identifier
- ⬜ `ColLabel` - Column label
- ⬜ `name` - Simple name
- ⬜ `name_list` - List of names
- ⬜ `qualified_name` - Schema-qualified name
- ⬜ `qualified_name_list` - List of qualified names
- ⬜ `any_name` - Any name (for generic objects)

## Phase 3B: Basic Expression Grammar (~40 rules)

### Core Expressions
- ⬜ `a_expr` - A-level expressions (most general)
- ⬜ `b_expr` - B-level expressions (restricted)
- ⬜ `c_expr` - C-level expressions (most restricted)
- ⬜ `AexprConst` - Constant expressions
- ⬜ `Iconst` - Integer constant
- ⬜ `Sconst` - String constant
- ⬜ `SignedIconst` - Signed integer constant

### Column and Function References
- ⬜ `columnref` - Column reference
- ⬜ `indirection` - Array/field access
- ⬜ `indirection_el` - Single indirection element
- ⬜ `opt_indirection` - Optional indirection

### Functions
- ⬜ `func_expr` - Function expressions
- ⬜ `func_expr_windowless` - Function without window
- ⬜ `func_expr_common_subexpr` - Common function subexpressions
- ⬜ `func_application` - Function application
- ⬜ `func_name` - Function name
- ⬜ `func_arg_list` - Function argument list
- ⬜ `func_arg_expr` - Function argument expression
- ⬜ `func_arg_list_opt` - Optional function arguments

### Operators and Math
- ⬜ `qual_Op` - Qualified operator
- ⬜ `qual_all_Op` - Qualified ALL operator
- ⬜ `all_Op` - All operators
- ⬜ `MathOp` - Mathematical operators
- ⬜ `any_operator` - Any operator

### Type Casting
- ⬜ `Typename` - Type name
- ⬜ `SimpleTypename` - Simple type name
- ⬜ `GenericType` - Generic type
- ⬜ `Numeric` - Numeric type
- ⬜ `Bit` - Bit type
- ⬜ `Character` - Character type
- ⬜ `ConstDatetime` - Datetime constant
- ⬜ `ConstInterval` - Interval constant

### Lists
- ⬜ `expr_list` - Expression list
- ⬜ `type_list` - Type list
- ⬜ `array_expr` - Array expression
- ⬜ `array_expr_list` - Array expression list
- ⬜ `row` - Row expression
- ⬜ `explicit_row` - Explicit row constructor
- ⬜ `implicit_row` - Implicit row constructor

## Phase 3C: SELECT Statement Core (~35 rules)

### Main SELECT Structure
- ⬜ `SelectStmt` - SELECT statement
- ⬜ `select_no_parens` - SELECT without parentheses
- ⬜ `select_with_parens` - SELECT with parentheses
- ⬜ `select_clause` - SELECT clause
- ⬜ `simple_select` - Simple SELECT

### Target List
- ⬜ `target_list` - SELECT target list
- ⬜ `target_el` - Target list element
- ⬜ `opt_target_list` - Optional target list

### FROM Clause
- ⬜ `from_clause` - FROM clause
- ⬜ `from_list` - FROM list
- ⬜ `table_ref` - Table reference

### WHERE Clause
- ⬜ `where_clause` - WHERE clause
- ⬜ `OptWhereClause` - Optional WHERE clause
- ⬜ `where_or_current_clause` - WHERE or CURRENT OF

### Basic Table References
- ⬜ `relation_expr` - Relation expression
- ⬜ `relation_expr_list` - Relation expression list
- ⬜ `relation_expr_opt_alias` - Relation with optional alias
- ⬜ `extended_relation_expr` - Extended relation expression

### Aliases
- ⬜ `alias_clause` - Alias clause
- ⬜ `opt_alias_clause` - Optional alias clause
- ⬜ `opt_alias_clause_for_join_using` - Alias for JOIN USING
- ⬜ `func_alias_clause` - Function alias clause

### Set Operations
- ⬜ `set_quantifier` - ALL/DISTINCT
- ⬜ `opt_all_clause` - Optional ALL clause
- ⬜ `distinct_clause` - DISTINCT clause
- ⬜ `opt_distinct_clause` - Optional DISTINCT clause

## Phase 3D: JOIN & Table References (~45 rules)

### JOIN Operations
- ⬜ `joined_table` - Joined table
- ⬜ `join_type` - JOIN type (INNER, LEFT, etc.)
- ⬜ `join_qual` - JOIN qualification
- ⬜ `using_clause` - USING clause

### WITH Clause (CTEs)
- ⬜ `with_clause` - WITH clause
- ⬜ `opt_with_clause` - Optional WITH clause
- ⬜ `cte_list` - CTE list
- ⬜ `common_table_expr` - Common table expression
- ⬜ `opt_search_clause` - Optional SEARCH clause
- ⬜ `opt_cycle_clause` - Optional CYCLE clause
- ⬜ `opt_materialized` - MATERIALIZED option

### Subqueries
- ⬜ `subquery_Op` - Subquery operators
- ⬜ `in_expr` - IN expression

### Table Sampling
- ⬜ `tablesample_clause` - TABLESAMPLE clause
- ⬜ `opt_repeatable_clause` - REPEATABLE clause

### VALUES Clause
- ⬜ `values_clause` - VALUES clause

### Row Pattern Recognition
- ⬜ `rowsfrom_item` - ROWS FROM item
- ⬜ `rowsfrom_list` - ROWS FROM list

### Table Functions
- ⬜ `func_table` - Table function
- ⬜ `TableFuncElement` - Table function element
- ⬜ `TableFuncElementList` - Table function element list
- ⬜ `OptTableFuncElementList` - Optional table function elements
- ⬜ `table_func_column` - Table function column
- ⬜ `table_func_column_list` - Table function column list

### XMLTABLE
- ⬜ `xmltable` - XMLTABLE
- ⬜ `xmltable_column_list` - XMLTABLE column list
- ⬜ `xmltable_column_el` - XMLTABLE column element
- ⬜ `xmltable_column_option_list` - XMLTABLE column options
- ⬜ `xmltable_column_option_el` - XMLTABLE column option

### JSON_TABLE
- ⬜ `json_table` - JSON_TABLE
- ⬜ `json_table_column_definition` - JSON table column
- ⬜ `json_table_column_definition_list` - JSON table columns
- ⬜ `json_table_column_path_clause_opt` - JSON path clause
- ⬜ `json_table_path_name_opt` - JSON path name

## Phase 3E: Data Manipulation - DML (~50 rules)

### INSERT Statement
- ⬜ `InsertStmt` - INSERT statement
- ⬜ `insert_rest` - INSERT rest
- ⬜ `insert_target` - INSERT target
- ⬜ `insert_column_list` - Column list for INSERT
- ⬜ `insert_column_item` - Single column in INSERT

### UPDATE Statement
- ⬜ `UpdateStmt` - UPDATE statement
- ⬜ `set_clause_list` - SET clause list
- ⬜ `set_clause` - Single SET clause
- ⬜ `set_target` - SET target
- ⬜ `set_target_list` - SET target list

### DELETE Statement
- ⬜ `DeleteStmt` - DELETE statement

### MERGE Statement
- ⬜ `MergeStmt` - MERGE statement
- ⬜ `merge_when_list` - MERGE WHEN list
- ⬜ `merge_when_clause` - MERGE WHEN clause
- ⬜ `merge_when_tgt_matched` - Target matched
- ⬜ `merge_when_tgt_not_matched` - Target not matched
- ⬜ `opt_merge_when_condition` - MERGE condition
- ⬜ `merge_update` - MERGE UPDATE
- ⬜ `merge_delete` - MERGE DELETE
- ⬜ `merge_insert` - MERGE INSERT
- ⬜ `merge_values_clause` - MERGE VALUES

### ON CONFLICT (UPSERT)
- ⬜ `opt_on_conflict` - ON CONFLICT clause
- ⬜ `opt_conf_expr` - Conflict expression

### RETURNING Clause
- ⬜ `returning_clause` - RETURNING clause

### COPY Statement
- ⬜ `CopyStmt` - COPY statement
- ⬜ `copy_from` - COPY FROM/TO
- ⬜ `copy_file_name` - COPY filename
- ⬜ `copy_options` - COPY options
- ⬜ `copy_opt_list` - COPY option list
- ⬜ `copy_opt_item` - COPY option item
- ⬜ `copy_delimiter` - COPY delimiter
- ⬜ `copy_generic_opt_list` - Generic COPY options
- ⬜ `copy_generic_opt_elem` - Generic COPY option
- ⬜ `copy_generic_opt_arg` - Generic option argument
- ⬜ `copy_generic_opt_arg_list` - Generic option arg list
- ⬜ `copy_generic_opt_arg_list_item` - Generic option arg item

### Utility
- ⬜ `opt_binary` - BINARY option
- ⬜ `opt_freeze` - FREEZE option
- ⬜ `opt_verbose` - VERBOSE option
- ⬜ `opt_analyze` - ANALYZE option
- ⬜ `opt_full` - FULL option

## Phase 3F: Basic DDL - Tables & Indexes (~80 rules)

### CREATE TABLE
- ⬜ `CreateStmt` - CREATE TABLE statement
- ⬜ `OptTableElementList` - Optional table elements
- ⬜ `TableElementList` - Table element list
- ⬜ `TableElement` - Single table element

### Column Definition
- ⬜ `columnDef` - Column definition
- ⬜ `columnOptions` - Column options
- ⬜ `column_compression` - Column compression
- ⬜ `opt_column_compression` - Optional compression
- ⬜ `column_storage` - Column storage
- ⬜ `opt_column_storage` - Optional storage
- ⬜ `ColQualList` - Column qualifier list
- ⬜ `ColConstraint` - Column constraint
- ⬜ `ColConstraintElem` - Column constraint element
- ⬜ `generated_when` - GENERATED WHEN

### Table Constraints
- ⬜ `TableConstraint` - Table constraint
- ⬜ `ConstraintElem` - Constraint element
- ⬜ `ConstraintAttr` - Constraint attribute
- ⬜ `ConstraintAttributeSpec` - Constraint attribute spec
- ⬜ `ConstraintAttributeElem` - Constraint attribute element

### Constraint Options
- ⬜ `ExistingIndex` - Existing index reference
- ⬜ `key_match` - Foreign key MATCH
- ⬜ `key_actions` - Foreign key actions
- ⬜ `key_action` - Single key action
- ⬜ `key_update` - ON UPDATE action
- ⬜ `key_delete` - ON DELETE action
- ⬜ `opt_no_inherit` - NO INHERIT option

### ALTER TABLE
- ⬜ `AlterTableStmt` - ALTER TABLE statement
- ⬜ `alter_table_cmds` - ALTER TABLE commands
- ⬜ `alter_table_cmd` - Single ALTER TABLE command
- ⬜ `alter_column_default` - ALTER column default
- ⬜ `alter_using` - ALTER USING clause
- ⬜ `alter_identity_column_option_list` - Identity options
- ⬜ `alter_identity_column_option` - Single identity option
- ⬜ `set_statistics_value` - SET STATISTICS value
- ⬜ `set_access_method_name` - SET ACCESS METHOD

### Partitioning
- ⬜ `PartitionSpec` - Partition specification
- ⬜ `OptPartitionSpec` - Optional partition spec
- ⬜ `part_params` - Partition parameters
- ⬜ `part_elem` - Partition element
- ⬜ `PartitionBoundSpec` - Partition bound spec
- ⬜ `hash_partbound` - Hash partition bound
- ⬜ `hash_partbound_elem` - Hash partition element
- ⬜ `partition_cmd` - Partition command

### Indexes
- ⬜ `IndexStmt` - CREATE INDEX statement
- ⬜ `index_params` - Index parameters
- ⬜ `index_elem` - Index element
- ⬜ `index_elem_options` - Index element options
- ⬜ `index_including_params` - INCLUDING params
- ⬜ `opt_include` - Optional INCLUDING
- ⬜ `opt_unique` - UNIQUE option
- ⬜ `opt_recheck` - RECHECK option
- ⬜ `access_method_clause` - Access method

### Index Partitioning
- ⬜ `index_partition_cmd` - Index partition command

### Table Options
- ⬜ `OptInherit` - INHERITS clause
- ⬜ `OptWith` - WITH clause
- ⬜ `reloptions` - Storage options
- ⬜ `reloption_list` - Storage option list
- ⬜ `reloption_elem` - Storage option element
- ⬜ `opt_reloptions` - Optional storage options

### Tablespace
- ⬜ `OptTableSpace` - TABLESPACE clause
- ⬜ `OptConsTableSpace` - Constraint tablespace
- ⬜ `OnCommitOption` - ON COMMIT option

### Table Copying
- ⬜ `TableLikeClause` - LIKE clause
- ⬜ `TableLikeOptionList` - LIKE options
- ⬜ `TableLikeOption` - Single LIKE option

### Typed Tables
- ⬜ `OptTypedTableElementList` - Typed table elements
- ⬜ `TypedTableElementList` - Typed table element list
- ⬜ `TypedTableElement` - Typed table element

### Temp Tables
- ⬜ `OptTemp` - TEMP/TEMPORARY option
- ⬜ `OptTempTableName` - Temp table name

## Phase 3G: Advanced DDL (~100 rules)

### CREATE FUNCTION/PROCEDURE
- ⬜ `CreateFunctionStmt` - CREATE FUNCTION
- ⬜ `FUNCTION_or_PROCEDURE` - FUNCTION or PROCEDURE
- ⬜ `func_args` - Function arguments
- ⬜ `func_args_list` - Function argument list
- ⬜ `func_arg` - Function argument
- ⬜ `func_arg_with_default` - Arg with default
- ⬜ `func_args_with_defaults` - Args with defaults
- ⬜ `func_args_with_defaults_list` - List of args with defaults
- ⬜ `func_return` - Function return type
- ⬜ `func_type` - Function type
- ⬜ `createfunc_opt_list` - Function options
- ⬜ `createfunc_opt_item` - Function option item
- ⬜ `common_func_opt_item` - Common function option
- ⬜ `func_as` - Function body
- ⬜ `opt_routine_body` - Optional routine body
- ⬜ `routine_body_stmt` - Routine body statement
- ⬜ `routine_body_stmt_list` - Routine body statements
- ⬜ `opt_createfunc_opt_list` - Optional function options

### ALTER FUNCTION
- ⬜ `AlterFunctionStmt` - ALTER FUNCTION
- ⬜ `alterfunc_opt_list` - ALTER FUNCTION options
- ⬜ `function_with_argtypes` - Function with arg types
- ⬜ `function_with_argtypes_list` - List of functions

### CREATE TRIGGER
- ⬜ `CreateTrigStmt` - CREATE TRIGGER
- ⬜ `TriggerActionTime` - BEFORE/AFTER
- ⬜ `TriggerEvents` - Trigger events
- ⬜ `TriggerOneEvent` - Single trigger event
- ⬜ `TriggerForSpec` - FOR specification
- ⬜ `TriggerForOptEach` - FOR EACH option
- ⬜ `TriggerForType` - ROW/STATEMENT
- ⬜ `TriggerWhen` - WHEN clause
- ⬜ `TriggerFuncArgs` - Trigger function args
- ⬜ `TriggerFuncArg` - Single trigger arg
- ⬜ `TriggerReferencing` - REFERENCING clause
- ⬜ `TriggerTransitions` - Trigger transitions
- ⬜ `TriggerTransition` - Single transition
- ⬜ `TransitionOldOrNew` - OLD/NEW
- ⬜ `TransitionRowOrTable` - ROW/TABLE
- ⬜ `TransitionRelName` - Transition relation name

### CREATE VIEW
- ⬜ `ViewStmt` - CREATE VIEW
- ⬜ `opt_check_option` - CHECK OPTION

### CREATE MATERIALIZED VIEW
- ⬜ `CreateMatViewStmt` - CREATE MATERIALIZED VIEW
- ⬜ `create_mv_target` - Materialized view target
- ⬜ `opt_with_data` - WITH DATA option
- ⬜ `RefreshMatViewStmt` - REFRESH MATERIALIZED VIEW

### CREATE SCHEMA
- ⬜ `CreateSchemaStmt` - CREATE SCHEMA
- ⬜ `OptSchemaEltList` - Schema elements
- ⬜ `schema_stmt` - Schema statement

### CREATE DOMAIN
- ⬜ `CreateDomainStmt` - CREATE DOMAIN
- ⬜ `AlterDomainStmt` - ALTER DOMAIN
- ⬜ `DomainConstraint` - Domain constraint
- ⬜ `DomainConstraintElem` - Domain constraint element

### CREATE TYPE
- ⬜ `DefineStmt` - DEFINE statement (types, etc.)
- ⬜ `definition` - Definition list
- ⬜ `def_list` - Definition element list
- ⬜ `def_elem` - Definition element
- ⬜ `def_arg` - Definition argument
- ⬜ `AlterEnumStmt` - ALTER TYPE for enums
- ⬜ `enum_val_list` - Enum value list
- ⬜ `opt_enum_val_list` - Optional enum values
- ⬜ `AlterTypeStmt` - ALTER TYPE
- ⬜ `alter_type_cmds` - ALTER TYPE commands
- ⬜ `alter_type_cmd` - Single ALTER TYPE command

### CREATE SEQUENCE
- ⬜ `CreateSeqStmt` - CREATE SEQUENCE
- ⬜ `AlterSeqStmt` - ALTER SEQUENCE
- ⬜ `OptSeqOptList` - Sequence options
- ⬜ `OptParenthesizedSeqOptList` - Parenthesized options
- ⬜ `SeqOptList` - Sequence option list
- ⬜ `SeqOptElem` - Sequence option element

### CREATE EXTENSION
- ⬜ `CreateExtensionStmt` - CREATE EXTENSION
- ⬜ `create_extension_opt_list` - Extension options
- ⬜ `create_extension_opt_item` - Extension option item
- ⬜ `AlterExtensionStmt` - ALTER EXTENSION
- ⬜ `alter_extension_opt_list` - ALTER extension options
- ⬜ `alter_extension_opt_item` - ALTER extension option
- ⬜ `AlterExtensionContentsStmt` - ALTER EXTENSION contents

### CREATE FOREIGN DATA WRAPPER
- ⬜ `CreateFdwStmt` - CREATE FOREIGN DATA WRAPPER
- ⬜ `AlterFdwStmt` - ALTER FOREIGN DATA WRAPPER
- ⬜ `fdw_options` - FDW options
- ⬜ `fdw_option` - Single FDW option
- ⬜ `opt_fdw_options` - Optional FDW options

### CREATE FOREIGN TABLE
- ⬜ `CreateForeignTableStmt` - CREATE FOREIGN TABLE
- ⬜ `CreateForeignServerStmt` - CREATE SERVER
- ⬜ `AlterForeignServerStmt` - ALTER SERVER
- ⬜ `foreign_server_version` - Server version
- ⬜ `opt_foreign_server_version` - Optional version

### CREATE USER MAPPING
- ⬜ `CreateUserMappingStmt` - CREATE USER MAPPING
- ⬜ `AlterUserMappingStmt` - ALTER USER MAPPING
- ⬜ `DropUserMappingStmt` - DROP USER MAPPING

### CREATE EVENT TRIGGER
- ⬜ `CreateEventTrigStmt` - CREATE EVENT TRIGGER
- ⬜ `AlterEventTrigStmt` - ALTER EVENT TRIGGER
- ⬜ `event` - Event specification
- ⬜ `event_trigger_when_list` - Event trigger conditions
- ⬜ `event_trigger_when_item` - Single condition
- ⬜ `event_trigger_value_list` - Event trigger values

### Other CREATE Statements
- ⬜ `CreateTableSpaceStmt` - CREATE TABLESPACE
- ⬜ `AlterTblSpcStmt` - ALTER TABLESPACE
- ⬜ `CreatePolicyStmt` - CREATE POLICY
- ⬜ `AlterPolicyStmt` - ALTER POLICY
- ⬜ `CreateAmStmt` - CREATE ACCESS METHOD
- ⬜ `CreateStatsStmt` - CREATE STATISTICS
- ⬜ `AlterStatsStmt` - ALTER STATISTICS
- ⬜ `CreatePublicationStmt` - CREATE PUBLICATION
- ⬜ `AlterPublicationStmt` - ALTER PUBLICATION
- ⬜ `CreateSubscriptionStmt` - CREATE SUBSCRIPTION
- ⬜ `AlterSubscriptionStmt` - ALTER SUBSCRIPTION
- ⬜ `CreateCastStmt` - CREATE CAST
- ⬜ `CreateOpClassStmt` - CREATE OPERATOR CLASS
- ⬜ `CreateOpFamilyStmt` - CREATE OPERATOR FAMILY
- ⬜ `AlterOpFamilyStmt` - ALTER OPERATOR FAMILY
- ⬜ `CreateConversionStmt` - CREATE CONVERSION
- ⬜ `CreateTransformStmt` - CREATE TRANSFORM
- ⬜ `CreatePLangStmt` - CREATE LANGUAGE

## Phase 3H: Advanced SELECT Features (~60 rules)

### GROUP BY
- ⬜ `group_clause` - GROUP BY clause
- ⬜ `group_by_list` - GROUP BY list
- ⬜ `group_by_item` - GROUP BY item
- ⬜ `rollup_clause` - ROLLUP clause
- ⬜ `cube_clause` - CUBE clause
- ⬜ `grouping_sets_clause` - GROUPING SETS
- ⬜ `empty_grouping_set` - Empty grouping set

### HAVING
- ⬜ `having_clause` - HAVING clause

### ORDER BY
- ⬜ `sort_clause` - ORDER BY clause
- ⬜ `sortby_list` - Sort specification list
- ⬜ `sortby` - Single sort specification
- ⬜ `opt_asc_desc` - ASC/DESC option
- ⬜ `opt_nulls_order` - NULLS FIRST/LAST

### LIMIT/OFFSET
- ⬜ `select_limit` - LIMIT clause
- ⬜ `opt_select_limit` - Optional LIMIT
- ⬜ `limit_clause` - LIMIT specification
- ⬜ `offset_clause` - OFFSET clause
- ⬜ `select_limit_value` - LIMIT value
- ⬜ `select_offset_value` - OFFSET value
- ⬜ `select_fetch_first_value` - FETCH FIRST value
- ⬜ `row_or_rows` - ROW/ROWS keyword
- ⬜ `first_or_next` - FIRST/NEXT keyword

### Window Functions
- ⬜ `window_clause` - WINDOW clause
- ⬜ `window_definition_list` - Window definitions
- ⬜ `window_definition` - Single window definition
- ⬜ `window_specification` - Window specification
- ⬜ `over_clause` - OVER clause
- ⬜ `opt_existing_window_name` - Existing window ref
- ⬜ `opt_frame_clause` - Frame clause
- ⬜ `frame_extent` - Frame extent
- ⬜ `frame_bound` - Frame boundary
- ⬜ `opt_window_exclusion_clause` - Exclusion clause

### Aggregate Functions
- ⬜ `aggregate_with_argtypes` - Aggregate with types
- ⬜ `aggregate_with_argtypes_list` - Aggregate list
- ⬜ `aggr_arg` - Aggregate argument
- ⬜ `aggr_args` - Aggregate arguments
- ⬜ `aggr_args_list` - Aggregate argument list
- ⬜ `old_aggr_definition` - Old aggregate def
- ⬜ `old_aggr_list` - Old aggregate list
- ⬜ `old_aggr_elem` - Old aggregate element
- ⬜ `within_group_clause` - WITHIN GROUP
- ⬜ `filter_clause` - FILTER clause

### FOR UPDATE/SHARE
- ⬜ `for_locking_clause` - FOR UPDATE/SHARE
- ⬜ `opt_for_locking_clause` - Optional locking
- ⬜ `for_locking_items` - Locking items
- ⬜ `for_locking_item` - Single locking item
- ⬜ `for_locking_strength` - Locking strength
- ⬜ `locked_rels_list` - Locked relations
- ⬜ `opt_nowait` - NOWAIT option
- ⬜ `opt_nowait_or_skip` - NOWAIT or SKIP LOCKED

### INTO Clause
- ⬜ `into_clause` - INTO clause

### JSON Functions
- ⬜ `json_aggregate_func` - JSON aggregate
- ⬜ `json_argument` - JSON argument
- ⬜ `json_arguments` - JSON arguments
- ⬜ `json_value_expr` - JSON value expression
- ⬜ `json_value_expr_list` - JSON value list
- ⬜ `json_format_clause` - JSON FORMAT
- ⬜ `json_format_clause_opt` - Optional FORMAT
- ⬜ `json_returning_clause_opt` - RETURNING clause
- ⬜ `json_passing_clause_opt` - PASSING clause
- ⬜ `json_on_error_clause_opt` - ON ERROR clause
- ⬜ `json_wrapper_behavior` - Wrapper behavior

## Phase 3I: Transaction & Administrative (~80 rules)

### Transaction Control
- ⬜ `TransactionStmt` - Transaction statement
- ⬜ `TransactionStmtLegacy` - Legacy transaction
- ⬜ `opt_transaction` - TRANSACTION keyword
- ⬜ `opt_transaction_chain` - AND CHAIN option
- ⬜ `transaction_mode_list` - Transaction modes
- ⬜ `transaction_mode_list_or_empty` - Optional modes
- ⬜ `transaction_mode_item` - Single mode

### SAVEPOINT
- ⬜ `savepoint_level` - Savepoint level

### Security - Roles
- ⬜ `CreateRoleStmt` - CREATE ROLE
- ⬜ `AlterRoleStmt` - ALTER ROLE
- ⬜ `AlterRoleSetStmt` - ALTER ROLE SET
- ⬜ `DropRoleStmt` - DROP ROLE
- ⬜ `CreateGroupStmt` - CREATE GROUP
- ⬜ `AlterGroupStmt` - ALTER GROUP
- ⬜ `CreateUserStmt` - CREATE USER
- ⬜ `CreateOptRoleElem` - Role option
- ⬜ `AlterOptRoleElem` - ALTER role option
- ⬜ `AlterOptRoleList` - ALTER role options
- ⬜ `RoleId` - Role identifier
- ⬜ `RoleSpec` - Role specification
- ⬜ `role_list` - Role list
- ⬜ `add_drop` - ADD/DROP keyword
- ⬜ `opt_granted_by` - GRANTED BY clause

### GRANT/REVOKE
- ⬜ `GrantStmt` - GRANT statement
- ⬜ `RevokeStmt` - REVOKE statement
- ⬜ `GrantRoleStmt` - GRANT role
- ⬜ `RevokeRoleStmt` - REVOKE role
- ⬜ `grant_role_opt` - Grant role option
- ⬜ `grant_role_opt_list` - Grant role options
- ⬜ `grant_role_opt_value` - Grant option value
- ⬜ `privileges` - Privilege list
- ⬜ `privilege_list` - Individual privileges
- ⬜ `privilege` - Single privilege
- ⬜ `privilege_target` - Privilege target
- ⬜ `grantee_list` - Grantee list
- ⬜ `grantee` - Single grantee
- ⬜ `opt_grant_grant_option` - WITH GRANT OPTION

### DEFAULT PRIVILEGES
- ⬜ `AlterDefaultPrivilegesStmt` - ALTER DEFAULT PRIVILEGES
- ⬜ `DefACLOptionList` - Default ACL options
- ⬜ `DefACLOption` - Single ACL option
- ⬜ `DefACLAction` - ACL action
- ⬜ `defacl_privilege_target` - ACL privilege target

### SET/SHOW/RESET
- ⬜ `VariableSetStmt` - SET statement
- ⬜ `set_rest` - SET rest
- ⬜ `set_rest_more` - Additional SET options
- ⬜ `generic_set` - Generic SET
- ⬜ `var_name` - Variable name
- ⬜ `var_list` - Variable list
- ⬜ `var_value` - Variable value
- ⬜ `iso_level` - Isolation level
- ⬜ `opt_boolean_or_string` - Boolean or string
- ⬜ `zone_value` - Timezone value
- ⬜ `opt_encoding` - Encoding option
- ⬜ `NonReservedWord_or_Sconst` - Non-reserved or string
- ⬜ `VariableResetStmt` - RESET statement
- ⬜ `reset_rest` - RESET rest
- ⬜ `generic_reset` - Generic RESET
- ⬜ `VariableShowStmt` - SHOW statement
- ⬜ `SetResetClause` - SET/RESET clause
- ⬜ `FunctionSetResetClause` - Function SET/RESET
- ⬜ `AlterSystemStmt` - ALTER SYSTEM

### EXPLAIN
- ⬜ `ExplainStmt` - EXPLAIN statement
- ⬜ `ExplainableStmt` - Explainable statement
- ⬜ `utility_option_list` - Utility options
- ⬜ `utility_option_elem` - Utility option element
- ⬜ `utility_option_name` - Option name
- ⬜ `utility_option_arg` - Option argument

### VACUUM/ANALYZE
- ⬜ `VacuumStmt` - VACUUM statement
- ⬜ `AnalyzeStmt` - ANALYZE statement
- ⬜ `vacuum_relation_list` - Vacuum relations
- ⬜ `vacuum_relation` - Single vacuum relation
- ⬜ `opt_vacuum_relation_list` - Optional relations
- ⬜ `analyze_keyword` - ANALYZE/ANALYSE

### Other Administrative
- ⬜ `ClusterStmt` - CLUSTER statement
- ⬜ `cluster_index_specification` - Cluster index
- ⬜ `ReindexStmt` - REINDEX statement
- ⬜ `reindex_target_all` - REINDEX all target
- ⬜ `reindex_target_relation` - REINDEX relation
- ⬜ `opt_reindex_option_list` - REINDEX options
- ⬜ `CheckPointStmt` - CHECKPOINT
- ⬜ `DiscardStmt` - DISCARD statement

## Phase 3J: PostgreSQL-Specific & Edge Cases (~200+ rules)

### Cursors
- ⬜ `DeclareCursorStmt` - DECLARE CURSOR
- ⬜ `cursor_name` - Cursor name
- ⬜ `cursor_options` - Cursor options
- ⬜ `opt_hold` - WITH HOLD option
- ⬜ `FetchStmt` - FETCH statement
- ⬜ `fetch_args` - FETCH arguments
- ⬜ `from_in` - FROM/IN keyword
- ⬜ `opt_from_in` - Optional FROM/IN
- ⬜ `ClosePortalStmt` - CLOSE cursor

### Prepared Statements
- ⬜ `PrepareStmt` - PREPARE statement
- ⬜ `prep_type_clause` - Type clause
- ⬜ `PreparableStmt` - Preparable statement
- ⬜ `ExecuteStmt` - EXECUTE statement
- ⬜ `execute_param_clause` - Parameter clause
- ⬜ `DeallocateStmt` - DEALLOCATE

### LISTEN/NOTIFY
- ⬜ `ListenStmt` - LISTEN statement
- ⬜ `UnlistenStmt` - UNLISTEN statement
- ⬜ `NotifyStmt` - NOTIFY statement
- ⬜ `notify_payload` - NOTIFY payload

### LOAD
- ⬜ `LoadStmt` - LOAD statement

### LOCK
- ⬜ `LockStmt` - LOCK statement
- ⬜ `lock_type` - Lock type
- ⬜ `opt_lock` - Optional LOCK keyword

### TRUNCATE
- ⬜ `TruncateStmt` - TRUNCATE statement
- ⬜ `opt_restart_seqs` - RESTART IDENTITY

### Comments and Labels
- ⬜ `CommentStmt` - COMMENT statement
- ⬜ `comment_text` - Comment text
- ⬜ `SecLabelStmt` - SECURITY LABEL
- ⬜ `security_label` - Security label text

### DO Block
- ⬜ `DoStmt` - DO statement
- ⬜ `dostmt_opt_list` - DO options
- ⬜ `dostmt_opt_item` - DO option item

### CALL
- ⬜ `CallStmt` - CALL statement

### RENAME
- ⬜ `RenameStmt` - RENAME statement

### ALTER Miscellaneous
- ⬜ `AlterObjectSchemaStmt` - ALTER ... SET SCHEMA
- ⬜ `AlterOwnerStmt` - ALTER ... OWNER TO
- ⬜ `AlterOperatorStmt` - ALTER OPERATOR
- ⬜ `AlterObjectDependsStmt` - ALTER ... DEPENDS
- ⬜ `AlterCollationStmt` - ALTER COLLATION
- ⬜ `AlterDatabaseStmt` - ALTER DATABASE
- ⬜ `AlterDatabaseSetStmt` - ALTER DATABASE SET
- ⬜ `AlterCompositeTypeStmt` - ALTER TYPE (composite)
- ⬜ `AlterTSConfigurationStmt` - ALTER TEXT SEARCH CONFIG
- ⬜ `AlterTSDictionaryStmt` - ALTER TEXT SEARCH DICTIONARY

### DROP Miscellaneous
- ⬜ `DropStmt` - Generic DROP
- ⬜ `drop_type_name` - Drop type name
- ⬜ `drop_option_list` - Drop options
- ⬜ `drop_option` - Single drop option
- ⬜ `DropCastStmt` - DROP CAST
- ⬜ `DropOpClassStmt` - DROP OPERATOR CLASS
- ⬜ `DropOpFamilyStmt` - DROP OPERATOR FAMILY
- ⬜ `DropOwnedStmt` - DROP OWNED
- ⬜ `DropdbStmt` - DROP DATABASE
- ⬜ `DropTableSpaceStmt` - DROP TABLESPACE
- ⬜ `DropTransformStmt` - DROP TRANSFORM
- ⬜ `DropSubscriptionStmt` - DROP SUBSCRIPTION

### REASSIGN OWNED
- ⬜ `ReassignOwnedStmt` - REASSIGN OWNED

### CREATE DATABASE
- ⬜ `CreatedbStmt` - CREATE DATABASE
- ⬜ `createdb_opt_list` - Database options
- ⬜ `createdb_opt_items` - Database option items
- ⬜ `createdb_opt_item` - Single database option
- ⬜ `createdb_opt_name` - Database option name

### CREATE TABLESPACE
- ⬜ `OptTableSpaceOwner` - Tablespace owner

### IMPORT FOREIGN SCHEMA
- ⬜ `ImportForeignSchemaStmt` - IMPORT FOREIGN SCHEMA
- ⬜ `import_qualification` - Import qualification
- ⬜ `import_qualification_type` - Qualification type

### CREATE ASSERTION
- ⬜ `CreateAssertionStmt` - CREATE ASSERTION

### CREATE AS
- ⬜ `CreateAsStmt` - CREATE TABLE AS
- ⬜ `create_as_target` - CREATE AS target

### RULES
- ⬜ `RuleStmt` - CREATE RULE
- ⬜ `RuleActionList` - Rule actions
- ⬜ `RuleActionMulti` - Multiple rule actions
- ⬜ `RuleActionStmt` - Rule action statement
- ⬜ `RuleActionStmtOrEmpty` - Optional rule action
- ⬜ `event` - Rule event

### Row Security
- ⬜ `row_security_cmd` - Row security command
- ⬜ `RowSecurityDefaultForCmd` - Default FOR command
- ⬜ `RowSecurityDefaultPermissive` - Default permissive
- ⬜ `RowSecurityDefaultToRole` - Default TO role
- ⬜ `RowSecurityOptionalExpr` - Optional expression
- ⬜ `RowSecurityOptionalToRole` - Optional TO role
- ⬜ `RowSecurityOptionalWithCheck` - Optional WITH CHECK

### Publication/Subscription
- ⬜ `PublicationObjSpec` - Publication object
- ⬜ `pub_obj_list` - Publication object list

### Operator Classes
- ⬜ `opclass_item_list` - Operator class items
- ⬜ `opclass_item` - Operator class item
- ⬜ `opclass_purpose` - Operator class purpose
- ⬜ `opclass_drop_list` - Drop list
- ⬜ `opclass_drop` - Single drop item

### Statistics
- ⬜ `stats_params` - Statistics parameters
- ⬜ `stats_param` - Single statistics param

### Text Search
- ⬜ `text_search_config_name` - Config name
- ⬜ `text_search_dict_name` - Dictionary name

### Transform
- ⬜ `transform_element_list` - Transform elements
- ⬜ `transform_type_list` - Transform types

### Generic Options
- ⬜ `generic_option_list` - Generic options
- ⬜ `generic_option_elem` - Generic option
- ⬜ `generic_option_name` - Option name
- ⬜ `generic_option_arg` - Option argument
- ⬜ `create_generic_options` - CREATE generic options
- ⬜ `alter_generic_options` - ALTER generic options
- ⬜ `alter_generic_option_list` - ALTER option list
- ⬜ `alter_generic_option_elem` - ALTER option element

### PL/pgSQL Extensions
- ⬜ `PLAssignStmt` - PL/pgSQL assignment
- ⬜ `plassign_target` - Assignment target
- ⬜ `plassign_equals` - Assignment operator
- ⬜ `PLpgSQL_Expr` - PL/pgSQL expression

### RETURN
- ⬜ `ReturnStmt` - RETURN statement

### Constraints Set
- ⬜ `ConstraintsSetStmt` - SET CONSTRAINTS
- ⬜ `constraints_set_list` - Constraint list
- ⬜ `constraints_set_mode` - Constraint mode

### Special Functions
- ⬜ `extract_list` - EXTRACT arguments
- ⬜ `extract_arg` - EXTRACT argument
- ⬜ `overlay_list` - OVERLAY arguments
- ⬜ `position_list` - POSITION arguments
- ⬜ `substr_list` - SUBSTRING arguments
- ⬜ `trim_list` - TRIM arguments
- ⬜ `case_expr` - CASE expression
- ⬜ `when_clause_list` - WHEN clauses
- ⬜ `when_clause` - Single WHEN clause
- ⬜ `case_arg` - CASE argument
- ⬜ `case_default` - CASE default

### XML Functions
- ⬜ `xml_attributes` - XML attributes
- ⬜ `xml_attribute_list` - Attribute list
- ⬜ `xml_attribute_el` - Single attribute
- ⬜ `xml_root_version` - XML root version
- ⬜ `opt_xml_root_standalone` - Standalone option
- ⬜ `xml_namespace_list` - Namespace list
- ⬜ `xml_namespace_el` - Namespace element
- ⬜ `xml_passing_mech` - Passing mechanism
- ⬜ `xml_whitespace_option` - Whitespace option
- ⬜ `xml_indent_option` - Indent option
- ⬜ `xmlexists_argument` - XMLEXISTS argument
- ⬜ `document_or_content` - DOCUMENT/CONTENT

### JSON Additional
- ⬜ `json_name_and_value` - JSON name/value
- ⬜ `json_name_and_value_list` - Name/value list
- ⬜ `json_object_constructor_null_clause_opt` - NULL clause
- ⬜ `json_array_constructor_null_clause_opt` - Array NULL
- ⬜ `json_array_aggregate_order_by_clause_opt` - ORDER BY
- ⬜ `json_key_uniqueness_constraint_opt` - Uniqueness
- ⬜ `json_predicate_type_constraint` - Type constraint
- ⬜ `json_quotes_clause_opt` - Quotes clause
- ⬜ `json_behavior` - JSON behavior
- ⬜ `json_behavior_clause_opt` - Behavior clause
- ⬜ `json_behavior_type` - Behavior type

### Type System Details
- ⬜ `ConstTypename` - Constant type name
- ⬜ `NumericOnly` - Numeric only
- ⬜ `NumericOnly_list` - Numeric list
- ⬜ `BitWithLength` - Bit with length
- ⬜ `BitWithoutLength` - Bit without length
- ⬜ `CharacterWithLength` - Character with length
- ⬜ `CharacterWithoutLength` - Character without
- ⬜ `ConstBit` - Constant bit
- ⬜ `ConstCharacter` - Constant character
- ⬜ `opt_varying` - VARYING option
- ⬜ `opt_charset` - Character set
- ⬜ `opt_collate_clause` - COLLATE clause
- ⬜ `opt_interval` - INTERVAL option
- ⬜ `interval_second` - INTERVAL SECOND
- ⬜ `opt_timezone` - Timezone option
- ⬜ `opt_type_modifiers` - Type modifiers
- ⬜ `type_name_list` - Type name list

### Access Control
- ⬜ `opt_restrict` - RESTRICT option
- ⬜ `opt_trusted` - TRUSTED option
- ⬜ `opt_procedural` - PROCEDURAL option
- ⬜ `opt_inline_handler` - INLINE handler
- ⬜ `opt_validator` - VALIDATOR option
- ⬜ `validator_clause` - Validator clause
- ⬜ `handler_name` - Handler name

### System Catalogs
- ⬜ `object_type_any_name` - Object type (any)
- ⬜ `object_type_name` - Object type (named)
- ⬜ `object_type_name_on_any_name` - Object type on any

### Table Access
- ⬜ `table_access_method_clause` - Access method
- ⬜ `OptNoLog` - UNLOGGED option
- ⬜ `replica_identity` - REPLICA IDENTITY

### Miscellaneous
- ⬜ `opt_name_list` - Optional name list
- ⬜ `attrs` - Attribute list
- ⬜ `columnList` - Column list
- ⬜ `columnElem` - Column element
- ⬜ `opt_col_def_list` - Optional column defs
- ⬜ `opt_column_list` - Optional columns
- ⬜ `opt_column` - Optional COLUMN keyword
- ⬜ `opt_set_data` - SET DATA option
- ⬜ `opt_collate` - COLLATE option
- ⬜ `opt_class` - Operator class
- ⬜ `opt_asc_desc` - ASC/DESC option
- ⬜ `opt_nulls_order` - NULLS ordering
- ⬜ `any_with` - ANY WITH option
- ⬜ `filter_clause` - FILTER clause
- ⬜ `opt_sort_clause` - Optional SORT clause
- ⬜ `opt_array_bounds` - Array bounds
- ⬜ `opt_definition` - Optional definition
- ⬜ `opt_equal` - Optional equals
- ⬜ `opt_instead` - INSTEAD option
- ⬜ `opt_unique_null_treatment` - NULLS treatment
- ⬜ `override_kind` - Override type
- ⬜ `opt_no` - Optional NO
- ⬜ `opt_outer` - OUTER keyword
- ⬜ `opt_ordinality` - WITH ORDINALITY
- ⬜ `opt_asymmetric` - ASYMMETRIC option

### Reserved Keywords
- ⬜ `reserved_keyword` - Reserved keywords
- ⬜ `unreserved_keyword` - Unreserved keywords
- ⬜ `type_func_name_keyword` - Type/function keywords
- ⬜ `col_name_keyword` - Column name keywords
- ⬜ `bare_label_keyword` - Bare label keywords
- ⬜ `BareColLabel` - Bare column label
- ⬜ `NonReservedWord` - Non-reserved word

### Administrative Details
- ⬜ `file_name` - File name
- ⬜ `attr_name` - Attribute name
- ⬜ `param_name` - Parameter name
- ⬜ `type_function_name` - Type function name
- ⬜ `auth_ident` - Authentication identifier
- ⬜ `sub_type` - Subscription type
- ⬜ `opt_program` - PROGRAM option
- ⬜ `opt_provider` - Provider option
- ⬜ `enable_trigger` - Enable trigger option
- ⬜ `cast_context` - Cast context
- ⬜ `am_type` - Access method type
- ⬜ `opt_float` - Float option
- ⬜ `opt_default` - DEFAULT option
- ⬜ `character` - Character type base
- ⬜ `opt_using` - USING clause
- ⬜ `path_opt` - Path option
- ⬜ `I_or_F_const` - Integer or float
- ⬜ `AlteredTableInfo` - Altered table info
- ⬜ `opt_slice_bound` - Slice bound
- ⬜ `arg_class` - Argument class
- ⬜ `OptConsTableSpace` - Constraint tablespace
- ⬜ `OptConstrFromTable` - Constraint from table
- ⬜ `JsonType` - JSON type
- ⬜ `unicode_normal_form` - Unicode normal form
- ⬜ `first_or_next` - FIRST/NEXT
- ⬜ `any_name_list` - Any name list
- ⬜ `parameter_name` - Parameter name
- ⬜ `parameter_name_list` - Parameter names

## Progress Summary

**Total Rules**: 727
**Completed**: 0 (0%)
**In Progress**: 0 (0%)
**Not Started**: 727 (100%)

### Phase Breakdown:
- Phase 3A (Foundation): 0/20 completed
- Phase 3B (Expressions): 0/40 completed
- Phase 3C (SELECT Core): 0/35 completed
- Phase 3D (JOINs): 0/45 completed
- Phase 3E (DML): 0/50 completed
- Phase 3F (Basic DDL): 0/80 completed
- Phase 3G (Advanced DDL): 0/100 completed
- Phase 3H (Advanced SELECT): 0/60 completed
- Phase 3I (Transaction/Admin): 0/80 completed
- Phase 3J (PostgreSQL-Specific): 0/217 completed

## Next Steps
1. Start with Phase 3A - Grammar Foundation
2. Implement parse_toplevel, stmtmulti, stmt rules first
3. Set up goyacc integration with lexer
4. Test basic statement parsing before moving to expressions