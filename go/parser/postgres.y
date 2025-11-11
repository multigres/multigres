%{
// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//  Portions Copyright (c) 2025, Supabase, Inc
//
//  Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//  Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//
package parser

import (
	"fmt"
	"strings"
	"github.com/multigres/multigres/go/parser/ast"
)

// SelectLimit - Private struct for the result of opt_select_limit production
// Mirrors PostgreSQL's SelectLimit from gram.y:126-132
type selectLimit struct {
	limitOffset ast.Node
	limitCount  ast.Node
	limitOption ast.LimitOption
}

// LexerInterface - implements the lexer interface expected by goyacc
// Note: The actual Lexer struct is defined in lexer.go
type LexerInterface interface {
	Lex(lval *yySymType) int
	Error(s string)
}

// PrivTarget - represents a privilege target for GRANT/REVOKE statements
// Internal struct for handling privilege_target grammar rule
type PrivTarget struct {
	targtype ast.GrantTargetType
	objtype  ast.ObjectType
	objs     *ast.NodeList
}

// ImportQual - Private struct for the result of import_qualification production
// Matches PostgreSQL's ImportQual from gram.y
type ImportQual struct {
	typ        ast.ImportForeignSchemaType
	tableNames *ast.NodeList
}

// yySymType is the union type for semantic values
// This will be expanded as we add more grammar rules

%}

%union {
	// Basic types
	ival       int
	str        string
	keyword    string
	bval       bool
	byt        byte
	rune       rune

	// AST nodes
	node       ast.Node
	stmt       ast.Stmt
	stmtList   []ast.Stmt
	list       *ast.NodeList
	groupClause *ast.GroupClause
	selectLimit *selectLimit

	// Specific AST node types
	into       *ast.IntoClause
	onconflict *ast.OnConflictClause
	windef     *ast.WindowDef
	createStmt *ast.CreateStmt
	createAsStmt *ast.CreateTableAsStmt
	createAssertionStmt *ast.CreateAssertionStmt
	ruleStmt   *ast.RuleStmt
	indexStmt  *ast.IndexStmt
	alterStmt  *ast.AlterTableStmt
	dropStmt   *ast.DropStmt
	columnDef  *ast.ColumnDef
	constraint *ast.Constraint
	indexElem  *ast.IndexElem
	alterCmd   *ast.AlterTableCmd
	with	   *ast.WithClause
	rangevar   *ast.RangeVar
	objType    ast.ObjectType
	dropBehav  ast.DropBehavior
	setquant   ast.SetQuantifier
	typnam     *ast.TypeName
	partspec   *ast.PartitionSpec
	partboundspec *ast.PartitionBoundSpec
	oncommit   ast.OnCommitAction
	defelt     *ast.DefElem
	target     *ast.ResTarget   // For select targets, insert columns
	alias      *ast.Alias       // For table and column aliases
	jtype      ast.JoinType     // For join type specifications
	jexpr      *ast.JoinExpr    // For joined table expressions
	keyaction  *ast.KeyAction   // For foreign key actions
	keyactions *ast.KeyActions  // For foreign key action sets
	funparam   *ast.FunctionParameter  // For function parameters
	funparammode ast.FunctionParameterMode  // For parameter modes (IN/OUT/INOUT/VARIADIC)
	vsetstmt   *ast.VariableSetStmt    // For SET/RESET statements
	rolespec   *ast.RoleSpec
	objwithargs *ast.ObjectWithArgs
	statelem   *ast.StatsElem
	accesspriv *ast.AccessPriv         // For privilege specifications
	privtarget *PrivTarget             // For privilege target specifications
	vacrel     *ast.VacuumRelation     // For vacuum relation specifications
	importqual *ImportQual             // For import qualification specifications
	importqualtype ast.ImportForeignSchemaType // For import qualification type

	// Location tracking
	location   int
}

/*
 * Token declarations from PostgreSQL
 * Ported from postgres/src/backend/parser/gram.y:692-695
 */
%token <str>     IDENT UIDENT FCONST SCONST USCONST BCONST XCONST Op
%token <ival>    ICONST PARAM
%token           TYPECAST DOT_DOT COLON_EQUALS EQUALS_GREATER
%token           LESS_EQUALS GREATER_EQUALS NOT_EQUALS

/*
 * Reserved keywords (foundation and expression subset - will expand in later phases)
 * These are the minimum needed for foundation and expression rules
 */
%token <keyword> ALL ALTER AS CASCADE CONCURRENTLY CREATE DROP IF_P EXISTS
%token <keyword> AND NOT NULLS_P OR REPLACE RESTRICT WITH
/* Expression keywords */
%token <keyword> BETWEEN CASE COLLATE DEFAULT DISTINCT ESCAPE
%token <keyword> FALSE_P ILIKE IN_P LIKE NULL_P SIMILAR TRUE_P UNKNOWN WHEN
%token <keyword> IS ISNULL NOTNULL AT TIME ZONE LOCAL SYMMETRIC ASYMMETRIC TO
%token <keyword> OPERATOR
%token <keyword> SELECT FROM WHERE ONLY TABLE LIMIT OFFSET BY GROUP_P HAVING INTO ON
%token <keyword> JOIN INNER_P LEFT RIGHT FULL OUTER_P CROSS NATURAL USING
%token <keyword> RECURSIVE MATERIALIZED VALUES SEARCH BREADTH DEPTH CYCLE FIRST_P LAST_P SET ASC DESC
/* DML keywords for Phase 3E */
%token <keyword> INSERT UPDATE DELETE_P MERGE RETURNING CONFLICT OVERRIDING USER SYSTEM_P
%token <keyword> MATCHED THEN SOURCE TARGET DO NOTHING
/* COPY statement keywords */
%token <keyword> COPY PROGRAM STDIN STDOUT BINARY FREEZE
/* Constraint keyword */
%token <keyword> CONSTRAINT
/* Utility keywords */
%token <keyword> VERBOSE ANALYZE
%token <keyword> CURRENT_P CURSOR OF
/* Table function keywords */
%token <keyword> COLUMNS ORDINALITY XMLTABLE JSON_TABLE ROWS PATH PASSING FOR NESTED REF_P XMLNAMESPACES
/* JSON keywords */
%token <keyword> ARRAY ERROR ERROR_P EMPTY EMPTY_P OBJECT_P WRAPPER CONDITIONAL UNCONDITIONAL
%token <keyword> QUOTES OMIT KEEP SCALAR STRING_P ENCODING DELIMITER DELIMITERS HEADER_P QUOTE FORCE CSV
%token <keyword> VALUE_P JSON_QUERY JSON_VALUE JSON_SERIALIZE
%token <keyword> JSON_OBJECT JSON_ARRAY JSON_OBJECTAGG JSON_ARRAYAGG
%token <keyword> JSON_EXISTS JSON_SCALAR
%token <keyword> FORMAT JSON UTF8 WITHOUT
/* Type keywords */
%token <keyword> BIT NUMERIC INTEGER SMALLINT BIGINT REAL FLOAT_P DOUBLE_P PRECISION
%token <keyword> CHARACTER CHAR_P VARCHAR NATIONAL NCHAR VARYING
%token <keyword> TIMESTAMP INTERVAL INT_P DECIMAL_P DEC BOOLEAN_P
%token <keyword> VARIADIC
/* Unreserved keywords - additional tokens */
%token <keyword> ABORT_P ABSENT ABSOLUTE_P ACCESS ACTION ADD_P ADMIN AFTER AGGREGATE ALSO ALWAYS ASSERTION
%token <keyword> ANALYSE ASENSITIVE ASSIGNMENT ATOMIC ATTACH ATTRIBUTE AUTHORIZATION
%token <keyword> BACKWARD BEFORE BEGIN_P CACHE CALL CALLED CASCADED CATALOG_P CHAIN CHARACTERISTICS
%token <keyword> CHECKPOINT CLASS CLOSE CLUSTER COALESCE COLLATION COMMENT COMMENTS COMMIT COMMITTED
%token <keyword> COMPRESSION CONFIGURATION CONNECTION CONSTRAINTS CONTENT_P CONTINUE_P CONVERSION_P
%token <keyword> COST CUBE CURRENT_CATALOG CURRENT_DATE CURRENT_ROLE CURRENT_SCHEMA CURRENT_TIME
%token <keyword> CURRENT_TIMESTAMP CURRENT_USER DATA_P DATABASE DAY_P DEALLOCATE DECLARE DEFAULTS
%token <keyword> DEFERRABLE DEFERRED DEFINER DEPENDS DETACH DICTIONARY DISABLE_P DISCARD DOCUMENT_P
%token <keyword> DOMAIN_P EACH ELSE ENABLE_P ENCRYPTED END_P ENUM_P EVENT EXCEPT EXCLUDE EXCLUDING
%token <keyword> EXCLUSIVE EXECUTE EXPLAIN EXPRESSION EXTENSION EXTERNAL EXTRACT FAMILY FETCH FILTER
%token <keyword> FINALIZE FOLLOWING FOREIGN FORWARD FUNCTION FUNCTIONS GENERATED GLOBAL
%token <keyword> GRANT GRANTED GREATEST GROUPING GROUPS HANDLER HOLD HOUR_P IDENTITY_P IMMEDIATE
%token <keyword> IMMUTABLE IMPLICIT_P IMPORT_P INCLUDE INCLUDING INCREMENT INDENT INDEX INDEXES
%token <keyword> INHERIT INHERITS INITIALLY INLINE_P INPUT_P INSENSITIVE INSTEAD INTERSECT INVOKER
%token <keyword> ISOLATION KEYS LABEL LANGUAGE LARGE_P LATERAL_P LEAKPROOF LEAST LEADING LEVEL LISTEN
%token <keyword> LOAD LOCALTIME LOCALTIMESTAMP LOCATION LOCK_P LOCKED LOGGED MAPPING MATCH MAXVALUE
%token <keyword> MERGE_ACTION METHOD MINUTE_P MINVALUE MODE MONTH_P MOVE NAME_P NAMES NEW NEXT NFC
%token <keyword> NFD NFKC NFKD NO NONE NORMALIZE NORMALIZED NOTIFY NOWAIT NULLIF OUT_P OVERLAY
%token <keyword> OVERLAPS OWNED OWNER PARALLEL PARAMETER PARSER PARTIAL PARTITION PASSWORD PLACING
%token <keyword> PLAN PLANS POLICY POSITION PRECEDING PREPARE PREPARED PRESERVE PRIMARY PRIOR
%token <keyword> PRIVILEGES PROCEDURAL PROCEDURE PROCEDURES PUBLICATION RANGE READ REASSIGN
%token <keyword> RECHECK REFERENCES REFERENCING REFRESH REINDEX RELATIVE_P RELEASE RENAME REPEATABLE
%token <keyword> REPLICA RESET RESTART RETURN RETURNS REVOKE ROLE ROLLBACK ROLLUP ROUTINE ROUTINES
%token <keyword> ROW RULE SAVEPOINT SCHEMA SCHEMAS SCROLL SECOND_P SECURITY SEQUENCE SEQUENCES
%token <keyword> SERIALIZABLE SERVER SESSION SESSION_USER SETS SETOF SHARE SHOW SIMPLE SKIP SNAPSHOT
%token <keyword> SOME SQL_P STABLE STANDALONE_P START STATEMENT STATISTICS STORAGE STORED STRICT_P
%token <keyword> STRIP_P SUBSCRIPTION SUBSTRING SUPPORT SYSID SYSTEM_USER TABLES TABLESPACE TABLESAMPLE
%token <keyword> TEMP TEMPLATE TEMPORARY TEXT_P TIES TRAILING TRANSACTION TRANSFORM TREAT TRIGGER
%token <keyword> TRIM TRUNCATE TRUSTED TYPE_P TYPES_P UESCAPE UNBOUNDED UNCOMMITTED UNENCRYPTED
%token <keyword> UNION UNIQUE UNLISTEN UNLOGGED UNTIL VACUUM VALID VALIDATE VALIDATOR
%token <keyword> VERSION_P VIEW VIEWS VOLATILE WHITESPACE_P WINDOW WITHIN WORK WRITE XML_P XMLATTRIBUTES
%token <keyword> XMLCONCAT XMLELEMENT XMLEXISTS XMLFOREST XMLPARSE XMLPI XMLROOT XMLSERIALIZE YEAR_P
%token <keyword> YES_P INOUT OTHERS OLD
/* Additional missing tokens found during build */
%token <keyword> KEY OFF OIDS OPTION OPTIONS OVER
%token <keyword> ORDER
/* Missing tokens that are used in keyword rules */
%token <keyword> ANY BOTH CAST CHECK COLUMN
%token           COLON_EQUALS EQUALS_GREATER

/*
 * Special lookahead tokens
 * From postgres/src/backend/parser/gram.y:860-870
 */
%token           FORMAT_LA NOT_LA NULLS_LA WITH_LA WITHOUT_LA
%token           MODE_TYPE_NAME
%token           MODE_PLPGSQL_EXPR
%token           MODE_PLPGSQL_ASSIGN1
%token           MODE_PLPGSQL_ASSIGN2
%token           MODE_PLPGSQL_ASSIGN3

/*
 * Precedence declarations from PostgreSQL
 * From postgres/src/backend/parser/gram.y:874-924
 */
%left            UNION EXCEPT
%left            INTERSECT
%left            OR
%left            AND
%right           NOT
%nonassoc        IS ISNULL NOTNULL
%nonassoc        '<' '>' '=' LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%nonassoc        BETWEEN IN_P LIKE ILIKE SIMILAR NOT_LA
%nonassoc        ESCAPE
/* Keyword precedence - these assignments have global effect for conflict resolution */
%nonassoc	UNBOUNDED NESTED /* ideally would have same precedence as IDENT */
%nonassoc        IDENT SET PARTITION RANGE ROWS GROUPS PRECEDING FOLLOWING CUBE ROLLUP
                KEYS OBJECT_P SCALAR VALUE_P WITH WITHOUT PATH
%left            Op OPERATOR
/* Expression precedence */
%left            AT                /* sets precedence for AT TIME ZONE, AT LOCAL */
%left            COLLATE
%right           UMINUS
%left            '[' ']'
%left            '(' ')'
%left            TYPECAST
%left            '.'
%left            '+' '-'
%left            '*' '/' '%'
%left            '^'
/* JOIN operators - high precedence to support use as function names */
%left            JOIN CROSS LEFT FULL RIGHT INNER_P NATURAL

/*
 * Non-terminal type declarations for foundation and expressions
 */
%type <stmtList>     parse_toplevel stmtmulti
%type <stmt>         toplevel_stmt stmt
%type <str>          ColId ColLabel name BareColLabel NonReservedWord generic_option_name RoleId extract_arg
%type <list>         name_list
%type <list>         columnList any_name
%type <list>         qualified_name_list any_name_list relation_expr_list
%type <str>          opt_single_name
%type <str>          notify_payload file_name
%type <ival>         opt_lock lock_type
%type <bval>         opt_nowait opt_restart_seqs opt_no
%type <str>          unreserved_keyword col_name_keyword type_func_name_keyword reserved_keyword bare_label_keyword
%type <list>         opt_name_list
%type <ival>         opt_if_exists opt_if_not_exists
%type <bval>         opt_or_replace
%type <bval>         opt_concurrently
%type <node>         opt_with
%type <node>         alter_using
%type <list>         alter_generic_options alter_generic_option_list
%type <node>         generic_option_arg set_statistics_value
%type <defelt>       alter_generic_option_elem
%type <str>          set_access_method_name parameter_name
%type <node>         replica_identity
%type <list>         func_args func_args_list OptWith parameter_name_list NumericOnly_list
%type <groupClause>  group_clause
%type <setquant>     set_quantifier

/* LIMIT/OFFSET types */
%type <selectLimit>  opt_select_limit select_limit limit_clause
%type <node>         offset_clause select_limit_value select_offset_value
%type <node>         select_fetch_first_value I_or_F_const columnElem
%type <ival>         row_or_rows first_or_next

/* Expression types */
%type <node>         a_expr b_expr c_expr AexprConst columnref
%type <node>         func_expr func_expr_common_subexpr case_expr case_arg case_default when_clause
%type <list>         when_clause_list
%type <node>         array_expr explicit_row implicit_row
%type <list>         array_expr_list xml_attributes
%type <list>         func_arg_list func_arg_list_opt aggregate_with_argtypes_list substr_list trim_list extract_list overlay_list position_list xml_attribute_list
%type <bval>         xml_whitespace_option xml_indent_option
%type <node>         xml_root_version opt_xml_root_standalone
%type <node>         func_arg_expr
%type <list>         indirection opt_indirection oper_argtypes
%type <node>         indirection_el opt_slice_bound
%type <ival>         Iconst SignedIconst
%type <str>          Sconst
%type <str>          type_function_name character attr_name param_name
%type <typnam>	     Typename SimpleTypename ConstInterval
				     GenericType Numeric opt_float
				     Character
				     CharacterWithLength CharacterWithoutLength
				     ConstDatetime
				     Bit BitWithLength BitWithoutLength
%type <typnam>	     func_type
%type <list>         attrs opt_type_modifiers opt_interval interval_second opt_array_bounds
%type <ival>         opt_timezone opt_varying
%type <list>         expr_list row
%type <list>         opt_sort_clause
%type <node>         func_application filter_clause tablesample_clause opt_repeatable_clause
%type <list>         within_group_clause
%type <list>         window_clause window_definition_list opt_partition_clause
%type <windef>       window_definition window_specification over_clause
%type <str>          opt_existing_window_name
%type <windef>       opt_frame_clause frame_extent frame_bound
%type <ival>         opt_window_exclusion_clause
%type <list>         for_locking_clause opt_for_locking_clause for_locking_items locked_rels_list
%type <node>         for_locking_item
%type <ival>         for_locking_strength opt_nowait_or_skip
%type <list>         qual_Op any_operator qual_all_Op subquery_Op
%type <str>          all_Op MathOp
%type <node>         in_expr
%type <ival>         opt_asymmetric opt_asc_desc opt_nulls_order sub_type json_predicate_type_constraint
%type <str>          unicode_normal_form
%type <bval>         json_key_uniqueness_constraint_opt

%type <list>         group_by_list sort_clause sortby_list
%type <node>         group_by_item having_clause sortby
%type <node>         empty_grouping_set rollup_clause cube_clause grouping_sets_clause

%type <stmt>         SelectStmt PreparableStmt select_no_parens select_with_parens select_clause simple_select
%type <stmt>         InsertStmt UpdateStmt DeleteStmt MergeStmt CopyStmt
%type <stmt>         CreateStmt IndexStmt AlterTableStmt DropStmt RenameStmt CreateAsStmt CreateAssertionStmt RuleStmt
%type <stmt>         ClusterStmt ReindexStmt CheckPointStmt DiscardStmt
%type <stmt>         DeclareCursorStmt FetchStmt ClosePortalStmt PrepareStmt ExecuteStmt DeallocateStmt
%type <stmt>         ListenStmt UnlistenStmt NotifyStmt LoadStmt LockStmt TruncateStmt
%type <node>         insert_rest
%type <list>         insert_column_list set_clause_list set_target_list merge_when_list
%type <target>       insert_column_item set_target xml_attribute_el
%type <list>         set_clause
%type <onconflict>   opt_on_conflict
%type <node>         where_or_current_clause
%type <list>         returning_clause merge_values_clause opt_collate
%type <node>         merge_when_clause opt_merge_when_condition opt_conf_expr merge_update merge_delete merge_insert index_elem index_elem_options
%type <ival>         override_kind merge_when_tgt_matched merge_when_tgt_not_matched opt_asc_desc opt_nulls_order
%type <ival>         copy_from opt_program opt_freeze opt_verbose opt_analyze opt_full
%type <str>          cursor_name copy_file_name
%type <ival>         cursor_options opt_hold from_in opt_from_in
%type <stmt>         fetch_args
%type <list>         prep_type_clause execute_param_clause
%type <into>         create_as_target
%type <node>         opt_binary copy_delimiter copy_opt_item
%type <list>         copy_options copy_opt_list copy_generic_opt_list copy_generic_opt_arg_list opt_column_list index_params
%type <node>         copy_generic_opt_elem copy_generic_opt_arg copy_generic_opt_arg_list_item NumericOnly var_value
%type <ival>         opt_set_data
%type <list>         type_name_list
%type <str>          opt_boolean_or_string NonReservedWord_or_Sconst var_name
%type <list>         target_list opt_target_list var_list
%type <target>       target_el
%type <list>         from_clause from_list
%type <node>         table_ref
%type <node>         where_clause OptWhereClause
%type <alias>        alias_clause opt_alias_clause opt_alias_clause_for_join_using
%type <list>         func_alias_clause
%type <ival>         opt_all_clause
%type <list>         distinct_clause opt_distinct_clause
%type <into>	     into_clause
/* Phase 3D JOIN and CTE types */
%type <node>         joined_table
%type <ival>         join_type opt_outer
%type <node>         join_qual
%type <with>         with_clause opt_with_clause
%type <list>         cte_list using_clause
%type <node>         common_table_expr
%type <ival>         opt_materialized
%type <node>         opt_search_clause opt_cycle_clause
%type <stmt>         values_clause
/* Phase 3D Table function types */
%type <node>         func_table func_expr_windowless
%type <list>         TableFuncElementList OptTableFuncElementList
%type <node>         TableFuncElement
%type <list>         rowsfrom_list
%type <list>         rowsfrom_item
%type <list>         opt_col_def_list
%type <list>         table_func_column_list
// table_func_column type is declared with func_arg as <funparam>
%type <str>          param_name
%type <ival>         opt_ordinality
%type <node>         xmltable
%type <list>         xmltable_column_list
%type <node>         xmltable_column_el
%type <list>         xmltable_column_option_list
%type <node>         xmltable_column_option_el
%type <node>         opt_collate_clause
%type <node>         xmlexists_argument
%type <str>          xml_passing_mech
%type <list>         xml_namespace_list
%type <node>         xml_namespace_el
%type <node>         json_table
%type <list>         json_table_column_definition_list
%type <node>         json_table_column_definition
%type <node>         json_table_column_path_clause_opt
%type <node>         json_table_path_name_opt
%type <node>         json_value_expr
%type <list>         json_arguments
%type <node>         json_argument
%type <list>         json_value_expr_list
%type <node>         json_aggregate_func
%type <node>         json_name_and_value
%type <list>         json_name_and_value_list
%type <bval>         json_object_constructor_null_clause_opt
%type <bval>         json_array_constructor_null_clause_opt
%type <bval>         json_key_uniqueness_constraint_opt
%type <list>         json_array_aggregate_order_by_clause_opt
%type <node>         json_returning_clause_opt
%type <node>         json_passing_clause_opt
%type <node>         json_on_error_clause_opt
%type <node>         json_behavior
%type <ival>         json_behavior_type
%type <node>         json_behavior_clause_opt
%type <ival>         json_wrapper_behavior
%type <ival>         json_quotes_clause_opt
%type <node>         json_format_clause json_format_clause_opt
%type <node>         path_opt

/* Phase 3F DDL types */
%type <list>         OptTableElementList TableElementList OptTypedTableElementList TypedTableElementList
%type <node>         TableElement columnDef TableConstraint TableLikeClause TypedTableElement columnOptions
%type <partboundspec>PartitionBoundSpec
%type <node>         hash_partbound_elem ExclusionConstraintElem
%type <list>         ExclusionConstraintList hash_partbound
%type <ival>         TableLikeOptionList TableLikeOption ConstraintAttributeSpec ConstraintAttributeElem
%type <list>         ColQualList
%type <node>         ColConstraint ColConstraintElem ConstraintElem ConstraintAttr
%type <bval>         opt_no_inherit opt_unique opt_unique_null_treatment
%type <byt>          key_match
%type <list>         opt_c_include
%type <keyaction>    key_update key_delete key_action
%type <keyactions>   key_actions
%type <list>         OptInherit
%type <partspec>     OptPartitionSpec PartitionSpec
%type <list>         part_params
%type <node>         part_elem
%type <list>         reloptions reloption_list
%type <defelt>       reloption_elem
%type <oncommit>     OnCommitOption
%type <str>          OptTableSpace OptConsTableSpace
%type <str>          ExistingIndex
%type <str>          access_method_clause
%type <list>         index_params index_including_params
%type <node>         index_elem
%type <list>         opt_include
%type <list>         opt_qualified_name opt_reloptions
%type <list>         alter_table_cmds role_list
%type <node>         alter_table_cmd partition_cmd index_partition_cmd
%type <bval>         opt_nowait
%type <node>         alter_column_default
%type <objType>	     object_type_any_name object_type_name object_type_name_on_any_name drop_type_name
%type <node>         def_arg
%type <rune>         OptTemp
%type <str>          table_access_method_clause
%type <str>          column_compression opt_column_compression column_storage opt_column_storage
%type <list>         create_generic_options
%type <list>         generic_option_list
%type <defelt>       generic_option_elem
%type <node>         generic_option_arg
%type <bval>         opt_unique_null_treatment opt_recheck
%type <byt>          generated_when
%type <list>         SeqOptList OptParenthesizedSeqOptList alter_identity_column_option_list
%type <defelt>       SeqOptElem alter_identity_column_option
%type <node>         OptTempTableName
%type <list>         opt_definition definition def_list
%type <defelt>       def_elem
%type <dropBehav>	 opt_drop_behavior
%type <rangevar>	 qualified_name insert_target relation_expr extended_relation_expr relation_expr_opt_alias
%type <bval>         opt_with_data opt_instead
%type <rune>         OptNoLog
%type <into>         create_mv_target
%type <list>         OptSchemaEltList RuleActionList RuleActionMulti
%type <stmt>         schema_stmt RuleActionStmt RuleActionStmtOrEmpty
%type <objwithargs>  function_with_argtypes aggregate_with_argtypes operator_with_argtypes
%type <list>         alterfunc_opt_list function_with_argtypes_list operator_with_argtypes_list
%type <list>         alter_type_cmds
%type <node>         alter_type_cmd
%type <ival>     	 event

%type <stmt>         CreateFunctionStmt AlterFunctionStmt CreateTrigStmt ViewStmt ReturnStmt VariableSetStmt VariableResetStmt ConstraintsSetStmt PLAssignStmt PLpgSQL_Expr RemoveFuncStmt RemoveAggrStmt RemoveOperStmt ExplainStmt VacuumStmt VariableShowStmt AlterSystemStmt ExplainableStmt AnalyzeStmt
%type <stmt>         TransactionStmt TransactionStmtLegacy CreateRoleStmt AlterRoleStmt AlterRoleSetStmt DropRoleStmt CreateGroupStmt AlterGroupStmt CreateUserStmt GrantStmt RevokeStmt GrantRoleStmt RevokeRoleStmt AlterDefaultPrivilegesStmt CommentStmt SecLabelStmt DoStmt CallStmt
%type <str>          opt_in_database
%type <bval>         opt_transaction_chain
%type <defelt>       CreateOptRoleElem AlterOptRoleElem DefACLOption
%type <list>         utility_option_list opt_vacuum_relation_list vacuum_relation_list
%type <defelt>       utility_option_elem
%type <str>          utility_option_name analyze_keyword
%type <node>         utility_option_arg
%type <vacrel>       vacuum_relation
%type <str>          cluster_index_specification
%type <ival>         reindex_target_relation reindex_target_all
%type <list>         opt_reindex_option_list
%type <list>         transaction_mode_list transaction_mode_list_or_empty OptRoleList AlterOptRoleList role_list DefACLOptionList
%type <list>         privileges privilege_list grantee_list grant_role_opt_list
%type <ival>         defacl_privilege_target
%type <accesspriv>   privilege
%type <rolespec>     RoleSpec grantee opt_granted_by
%type <bval>         opt_grant_grant_option
%type <node>         grant_role_opt_value DefACLAction
%type <defelt>       grant_role_opt
%type <str>          comment_text opt_provider security_label
%type <list>         dostmt_opt_list
%type <defelt>       dostmt_opt_item
%type <privtarget>   privilege_target
%type <importqual>   import_qualification
%type <importqualtype> import_qualification_type
%type <str>          RoleId
%type <ival>         add_drop
%type <stmt>  		 CreateMatViewStmt RefreshMatViewStmt CreateSchemaStmt CreatedbStmt DropdbStmt DropTableSpaceStmt DropOwnedStmt ReassignOwnedStmt
%type <stmt>		 DropCastStmt DropOpClassStmt DropOpFamilyStmt DropTransformStmt DropSubscriptionStmt
%type <stmt>         CreateDomainStmt AlterDomainStmt DefineStmt AlterTypeStmt AlterCompositeTypeStmt AlterEnumStmt CreateSeqStmt AlterSeqStmt CreateExtensionStmt AlterExtensionStmt AlterExtensionContentsStmt
%type <stmt>         CreateEventTrigStmt AlterEventTrigStmt
%type <stmt>         CreateTableSpaceStmt AlterTblSpcStmt CreatePolicyStmt AlterPolicyStmt
%type <stmt>         CreateAmStmt CreateStatsStmt AlterStatsStmt CreatePublicationStmt AlterPublicationStmt CreateSubscriptionStmt AlterSubscriptionStmt
%type <stmt>         CreateCastStmt CreateOpClassStmt CreateOpFamilyStmt AlterOpFamilyStmt CreateConversionStmt CreateTransformStmt CreatePLangStmt
%type <stmt>         CreateFdwStmt AlterFdwStmt CreateForeignServerStmt AlterForeignServerStmt CreateForeignTableStmt ImportForeignSchemaStmt CreateUserMappingStmt AlterUserMappingStmt DropUserMappingStmt
%type <stmt>         AlterObjectSchemaStmt AlterOwnerStmt AlterOperatorStmt AlterObjectDependsStmt
%type <stmt>         AlterCollationStmt AlterDatabaseStmt AlterDatabaseSetStmt
%type <stmt>         AlterTSConfigurationStmt AlterTSDictionaryStmt
%type <list>         definition def_list opt_enum_val_list enum_val_list
%type <list>         OptSeqOptList OptParenthesizedSeqOptList SeqOptList create_extension_opt_list alter_extension_opt_list
%type <defelt>       SeqOptElem create_extension_opt_item alter_extension_opt_item
%type <list>         opt_fdw_options fdw_options createdb_opt_list createdb_opt_items drop_option_list
%type <defelt>       fdw_option createdb_opt_item drop_option
%type <str>          createdb_opt_name plassign_target
%type <list>         constraints_set_list
%type <bval>         constraints_set_mode
%type <str>          opt_type foreign_server_version opt_foreign_server_version
%type <rolespec>     auth_ident
%type <list>         handler_name
%type <list>         event_trigger_when_list event_trigger_value_list
%type <defelt>       event_trigger_when_item
%type <ival>         enable_trigger
%type <ival>         add_drop
%type <rolespec>     OptTableSpaceOwner
%type <bval>         RowSecurityDefaultPermissive opt_default opt_trusted opt_procedural
%type <str>          RowSecurityDefaultForCmd row_security_cmd
%type <list>         RowSecurityDefaultToRole RowSecurityOptionalToRole
%type <node>         RowSecurityOptionalExpr RowSecurityOptionalWithCheck
%type <ival>         am_type cast_context
%type <list>         stats_params pub_obj_list opclass_item_list opclass_drop_list transform_element_list
%type <statelem>     stats_param
%type <node>         PublicationObjSpec opclass_item opclass_drop
%type <list>         opt_opfamily opt_inline_handler opt_validator validator_clause
%type <bval>         opt_recheck
%type <list>         aggr_args aggr_args_list old_aggr_definition old_aggr_list
%type <defelt>       def_elem old_aggr_elem
%type <node>         def_arg opt_as DomainConstraint DomainConstraintElem aggr_arg
%type <rolespec>     RoleSpec
%type <vsetstmt>     generic_set set_rest set_rest_more generic_reset reset_rest SetResetClause FunctionSetResetClause
%type <list>         func_name func_args_with_defaults func_args_with_defaults_list
%type <funparam>     func_arg_with_default func_arg table_func_column
%type <funparammode> arg_class
%type <typnam>       func_return
%type <list>         opt_createfunc_opt_list createfunc_opt_list transform_type_list
%type <defelt>       createfunc_opt_item common_func_opt_item operator_def_elem
%type <list>         operator_def_list createdb_opt_list createdb_opt_items
%type <defelt>       createdb_opt_item
%type <str>          createdb_opt_name
%type <node>         operator_def_arg
%type <node>         func_as opt_routine_body
%type <stmt>         routine_body_stmt

%type <ival>         TriggerActionTime
%type <list>         TriggerOneEvent TriggerEvents
%type <bval>         TriggerForSpec TriggerForType TransitionOldOrNew TransitionRowOrTable
%type <list>         TriggerReferencing TriggerTransitions TriggerFuncArgs
%type <node>         TriggerTransition TriggerWhen TriggerFuncArg
%type <str>          TransitionRelName
%type <rangevar>     OptConstrFromTable

%type <list>         opt_column_list opt_reloptions
%type <ival>         opt_check_option
%type <list>         routine_body_stmt_list
%type <node>         zone_value var_value
%type <list>         var_list transaction_mode_list
%type <node>         transaction_mode_item
%type <str>          NonReservedWord_or_Sconst NonReservedWord opt_encoding iso_level
%type <ival>         document_or_content
%type <list>         attrs opclass_purpose type_list
%type <list>         opt_interval interval_second
%type <typnam>       ConstTypename ConstBit ConstCharacter JsonType

/* Start symbol */
%start parse_toplevel

%%

/*
 * Grammar Foundation & Infrastructure Rules
 * From postgres/src/backend/parser/gram.y
 */

/*
 * The target production for the whole parse.
 * From postgres/src/backend/parser/gram.y:1025-1029
 */
parse_toplevel:
			stmtmulti
			{
				$$ = $1
				// In the actual implementation, we'll set this as the parse result
				if l, ok := yylex.(interface{ SetParseTree([]ast.Stmt) }); ok {
					l.SetParseTree($1)
				}
			}
		;

/*
 * Multiple SQL statements separated by semicolons
 * From postgres/src/backend/parser/gram.y:1031-1061
 */
stmtmulti:
			stmtmulti ';' toplevel_stmt
			{
				if $3 != nil {
					$$ = append($1, $3)
				} else {
					$$ = $1
				}
			}
		|	toplevel_stmt
			{
				if $1 != nil {
					$$ = []ast.Stmt{$1}
				} else {
					$$ = []ast.Stmt{}
				}
			}
		;

/*
 * toplevel_stmt includes BEGIN and END.  stmt does not.
 * From postgres/src/backend/parser/gram.y:1063-1069
 */
toplevel_stmt:
			stmt
			{
				$$ = $1
			}
		|	TransactionStmtLegacy					{ $$ = $1 }
		;

stmt:
			SelectStmt								{ $$ = $1 }
		|	InsertStmt								{ $$ = $1 }
		|	UpdateStmt								{ $$ = $1 }
		|	DeleteStmt								{ $$ = $1 }
		|	MergeStmt								{ $$ = $1 }
		|	CopyStmt								{ $$ = $1 }
		|	CreateStmt								{ $$ = $1 }
		|	IndexStmt								{ $$ = $1 }
		|	AlterTableStmt							{ $$ = $1 }
		|	DropStmt								{ $$ = $1 }
		|	RenameStmt								{ $$ = $1 }
		|	CreateFunctionStmt						{ $$ = $1 }
		|	AlterFunctionStmt						{ $$ = $1 }
		|	CreateTrigStmt							{ $$ = $1 }
		|	ViewStmt								{ $$ = $1 }
		|	CreateMatViewStmt						{ $$ = $1 }
		|	RefreshMatViewStmt						{ $$ = $1 }
		|	CreateAsStmt							{ $$ = $1 }
		|	CreateAssertionStmt						{ $$ = $1 }
		|	RuleStmt								{ $$ = $1 }
		|	CreateSchemaStmt						{ $$ = $1 }
		|	CreatedbStmt							{ $$ = $1 }
		|	DropdbStmt								{ $$ = $1 }
		|	DropTableSpaceStmt						{ $$ = $1 }
		|	DropOwnedStmt							{ $$ = $1 }
		| 	DropCastStmt							{ $$ = $1 }
		| 	DropOpClassStmt							{ $$ = $1 }
		| 	DropOpFamilyStmt						{ $$ = $1 }
		| 	DropTransformStmt						{ $$ = $1 }
		| 	DropSubscriptionStmt					{ $$ = $1 }
		|	ReassignOwnedStmt						{ $$ = $1 }
		|	CreateDomainStmt						{ $$ = $1 }
		|	AlterDomainStmt							{ $$ = $1 }
		|	DefineStmt								{ $$ = $1 }
		|	AlterTypeStmt							{ $$ = $1 }
		|	AlterCompositeTypeStmt					{ $$ = $1 }
		|	AlterEnumStmt							{ $$ = $1 }
		|	CreateSeqStmt							{ $$ = $1 }
		|	AlterSeqStmt							{ $$ = $1 }
		|	CreateExtensionStmt						{ $$ = $1 }
		|	AlterExtensionStmt						{ $$ = $1 }
		|	AlterExtensionContentsStmt				{ $$ = $1 }
		|	CreateFdwStmt							{ $$ = $1 }
		|	AlterFdwStmt							{ $$ = $1 }
		|	CreateForeignServerStmt					{ $$ = $1 }
		|	AlterForeignServerStmt					{ $$ = $1 }
		|	CreateForeignTableStmt					{ $$ = $1 }
		|	ImportForeignSchemaStmt					{ $$ = $1 }
		|	CreateUserMappingStmt					{ $$ = $1 }
		|	AlterUserMappingStmt					{ $$ = $1 }
		|	DropUserMappingStmt						{ $$ = $1 }
		|	CreateEventTrigStmt						{ $$ = $1 }
		|	AlterEventTrigStmt						{ $$ = $1 }
		|	CreateTableSpaceStmt					{ $$ = $1 }
		|	AlterTblSpcStmt							{ $$ = $1 }
		|	CreatePolicyStmt						{ $$ = $1 }
		|	AlterPolicyStmt							{ $$ = $1 }
		|	CreateAmStmt							{ $$ = $1 }
		|	CreateStatsStmt							{ $$ = $1 }
		|	AlterStatsStmt							{ $$ = $1 }
		|	CreatePublicationStmt					{ $$ = $1 }
		|	AlterPublicationStmt					{ $$ = $1 }
		|	CreateSubscriptionStmt					{ $$ = $1 }
		|	AlterSubscriptionStmt					{ $$ = $1 }
		|	CreateCastStmt							{ $$ = $1 }
		|	CreateOpClassStmt						{ $$ = $1 }
		|	CreateOpFamilyStmt						{ $$ = $1 }
		|	AlterOpFamilyStmt						{ $$ = $1 }
		|	CreateConversionStmt					{ $$ = $1 }
		|	CreateTransformStmt						{ $$ = $1 }
		|	CreatePLangStmt							{ $$ = $1 }
		|	VariableSetStmt							{ $$ = $1 }
		|	VariableResetStmt						{ $$ = $1 }
		|	ConstraintsSetStmt						{ $$ = $1 }
		|	RemoveFuncStmt							{ $$ = $1 }
		|	RemoveAggrStmt							{ $$ = $1 }
		|	RemoveOperStmt							{ $$ = $1 }
		|	TransactionStmt							{ $$ = $1 }
		|	CreateRoleStmt							{ $$ = $1 }
		|	AlterRoleStmt							{ $$ = $1 }
		|	AlterRoleSetStmt						{ $$ = $1 }
		|	DropRoleStmt							{ $$ = $1 }
		|	CreateGroupStmt							{ $$ = $1 }
		|	AlterGroupStmt							{ $$ = $1 }
		|	CreateUserStmt							{ $$ = $1 }
		|	GrantStmt								{ $$ = $1 }
		|	RevokeStmt								{ $$ = $1 }
		|	GrantRoleStmt							{ $$ = $1 }
		|	RevokeRoleStmt							{ $$ = $1 }
		|	AlterDefaultPrivilegesStmt				{ $$ = $1 }
		|	ExplainStmt								{ $$ = $1 }
		|	VacuumStmt								{ $$ = $1 }
		| 	AnalyzeStmt								{ $$ = $1 }
		|	VariableShowStmt						{ $$ = $1 }
		|	AlterSystemStmt							{ $$ = $1 }
		|	ClusterStmt								{ $$ = $1 }
		|	ReindexStmt								{ $$ = $1 }
		|	CheckPointStmt							{ $$ = $1 }
		|	DiscardStmt								{ $$ = $1 }
		|	DeclareCursorStmt						{ $$ = $1 }
		|	FetchStmt								{ $$ = $1 }
		|	ClosePortalStmt							{ $$ = $1 }
		|	PrepareStmt								{ $$ = $1 }
		|	ExecuteStmt								{ $$ = $1 }
		|	DeallocateStmt							{ $$ = $1 }
		|	ListenStmt								{ $$ = $1 }
		|	UnlistenStmt							{ $$ = $1 }
		|	NotifyStmt								{ $$ = $1 }
		|	LoadStmt								{ $$ = $1 }
		|	LockStmt								{ $$ = $1 }
		|	TruncateStmt							{ $$ = $1 }
		|	CommentStmt								{ $$ = $1 }
		|	SecLabelStmt							{ $$ = $1 }
		|	DoStmt									{ $$ = $1 }
		|	CallStmt								{ $$ = $1 }
		|	AlterObjectSchemaStmt					{ $$ = $1 }
		|	AlterOwnerStmt							{ $$ = $1 }
		|	AlterOperatorStmt						{ $$ = $1 }
		|	AlterObjectDependsStmt					{ $$ = $1 }
		|	AlterCollationStmt						{ $$ = $1 }
		|	AlterDatabaseStmt						{ $$ = $1 }
		|	AlterDatabaseSetStmt					{ $$ = $1 }
		|	AlterTSConfigurationStmt				{ $$ = $1 }
		|	AlterTSDictionaryStmt					{ $$ = $1 }
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

/*
 * Common option clauses used across many statements
 * From various locations in postgres/src/backend/parser/gram.y
 */

opt_single_name:
			ColId									{ $$ = $1 }
		|	/* EMPTY */								{ $$ = "" }
		;

opt_qualified_name:
			any_name								{ $$ = $1 }
		|	/* EMPTY */								{ $$ = nil }
		;

opt_drop_behavior:
			CASCADE									{ $$ = ast.DropCascade }
		|	RESTRICT								{ $$ = ast.DropRestrict }
		|	/* EMPTY */								{ $$ = ast.DropRestrict }
		;

opt_if_exists:
			IF_P EXISTS								{ $$ = 1 }
		|	/* EMPTY */								{ $$ = 0 }
		;

opt_if_not_exists:
			IF_P NOT EXISTS							{ $$ = 1 }
		|	/* EMPTY */								{ $$ = 0 }
		;

opt_or_replace:
			OR REPLACE								{ $$ = true }
		|	/* EMPTY */								{ $$ = false }
		;

opt_concurrently:
			CONCURRENTLY							{ $$ = true }
		|	/* EMPTY */								{ $$ = false }
		;

OptWith:
			WITH reloptions							{ $$ = $2 }
		|	WITHOUT OIDS							{ $$ = nil }
		|	/* EMPTY */								{ $$ = nil }
		;

/*
 * Names and identifiers
 * From postgres/src/backend/parser/gram.y:17468-17491
 */

ColId:
			IDENT									{ $$ = $1 }
		|	unreserved_keyword						{ $$ = $1 }
		|	col_name_keyword						{ $$ = $1 }
		;

ColLabel:
			IDENT									{ $$ = $1 }
		|	unreserved_keyword						{ $$ = $1 }
		|	col_name_keyword						{ $$ = $1 }
		|	type_func_name_keyword					{ $$ = $1 }
		|	reserved_keyword						{ $$ = $1 }
		;

BareColLabel:	IDENT								{ $$ = $1; }
			| bare_label_keyword					{ $$ = $1; }

name:
			ColId									{ $$ = $1 }
		;

name_list:
			name
			{
				$$ = ast.NewNodeList(ast.NewString($1))
			}
		|	name_list ',' name
			{
				list := $1
				list.Append(ast.NewString($3))
				$$ = list
			}
		;

columnList:
			columnElem
			{
				$$ = ast.NewNodeList($1)
			}
		|	columnList ',' columnElem
			{
				$1.Append($3)
				$$ = $1
			}
		;

columnElem: ColId
			{
				$$ = ast.NewString($1);
			}
		;

qualified_name:
			ColId
			{
				$$ = &ast.RangeVar{
					RelName: $1,
					Inh:     true, // inheritance enabled by default
				}
			}
		|	ColId indirection
			{
				// Handle complex qualified names like "schema.table.field" or "catalog.schema.table"
				// This creates a RangeVar from indirection - for now we'll handle 2-part names only
				// Full indirection support would require more complex parsing
				if len($2.Items) == 1 {
					if str, ok := $2.Items[0].(*ast.String); ok {
						$$ = &ast.RangeVar{
							SchemaName: $1,
							RelName:    str.SVal,
							Inh:        true, // inheritance enabled by default
						}
					} else {
						// Complex indirection - return a simpler form for now
						$$ = &ast.RangeVar{
							RelName: $1,
							Inh:     true, // inheritance enabled by default
						}
					}
				} else {
					// Multiple indirection elements - return simple form
					$$ = &ast.RangeVar{
						RelName: $1,
						Inh:     true, // inheritance enabled by default
					}
				}
			}
		;

opt_name_list:
			'(' name_list ')'					{ $$ = $2 }
		|	/* EMPTY */							{ $$ = nil }
		;

qualified_name_list:
			qualified_name
			{
				$$ = ast.NewNodeList($1)
			}
		|	qualified_name_list ',' qualified_name
			{
				list := $1
				list.Append($3)
				$$ = list
			}
		;

relation_expr_list:
		relation_expr
			{
				$$ = ast.NewNodeList($1)
			}
	|	relation_expr_list ',' relation_expr
			{
				list := $1
				list.Append($3)
				$$ = list
			}
		;

opt_table:	TABLE
		|	/* EMPTY */
		;

OptTempTableName:
			TEMPORARY opt_table qualified_name
				{
					rangeVar := $3
					rangeVar.RelPersistence = ast.RELPERSISTENCE_TEMP
					$$ = rangeVar
				}
		|	TEMP opt_table qualified_name
				{
					rangeVar := $3
					rangeVar.RelPersistence = ast.RELPERSISTENCE_TEMP
					$$ = rangeVar
				}
		|	LOCAL TEMPORARY opt_table qualified_name
				{
					rangeVar := $4
					rangeVar.RelPersistence = ast.RELPERSISTENCE_TEMP
					$$ = rangeVar
				}
		|	LOCAL TEMP opt_table qualified_name
				{
					rangeVar := $4
					rangeVar.RelPersistence = ast.RELPERSISTENCE_TEMP
					$$ = rangeVar
				}
		|	GLOBAL TEMPORARY opt_table qualified_name
				{
					// GLOBAL is deprecated but still accepted
					rangeVar := $4
					rangeVar.RelPersistence = ast.RELPERSISTENCE_TEMP
					$$ = rangeVar
				}
		|	GLOBAL TEMP opt_table qualified_name
				{
					// GLOBAL is deprecated but still accepted
					rangeVar := $4
					rangeVar.RelPersistence = ast.RELPERSISTENCE_TEMP
					$$ = rangeVar
				}
		|	UNLOGGED opt_table qualified_name
				{
					rangeVar := $3
					rangeVar.RelPersistence = ast.RELPERSISTENCE_UNLOGGED
					$$ = rangeVar
				}
		|	TABLE qualified_name
				{
					rangeVar := $2
					rangeVar.RelPersistence = ast.RELPERSISTENCE_PERMANENT
					$$ = rangeVar
				}
		|	qualified_name
				{
					rangeVar := $1
					rangeVar.RelPersistence = ast.RELPERSISTENCE_PERMANENT
					$$ = rangeVar
				}
		;

any_name:
			ColId
			{
				$$ = ast.NewNodeList(ast.NewString($1))
			}
		|	ColId attrs
			{
				// Create a NodeList with the first ColId followed by all attrs
				nodes := ast.NewNodeList(ast.NewString($1))
				for _, item := range $2.Items {
					nodes.Append(item)
				}
				$$ = nodes
			}
		;

any_name_list:
			any_name
			{
				$$ = ast.NewNodeList($1)
			}
		|	any_name_list ',' any_name
			{
				list := $1
				list.Append($3)
				$$ = list
			}
		;

/* "Unreserved" keywords --- available for use as any kind of name.
 */
unreserved_keyword:
			  ABORT_P									{ $$ = "abort" }
			| ABSENT										{ $$ = "absent" }
			| ABSOLUTE_P									{ $$ = "absolute" }
			| ACCESS										{ $$ = "access" }
			| ACTION										{ $$ = "action" }
			| ADD_P										{ $$ = "add" }
			| ADMIN										{ $$ = "admin" }
			| AFTER										{ $$ = "after" }
			| AGGREGATE									{ $$ = "aggregate" }
			| ALSO										{ $$ = "also" }
			| ALTER										{ $$ = "alter" }
			| ALWAYS										{ $$ = "always" }
			| ASENSITIVE									{ $$ = "asensitive" }
			| ASSERTION									{ $$ = "assertion" }
			| ASSIGNMENT									{ $$ = "assignment" }
			| AT											{ $$ = "at" }
			| ATOMIC										{ $$ = "atomic" }
			| ATTACH										{ $$ = "attach" }
			| ATTRIBUTE									{ $$ = "attribute" }
			| BACKWARD									{ $$ = "backward" }
			| BEFORE										{ $$ = "before" }
			| BEGIN_P										{ $$ = "begin" }
			| BREADTH										{ $$ = "breadth" }
			| BY											{ $$ = "by" }
			| CACHE										{ $$ = "cache" }
			| CALL										{ $$ = "call" }
			| CALLED										{ $$ = "called" }
			| CASCADE										{ $$ = "cascade" }
			| CASCADED									{ $$ = "cascaded" }
			| CATALOG_P									{ $$ = "catalog" }
			| CHAIN										{ $$ = "chain" }
			| CHARACTERISTICS								{ $$ = "characteristics" }
			| CHECKPOINT									{ $$ = "checkpoint" }
			| CLASS										{ $$ = "class" }
			| CLOSE										{ $$ = "close" }
			| CLUSTER										{ $$ = "cluster" }
			| COLUMNS										{ $$ = "columns" }
			| COMMENT										{ $$ = "comment" }
			| COMMENTS									{ $$ = "comments" }
			| COMMIT										{ $$ = "commit" }
			| COMMITTED									{ $$ = "committed" }
			| COMPRESSION									{ $$ = "compression" }
			| CONDITIONAL									{ $$ = "conditional" }
			| CONFIGURATION								{ $$ = "configuration" }
			| CONFLICT									{ $$ = "conflict" }
			| CONNECTION									{ $$ = "connection" }
			| CONSTRAINTS									{ $$ = "constraints" }
			| CONTENT_P									{ $$ = "content" }
			| CONTINUE_P									{ $$ = "continue" }
			| CONVERSION_P									{ $$ = "conversion" }
			| COPY										{ $$ = "copy" }
			| COST										{ $$ = "cost" }
			| CSV										{ $$ = "csv" }
			| CUBE										{ $$ = "cube" }
			| CURRENT_P									{ $$ = "current" }
			| CURSOR										{ $$ = "cursor" }
			| CYCLE										{ $$ = "cycle" }
			| DATA_P										{ $$ = "data" }
			| DATABASE									{ $$ = "database" }
			| DAY_P										{ $$ = "day" }
			| DEALLOCATE									{ $$ = "deallocate" }
			| DECLARE										{ $$ = "declare" }
			| DEFAULTS									{ $$ = "defaults" }
			| DEFERRED									{ $$ = "deferred" }
			| DEFINER										{ $$ = "definer" }
			| DELETE_P									{ $$ = "delete" }
			| DELIMITER									{ $$ = "delimiter" }
			| DELIMITERS									{ $$ = "delimiters" }
			| DEPENDS										{ $$ = "depends" }
			| DEPTH										{ $$ = "depth" }
			| DETACH										{ $$ = "detach" }
			| DICTIONARY									{ $$ = "dictionary" }
			| DISABLE_P									{ $$ = "disable" }
			| DISCARD										{ $$ = "discard" }
			| DOCUMENT_P									{ $$ = "document" }
			| DOMAIN_P									{ $$ = "domain" }
			| DOUBLE_P									{ $$ = "double" }
			| DROP										{ $$ = "drop" }
			| EACH										{ $$ = "each" }
			| EMPTY_P										{ $$ = "empty" }
			| ENABLE_P									{ $$ = "enable" }
			| ENCODING									{ $$ = "encoding" }
			| ENCRYPTED									{ $$ = "encrypted" }
			| ENUM_P										{ $$ = "enum" }
			| ERROR_P										{ $$ = "error" }
			| ESCAPE										{ $$ = "escape" }
			| EVENT										{ $$ = "event" }
			| EXCLUDE										{ $$ = "exclude" }
			| EXCLUDING									{ $$ = "excluding" }
			| EXCLUSIVE									{ $$ = "exclusive" }
			| EXECUTE										{ $$ = "execute" }
			| EXPLAIN										{ $$ = "explain" }
			| EXPRESSION									{ $$ = "expression" }
			| EXTENSION									{ $$ = "extension" }
			| EXTERNAL									{ $$ = "external" }
			| FAMILY										{ $$ = "family" }
			| FILTER										{ $$ = "filter" }
			| FINALIZE									{ $$ = "finalize" }
			| FIRST_P										{ $$ = "first" }
			| FOLLOWING									{ $$ = "following" }
			| FORCE										{ $$ = "force" }
			| FORMAT										{ $$ = "format" }
			| FORWARD										{ $$ = "forward" }
			| FUNCTION									{ $$ = "function" }
			| FUNCTIONS									{ $$ = "functions" }
			| GENERATED									{ $$ = "generated" }
			| GLOBAL										{ $$ = "global" }
			| GRANTED										{ $$ = "granted" }
			| GROUPS										{ $$ = "groups" }
			| HANDLER										{ $$ = "handler" }
			| HEADER_P									{ $$ = "header" }
			| HOLD										{ $$ = "hold" }
			| HOUR_P										{ $$ = "hour" }
			| IDENTITY_P									{ $$ = "identity" }
			| IF_P										{ $$ = "if" }
			| IMMEDIATE									{ $$ = "immediate" }
			| IMMUTABLE									{ $$ = "immutable" }
			| IMPLICIT_P									{ $$ = "implicit" }
			| IMPORT_P									{ $$ = "import" }
			| INCLUDE										{ $$ = "include" }
			| INCLUDING									{ $$ = "including" }
			| INCREMENT									{ $$ = "increment" }
			| INDENT										{ $$ = "indent" }
			| INDEX										{ $$ = "index" }
			| INDEXES										{ $$ = "indexes" }
			| INHERIT										{ $$ = "inherit" }
			| INHERITS									{ $$ = "inherits" }
			| INLINE_P									{ $$ = "inline" }
			| INPUT_P										{ $$ = "input" }
			| INSENSITIVE									{ $$ = "insensitive" }
			| INSERT										{ $$ = "insert" }
			| INSTEAD										{ $$ = "instead" }
			| INVOKER										{ $$ = "invoker" }
			| ISOLATION									{ $$ = "isolation" }
			| KEEP										{ $$ = "keep" }
			| KEY										{ $$ = "key" }
			| KEYS										{ $$ = "keys" }
			| LABEL										{ $$ = "label" }
			| LANGUAGE									{ $$ = "language" }
			| LARGE_P										{ $$ = "large" }
			| LAST_P										{ $$ = "last" }
			| LEAKPROOF									{ $$ = "leakproof" }
			| LEVEL										{ $$ = "level" }
			| LISTEN										{ $$ = "listen" }
			| LOAD										{ $$ = "load" }
			| LOCAL										{ $$ = "local" }
			| LOCATION									{ $$ = "location" }
			| LOCK_P										{ $$ = "lock" }
			| LOCKED										{ $$ = "locked" }
			| LOGGED										{ $$ = "logged" }
			| MAPPING										{ $$ = "mapping" }
			| MATCH										{ $$ = "match" }
			| MATCHED										{ $$ = "matched" }
			| MATERIALIZED									{ $$ = "materialized" }
			| MAXVALUE									{ $$ = "maxvalue" }
			| MERGE										{ $$ = "merge" }
			| METHOD										{ $$ = "method" }
			| MINUTE_P									{ $$ = "minute" }
			| MINVALUE									{ $$ = "minvalue" }
			| MODE										{ $$ = "mode" }
			| MONTH_P										{ $$ = "month" }
			| MOVE										{ $$ = "move" }
			| NAME_P										{ $$ = "name" }
			| NAMES										{ $$ = "names" }
			| NESTED										{ $$ = "nested" }
			| NEW										{ $$ = "new" }
			| NEXT										{ $$ = "next" }
			| NFC										{ $$ = "nfc" }
			| NFD										{ $$ = "nfd" }
			| NFKC										{ $$ = "nfkc" }
			| NFKD										{ $$ = "nfkd" }
			| NO										{ $$ = "no" }
			| NORMALIZED									{ $$ = "normalized" }
			| NOTHING										{ $$ = "nothing" }
			| NOTIFY										{ $$ = "notify" }
			| NOWAIT										{ $$ = "nowait" }
			| NULLS_P										{ $$ = "nulls" }
			| OBJECT_P									{ $$ = "object" }
			| OF										{ $$ = "of" }
			| OFF										{ $$ = "off" }
			| OIDS										{ $$ = "oids" }
			| OLD										{ $$ = "old" }
			| OMIT										{ $$ = "omit" }
			| OPERATOR									{ $$ = "operator" }
			| OPTION										{ $$ = "option" }
			| OPTIONS										{ $$ = "options" }
			| ORDINALITY									{ $$ = "ordinality" }
			| OTHERS										{ $$ = "others" }
			| OVER										{ $$ = "over" }
			| OVERRIDING									{ $$ = "overriding" }
			| OWNED										{ $$ = "owned" }
			| OWNER										{ $$ = "owner" }
			| PARALLEL									{ $$ = "parallel" }
			| PARAMETER									{ $$ = "parameter" }
			| PARSER										{ $$ = "parser" }
			| PARTIAL										{ $$ = "partial" }
			| PARTITION									{ $$ = "partition" }
			| PASSING										{ $$ = "passing" }
			| PASSWORD									{ $$ = "password" }
			| PATH										{ $$ = "path" }
			| PLAN										{ $$ = "plan" }
			| PLANS										{ $$ = "plans" }
			| POLICY										{ $$ = "policy" }
			| PRECEDING									{ $$ = "preceding" }
			| PREPARE										{ $$ = "prepare" }
			| PREPARED									{ $$ = "prepared" }
			| PRESERVE									{ $$ = "preserve" }
			| PRIOR										{ $$ = "prior" }
			| PRIVILEGES									{ $$ = "privileges" }
			| PROCEDURAL									{ $$ = "procedural" }
			| PROCEDURE									{ $$ = "procedure" }
			| PROCEDURES									{ $$ = "procedures" }
			| PROGRAM										{ $$ = "program" }
			| PUBLICATION									{ $$ = "publication" }
			| QUOTE										{ $$ = "quote" }
			| QUOTES										{ $$ = "quotes" }
			| RANGE										{ $$ = "range" }
			| READ										{ $$ = "read" }
			| REASSIGN									{ $$ = "reassign" }
			| RECHECK										{ $$ = "recheck" }
			| RECURSIVE									{ $$ = "recursive" }
			| REF_P										{ $$ = "ref" }
			| REFERENCING									{ $$ = "referencing" }
			| REFRESH										{ $$ = "refresh" }
			| REINDEX										{ $$ = "reindex" }
			| RELATIVE_P									{ $$ = "relative" }
			| RELEASE										{ $$ = "release" }
			| RENAME										{ $$ = "rename" }
			| REPEATABLE									{ $$ = "repeatable" }
			| REPLACE										{ $$ = "replace" }
			| REPLICA										{ $$ = "replica" }
			| RESET										{ $$ = "reset" }
			| RESTART										{ $$ = "restart" }
			| RESTRICT									{ $$ = "restrict" }
			| RETURN										{ $$ = "return" }
			| RETURNS										{ $$ = "returns" }
			| REVOKE										{ $$ = "revoke" }
			| ROLE										{ $$ = "role" }
			| ROLLBACK									{ $$ = "rollback" }
			| ROLLUP										{ $$ = "rollup" }
			| ROUTINE										{ $$ = "routine" }
			| ROUTINES									{ $$ = "routines" }
			| ROWS										{ $$ = "rows" }
			| RULE										{ $$ = "rule" }
			| SAVEPOINT									{ $$ = "savepoint" }
			| SCALAR										{ $$ = "scalar" }
			| SCHEMA										{ $$ = "schema" }
			| SCHEMAS										{ $$ = "schemas" }
			| SCROLL										{ $$ = "scroll" }
			| SEARCH										{ $$ = "search" }
			| SECOND_P									{ $$ = "second" }
			| SECURITY									{ $$ = "security" }
			| SEQUENCE									{ $$ = "sequence" }
			| SEQUENCES									{ $$ = "sequences" }
			| SERIALIZABLE									{ $$ = "serializable" }
			| SERVER										{ $$ = "server" }
			| SESSION										{ $$ = "session" }
			| SET										{ $$ = "set" }
			| SETS										{ $$ = "sets" }
			| SHARE										{ $$ = "share" }
			| SHOW										{ $$ = "show" }
			| SIMPLE										{ $$ = "simple" }
			| SKIP										{ $$ = "skip" }
			| SNAPSHOT									{ $$ = "snapshot" }
			| SOURCE										{ $$ = "source" }
			| SQL_P										{ $$ = "sql" }
			| STABLE										{ $$ = "stable" }
			| STANDALONE_P									{ $$ = "standalone" }
			| START										{ $$ = "start" }
			| STATEMENT									{ $$ = "statement" }
			| STATISTICS									{ $$ = "statistics" }
			| STDIN										{ $$ = "stdin" }
			| STDOUT										{ $$ = "stdout" }
			| STORAGE										{ $$ = "storage" }
			| STORED										{ $$ = "stored" }
			| STRICT_P									{ $$ = "strict" }
			| STRING_P									{ $$ = "string" }
			| STRIP_P										{ $$ = "strip" }
			| SUBSCRIPTION									{ $$ = "subscription" }
			| SUPPORT										{ $$ = "support" }
			| SYSID										{ $$ = "sysid" }
			| SYSTEM_P									{ $$ = "system" }
			| TABLES										{ $$ = "tables" }
			| TABLESPACE									{ $$ = "tablespace" }
			| TARGET										{ $$ = "target" }
			| TEMP										{ $$ = "temp" }
			| TEMPLATE									{ $$ = "template" }
			| TEMPORARY									{ $$ = "temporary" }
			| TEXT_P										{ $$ = "text" }
			| TIES										{ $$ = "ties" }
			| TRANSACTION									{ $$ = "transaction" }
			| TRANSFORM									{ $$ = "transform" }
			| TRIGGER										{ $$ = "trigger" }
			| TRUNCATE									{ $$ = "truncate" }
			| TRUSTED										{ $$ = "trusted" }
			| TYPE_P										{ $$ = "type" }
			| TYPES_P										{ $$ = "types" }
			| UESCAPE										{ $$ = "uescape" }
			| UNBOUNDED									{ $$ = "unbounded" }
			| UNCOMMITTED									{ $$ = "uncommitted" }
			| UNCONDITIONAL								{ $$ = "unconditional" }
			| UNENCRYPTED									{ $$ = "unencrypted" }
			| UNKNOWN										{ $$ = "unknown" }
			| UNLISTEN									{ $$ = "unlisten" }
			| UNLOGGED									{ $$ = "unlogged" }
			| UNTIL										{ $$ = "until" }
			| UPDATE										{ $$ = "update" }
			| VACUUM										{ $$ = "vacuum" }
			| VALID										{ $$ = "valid" }
			| VALIDATE									{ $$ = "validate" }
			| VALIDATOR									{ $$ = "validator" }
			| VALUE_P										{ $$ = "value" }
			| VARYING										{ $$ = "varying" }
			| VERSION_P									{ $$ = "version" }
			| VIEW										{ $$ = "view" }
			| VIEWS										{ $$ = "views" }
			| VOLATILE										{ $$ = "volatile" }
			| WHITESPACE_P									{ $$ = "whitespace" }
			| WITHIN										{ $$ = "within" }
			| WITHOUT										{ $$ = "without" }
			| WORK										{ $$ = "work" }
			| WRAPPER										{ $$ = "wrapper" }
			| WRITE										{ $$ = "write" }
			| XML_P										{ $$ = "xml" }
			| YEAR_P										{ $$ = "year" }
			| YES_P										{ $$ = "yes" }
			| ZONE										{ $$ = "zone" }
		;

/* Column identifier --- keywords that can be column, table, etc names.
 *
 * Many of these keywords will in fact be recognized as type or function
 * names too; but they have special productions for the purpose, and so
 * can't be treated as "generic" type or function names.
 *
 * The type names appearing here are not usable as function names
 * because they can be followed by '(' in typename productions, which
 * looks too much like a function call for an LR(1) parser.
 */
col_name_keyword:
			  BETWEEN									{ $$ = "between" }
			| BIGINT										{ $$ = "bigint" }
			| BIT										{ $$ = "bit" }
			| BOOLEAN_P									{ $$ = "boolean" }
			| CHAR_P										{ $$ = "char" }
			| CHARACTER									{ $$ = "character" }
			| COALESCE									{ $$ = "coalesce" }
			| DEC										{ $$ = "dec" }
			| DECIMAL_P									{ $$ = "decimal" }
			| EXISTS										{ $$ = "exists" }
			| EXTRACT										{ $$ = "extract" }
			| FLOAT_P										{ $$ = "float" }
			| GREATEST									{ $$ = "greatest" }
			| GROUPING									{ $$ = "grouping" }
			| INOUT										{ $$ = "inout" }
			| INT_P										{ $$ = "int" }
			| INTEGER										{ $$ = "integer" }
			| INTERVAL									{ $$ = "interval" }
			| JSON										{ $$ = "json" }
			| JSON_ARRAY									{ $$ = "json_array" }
			| JSON_ARRAYAGG								{ $$ = "json_arrayagg" }
			| JSON_EXISTS									{ $$ = "json_exists" }
			| JSON_OBJECT									{ $$ = "json_object" }
			| JSON_OBJECTAGG								{ $$ = "json_objectagg" }
			| JSON_QUERY									{ $$ = "json_query" }
			| JSON_SCALAR									{ $$ = "json_scalar" }
			| JSON_SERIALIZE								{ $$ = "json_serialize" }
			| JSON_TABLE									{ $$ = "json_table" }
			| JSON_VALUE									{ $$ = "json_value" }
			| LEAST										{ $$ = "least" }
			| MERGE_ACTION									{ $$ = "merge_action" }
			| NATIONAL									{ $$ = "national" }
			| NCHAR										{ $$ = "nchar" }
			| NONE										{ $$ = "none" }
			| NORMALIZE									{ $$ = "normalize" }
			| NULLIF										{ $$ = "nullif" }
			| NUMERIC										{ $$ = "numeric" }
			| OUT_P										{ $$ = "out" }
			| OVERLAY										{ $$ = "overlay" }
			| POSITION									{ $$ = "position" }
			| PRECISION									{ $$ = "precision" }
			| REAL										{ $$ = "real" }
			| ROW										{ $$ = "row" }
			| SETOF										{ $$ = "setof" }
			| SMALLINT									{ $$ = "smallint" }
			| SUBSTRING									{ $$ = "substring" }
			| TIME										{ $$ = "time" }
			| TIMESTAMP									{ $$ = "timestamp" }
			| TREAT										{ $$ = "treat" }
			| TRIM										{ $$ = "trim" }
			| VALUES										{ $$ = "values" }
			| VARCHAR										{ $$ = "varchar" }
			| XMLATTRIBUTES								{ $$ = "xmlattributes" }
			| XMLCONCAT									{ $$ = "xmlconcat" }
			| XMLELEMENT									{ $$ = "xmlelement" }
			| XMLEXISTS									{ $$ = "xmlexists" }
			| XMLFOREST									{ $$ = "xmlforest" }
			| XMLNAMESPACES								{ $$ = "xmlnamespaces" }
			| XMLPARSE									{ $$ = "xmlparse" }
			| XMLPI										{ $$ = "xmlpi" }
			| XMLROOT										{ $$ = "xmlroot" }
			| XMLSERIALIZE									{ $$ = "xmlserialize" }
			| XMLTABLE									{ $$ = "xmltable" }
		;

/* Type/function identifier --- keywords that can be type or function names.
 *
 * Most of these are keywords that are used as operators in expressions;
 * in general such keywords can't be column names because they would be
 * ambiguous with variables, but they are unambiguous as function identifiers.
 *
 * Do not include POSITION, SUBSTRING, etc here since they have explicit
 * productions in a_expr to support the goofy SQL9x argument syntax.
 * - thomas 2000-11-28
 */
type_func_name_keyword:
			  AUTHORIZATION								{ $$ = "authorization" }
			| BINARY										{ $$ = "binary" }
			| COLLATION									{ $$ = "collation" }
			| CONCURRENTLY								{ $$ = "concurrently" }
			| CROSS										{ $$ = "cross" }
			| CURRENT_SCHEMA								{ $$ = "current_schema" }
			| FREEZE										{ $$ = "freeze" }
			| FULL										{ $$ = "full" }
			| ILIKE										{ $$ = "ilike" }
			| INNER_P										{ $$ = "inner" }
			| IS										{ $$ = "is" }
			| ISNULL										{ $$ = "isnull" }
			| JOIN										{ $$ = "join" }
			| LEFT										{ $$ = "left" }
			| LIKE										{ $$ = "like" }
			| NATURAL										{ $$ = "natural" }
			| NOTNULL										{ $$ = "notnull" }
			| OUTER_P										{ $$ = "outer" }
			| OVERLAPS									{ $$ = "overlaps" }
			| RIGHT										{ $$ = "right" }
			| SIMILAR										{ $$ = "similar" }
			| TABLESAMPLE									{ $$ = "tablesample" }
			| VERBOSE										{ $$ = "verbose" }
		;

/* Reserved keyword --- these keywords are usable only as a ColLabel.
 *
 * Keywords appear here if they could not be distinguished from variable,
 * type, or function names in some contexts.  Don't put things here unless
 * forced to.
 */
reserved_keyword:
			  ALL										{ $$ = "all" }
			| ANALYSE										{ $$ = "analyse" }
			| ANALYZE										{ $$ = "analyze" }
			| AND										{ $$ = "and" }
			| ANY										{ $$ = "any" }
			| ARRAY										{ $$ = "array" }
			| AS										{ $$ = "as" }
			| ASC										{ $$ = "asc" }
			| ASYMMETRIC									{ $$ = "asymmetric" }
			| BOTH										{ $$ = "both" }
			| CASE										{ $$ = "case" }
			| CAST										{ $$ = "cast" }
			| CHECK										{ $$ = "check" }
			| COLLATE										{ $$ = "collate" }
			| COLUMN										{ $$ = "column" }
			| CONSTRAINT									{ $$ = "constraint" }
			| CREATE										{ $$ = "create" }
			| CURRENT_CATALOG								{ $$ = "current_catalog" }
			| CURRENT_DATE									{ $$ = "current_date" }
			| CURRENT_ROLE									{ $$ = "current_role" }
			| CURRENT_TIME									{ $$ = "current_time" }
			| CURRENT_TIMESTAMP								{ $$ = "current_timestamp" }
			| CURRENT_USER									{ $$ = "current_user" }
			| DEFAULT										{ $$ = "default" }
			| DEFERRABLE									{ $$ = "deferrable" }
			| DESC										{ $$ = "desc" }
			| DISTINCT									{ $$ = "distinct" }
			| DO										{ $$ = "do" }
			| ELSE										{ $$ = "else" }
			| END_P										{ $$ = "end" }
			| EXCEPT										{ $$ = "except" }
			| FALSE_P										{ $$ = "false" }
			| FETCH										{ $$ = "fetch" }
			| FOR										{ $$ = "for" }
			| FOREIGN										{ $$ = "foreign" }
			| FROM										{ $$ = "from" }
			| GRANT										{ $$ = "grant" }
			| GROUP_P										{ $$ = "group" }
			| HAVING										{ $$ = "having" }
			| IN_P										{ $$ = "in" }
			| INITIALLY									{ $$ = "initially" }
			| INTERSECT									{ $$ = "intersect" }
			| INTO										{ $$ = "into" }
			| LATERAL_P									{ $$ = "lateral" }
			| LEADING										{ $$ = "leading" }
			| LIMIT										{ $$ = "limit" }
			| LOCALTIME									{ $$ = "localtime" }
			| LOCALTIMESTAMP								{ $$ = "localtimestamp" }
			| NOT										{ $$ = "not" }
			| NULL_P										{ $$ = "null" }
			| OFFSET										{ $$ = "offset" }
			| ON										{ $$ = "on" }
			| ONLY										{ $$ = "only" }
			| OR										{ $$ = "or" }
			| ORDER										{ $$ = "order" }
			| PLACING										{ $$ = "placing" }
			| PRIMARY										{ $$ = "primary" }
			| REFERENCES									{ $$ = "references" }
			| RETURNING									{ $$ = "returning" }
			| SELECT										{ $$ = "select" }
			| SESSION_USER									{ $$ = "session_user" }
			| SOME										{ $$ = "some" }
			| SYMMETRIC									{ $$ = "symmetric" }
			| SYSTEM_USER									{ $$ = "system_user" }
			| TABLE										{ $$ = "table" }
			| THEN										{ $$ = "then" }
			| TO										{ $$ = "to" }
			| TRAILING									{ $$ = "trailing" }
			| TRUE_P										{ $$ = "true" }
			| UNION										{ $$ = "union" }
			| UNIQUE										{ $$ = "unique" }
			| USER										{ $$ = "user" }
			| USING										{ $$ = "using" }
			| VARIADIC									{ $$ = "variadic" }
			| WHEN										{ $$ = "when" }
			| WHERE										{ $$ = "where" }
			| WINDOW										{ $$ = "window" }
			| WITH										{ $$ = "with" }
		;

/*
 * While all keywords can be used as column labels when preceded by AS,
 * not all of them can be used as a "bare" column label without AS.
 * Those that can be used as a bare label must be listed here,
 * in addition to appearing in one of the category lists above.
 *
 * Always add a new keyword to this list if possible.  Mark it BARE_LABEL
 * in kwlist.h if it is included here, or AS_LABEL if it is not.
 */
bare_label_keyword:
			  ABORT_P									{ $$ = "abort" }
			| ABSENT										{ $$ = "absent" }
			| ABSOLUTE_P									{ $$ = "absolute" }
			| ACCESS										{ $$ = "access" }
			| ACTION										{ $$ = "action" }
			| ADD_P										{ $$ = "add" }
			| ADMIN										{ $$ = "admin" }
			| AFTER										{ $$ = "after" }
			| AGGREGATE									{ $$ = "aggregate" }
			| ALL										{ $$ = "all" }
			| ALSO										{ $$ = "also" }
			| ALTER										{ $$ = "alter" }
			| ALWAYS										{ $$ = "always" }
			| ANALYSE										{ $$ = "analyse" }
			| ANALYZE										{ $$ = "analyze" }
			| AND										{ $$ = "and" }
			| ANY										{ $$ = "any" }
			| ASC										{ $$ = "asc" }
			| ASENSITIVE									{ $$ = "asensitive" }
			| ASSERTION									{ $$ = "assertion" }
			| ASSIGNMENT									{ $$ = "assignment" }
			| ASYMMETRIC									{ $$ = "asymmetric" }
			| AT										{ $$ = "at" }
			| ATOMIC										{ $$ = "atomic" }
			| ATTACH										{ $$ = "attach" }
			| ATTRIBUTE									{ $$ = "attribute" }
			| AUTHORIZATION								{ $$ = "authorization" }
			| BACKWARD									{ $$ = "backward" }
			| BEFORE										{ $$ = "before" }
			| BEGIN_P										{ $$ = "begin" }
			| BETWEEN										{ $$ = "between" }
			| BIGINT										{ $$ = "bigint" }
			| BINARY										{ $$ = "binary" }
			| BIT										{ $$ = "bit" }
			| BOOLEAN_P									{ $$ = "boolean" }
			| BOTH										{ $$ = "both" }
			| BREADTH										{ $$ = "breadth" }
			| BY										{ $$ = "by" }
			| CACHE										{ $$ = "cache" }
			| CALL										{ $$ = "call" }
			| CALLED										{ $$ = "called" }
			| CASCADE										{ $$ = "cascade" }
			| CASCADED									{ $$ = "cascaded" }
			| CASE										{ $$ = "case" }
			| CAST										{ $$ = "cast" }
			| CATALOG_P									{ $$ = "catalog" }
			| CHAIN										{ $$ = "chain" }
			| CHARACTERISTICS								{ $$ = "characteristics" }
			| CHECK										{ $$ = "check" }
			| CHECKPOINT									{ $$ = "checkpoint" }
			| CLASS										{ $$ = "class" }
			| CLOSE										{ $$ = "close" }
			| CLUSTER										{ $$ = "cluster" }
			| COALESCE									{ $$ = "coalesce" }
			| COLLATE										{ $$ = "collate" }
			| COLLATION									{ $$ = "collation" }
			| COLUMN										{ $$ = "column" }
			| COLUMNS										{ $$ = "columns" }
			| COMMENT										{ $$ = "comment" }
			| COMMENTS									{ $$ = "comments" }
			| COMMIT										{ $$ = "commit" }
			| COMMITTED									{ $$ = "committed" }
			| COMPRESSION									{ $$ = "compression" }
			| CONCURRENTLY								{ $$ = "concurrently" }
			| CONDITIONAL									{ $$ = "conditional" }
			| CONFIGURATION								{ $$ = "configuration" }
			| CONFLICT									{ $$ = "conflict" }
			| CONNECTION									{ $$ = "connection" }
			| CONSTRAINT									{ $$ = "constraint" }
			| CONSTRAINTS									{ $$ = "constraints" }
			| CONTENT_P									{ $$ = "content" }
			| CONTINUE_P									{ $$ = "continue" }
			| CONVERSION_P									{ $$ = "conversion" }
			| COPY										{ $$ = "copy" }
			| COST										{ $$ = "cost" }
			| CROSS										{ $$ = "cross" }
			| CSV										{ $$ = "csv" }
			| CUBE										{ $$ = "cube" }
			| CURRENT_P									{ $$ = "current" }
			| CURRENT_CATALOG									{ $$ = "current_catalog" }
			| CURRENT_DATE									{ $$ = "current_date" }
			| CURRENT_ROLE									{ $$ = "current_role" }
			| CURRENT_SCHEMA									{ $$ = "current_schema" }
			| CURRENT_TIME									{ $$ = "current_time" }
			| CURRENT_TIMESTAMP								{ $$ = "current_timestamp" }
			| CURRENT_USER									{ $$ = "current_user" }
			| CURSOR										{ $$ = "cursor" }
			| CYCLE										{ $$ = "cycle" }
			| DATA_P										{ $$ = "data" }
			| DATABASE									{ $$ = "database" }
			| DEALLOCATE									{ $$ = "deallocate" }
			| DEC										{ $$ = "dec" }
			| DECIMAL_P									{ $$ = "decimal" }
			| DECLARE										{ $$ = "declare" }
			| DEFAULT										{ $$ = "default" }
			| DEFAULTS									{ $$ = "defaults" }
			| DEFERRABLE									{ $$ = "deferrable" }
			| DEFERRED									{ $$ = "deferred" }
			| DEFINER										{ $$ = "definer" }
			| DELETE_P									{ $$ = "delete" }
			| DELIMITER									{ $$ = "delimiter" }
			| DELIMITERS									{ $$ = "delimiters" }
			| DEPENDS										{ $$ = "depends" }
			| DEPTH										{ $$ = "depth" }
			| DESC										{ $$ = "desc" }
			| DETACH										{ $$ = "detach" }
			| DICTIONARY									{ $$ = "dictionary" }
			| DISABLE_P									{ $$ = "disable" }
			| DISCARD										{ $$ = "discard" }
			| DISTINCT									{ $$ = "distinct" }
			| DO										{ $$ = "do" }
			| DOCUMENT_P									{ $$ = "document" }
			| DOMAIN_P									{ $$ = "domain" }
			| DOUBLE_P									{ $$ = "double" }
			| DROP										{ $$ = "drop" }
			| EACH										{ $$ = "each" }
			| ELSE										{ $$ = "else" }
			| EMPTY_P										{ $$ = "empty" }
			| ENABLE_P									{ $$ = "enable" }
			| ENCODING									{ $$ = "encoding" }
			| ENCRYPTED									{ $$ = "encrypted" }
			| END_P										{ $$ = "end" }
			| ENUM_P										{ $$ = "enum" }
			| ERROR_P										{ $$ = "error" }
			| ESCAPE										{ $$ = "escape" }
			| EVENT										{ $$ = "event" }
			| EXCLUDE										{ $$ = "exclude" }
			| EXCLUDING									{ $$ = "excluding" }
			| EXCLUSIVE									{ $$ = "exclusive" }
			| EXECUTE										{ $$ = "execute" }
			| EXISTS										{ $$ = "exists" }
			| EXPLAIN										{ $$ = "explain" }
			| EXPRESSION									{ $$ = "expression" }
			| EXTENSION									{ $$ = "extension" }
			| EXTERNAL									{ $$ = "external" }
			| EXTRACT										{ $$ = "extract" }
			| FALSE_P										{ $$ = "false" }
			| FAMILY										{ $$ = "family" }
			| FINALIZE									{ $$ = "finalize" }
			| FIRST_P										{ $$ = "first" }
			| FLOAT_P										{ $$ = "float" }
			| FOLLOWING									{ $$ = "following" }
			| FORCE										{ $$ = "force" }
			| FOREIGN										{ $$ = "foreign" }
			| FORMAT										{ $$ = "format" }
			| FORWARD										{ $$ = "forward" }
			| FREEZE										{ $$ = "freeze" }
			| FULL										{ $$ = "full" }
			| FUNCTION									{ $$ = "function" }
			| FUNCTIONS									{ $$ = "functions" }
			| GENERATED									{ $$ = "generated" }
			| GLOBAL										{ $$ = "global" }
			| GRANTED										{ $$ = "granted" }
			| GREATEST									{ $$ = "greatest" }
			| GROUPING									{ $$ = "grouping" }
			| GROUPS										{ $$ = "groups" }
			| HANDLER										{ $$ = "handler" }
			| HEADER_P									{ $$ = "header" }
			| HOLD										{ $$ = "hold" }
			| IDENTITY_P										{ $$ = "identity" }
			| IF_P										{ $$ = "if" }
			| ILIKE										{ $$ = "ilike" }
			| IMMEDIATE										{ $$ = "immediate" }
			| IMMUTABLE										{ $$ = "immutable" }
			| IMPLICIT_P										{ $$ = "implicit" }
			| IMPORT_P										{ $$ = "import" }
			| IN_P										{ $$ = "in" }
			| INCLUDE										{ $$ = "include" }
			| INCLUDING										{ $$ = "including" }
			| INCREMENT										{ $$ = "increment" }
			| INDENT										{ $$ = "indent" }
			| INDEX										{ $$ = "index" }
			| INDEXES										{ $$ = "indexes" }
			| INHERIT										{ $$ = "inherit" }
			| INHERITS										{ $$ = "inherits" }
			| INITIALLY										{ $$ = "initially" }
			| INLINE_P										{ $$ = "inline" }
			| INNER_P										{ $$ = "inner" }
			| INOUT										{ $$ = "inout" }
			| INPUT_P										{ $$ = "input" }
			| INSENSITIVE										{ $$ = "insensitive" }
			| INSERT										{ $$ = "insert" }
			| INSTEAD										{ $$ = "instead" }
			| INT_P										{ $$ = "int" }
			| INTEGER										{ $$ = "integer" }
			| INTERVAL										{ $$ = "interval" }
			| INVOKER										{ $$ = "invoker" }
			| IS										{ $$ = "is" }
			| ISOLATION										{ $$ = "isolation" }
			| JOIN										{ $$ = "join" }
			| JSON										{ $$ = "json" }
			| JSON_ARRAY										{ $$ = "json_array" }
			| JSON_ARRAYAGG										{ $$ = "json_arrayagg" }
			| JSON_EXISTS										{ $$ = "json_exists" }
			| JSON_OBJECT										{ $$ = "json_object" }
			| JSON_OBJECTAGG										{ $$ = "json_objectagg" }
			| JSON_QUERY										{ $$ = "json_query" }
			| JSON_SCALAR										{ $$ = "json_scalar" }
			| JSON_SERIALIZE										{ $$ = "json_serialize" }
			| JSON_TABLE										{ $$ = "json_table" }
			| JSON_VALUE										{ $$ = "json_value" }
			| KEEP										{ $$ = "keep" }
			| KEY										{ $$ = "key" }
			| KEYS										{ $$ = "keys" }
			| LABEL										{ $$ = "label" }
			| LANGUAGE										{ $$ = "language" }
			| LARGE_P										{ $$ = "large" }
			| LAST_P										{ $$ = "last" }
			| LATERAL_P										{ $$ = "lateral" }
			| LEADING										{ $$ = "leading" }
			| LEAKPROOF										{ $$ = "leakproof" }
			| LEAST										{ $$ = "least" }
			| LEFT										{ $$ = "left" }
			| LEVEL										{ $$ = "level" }
			| LIKE										{ $$ = "like" }
			| LISTEN										{ $$ = "listen" }
			| LOAD										{ $$ = "load" }
			| LOCAL										{ $$ = "local" }
			| LOCALTIME										{ $$ = "localtime" }
			| LOCALTIMESTAMP										{ $$ = "localtimestamp" }
			| LOCATION										{ $$ = "location" }
			| LOCK_P										{ $$ = "lock" }
			| LOCKED										{ $$ = "locked" }
			| LOGGED										{ $$ = "logged" }
			| MAPPING										{ $$ = "mapping" }
			| MATCH										{ $$ = "match" }
			| MATCHED										{ $$ = "matched" }
			| MATERIALIZED										{ $$ = "materialized" }
			| MAXVALUE										{ $$ = "maxvalue" }
			| MERGE										{ $$ = "merge" }
			| MERGE_ACTION										{ $$ = "merge_action" }
			| METHOD										{ $$ = "method" }
			| MINVALUE										{ $$ = "minvalue" }
			| MODE										{ $$ = "mode" }
			| MOVE										{ $$ = "move" }
			| NAME_P										{ $$ = "name" }
			| NAMES										{ $$ = "names" }
			| NATIONAL										{ $$ = "national" }
			| NATURAL										{ $$ = "natural" }
			| NCHAR										{ $$ = "nchar" }
			| NESTED										{ $$ = "nested" }
			| NEW										{ $$ = "new" }
			| NEXT										{ $$ = "next" }
			| NFC										{ $$ = "nfc" }
			| NFD										{ $$ = "nfd" }
			| NFKC										{ $$ = "nfkc" }
			| NFKD										{ $$ = "nfkd" }
			| NO										{ $$ = "no" }
			| NONE										{ $$ = "none" }
			| NORMALIZE										{ $$ = "normalize" }
			| NORMALIZED										{ $$ = "normalized" }
			| NOT										{ $$ = "not" }
			| NOTHING										{ $$ = "nothing" }
			| NOTIFY										{ $$ = "notify" }
			| NOWAIT										{ $$ = "nowait" }
			| NULL_P										{ $$ = "null" }
			| NULLIF										{ $$ = "nullif" }
			| NULLS_P										{ $$ = "nulls" }
			| NUMERIC										{ $$ = "numeric" }
			| OBJECT_P										{ $$ = "object" }
			| OF										{ $$ = "of" }
			| OFF										{ $$ = "off" }
			| OIDS										{ $$ = "oids" }
			| OLD										{ $$ = "old" }
			| OMIT										{ $$ = "omit" }
			| ONLY										{ $$ = "only" }
			| OPERATOR										{ $$ = "operator" }
			| OPTION										{ $$ = "option" }
			| OPTIONS										{ $$ = "options" }
			| OR										{ $$ = "or" }
			| ORDINALITY										{ $$ = "ordinality" }
			| OTHERS										{ $$ = "others" }
			| OUT_P										{ $$ = "out" }
			| OUTER_P										{ $$ = "outer" }
			| OVERLAY										{ $$ = "overlay" }
			| OVERRIDING										{ $$ = "overriding" }
			| OWNED										{ $$ = "owned" }
			| OWNER										{ $$ = "owner" }
			| PARALLEL										{ $$ = "parallel" }
			| PARAMETER										{ $$ = "parameter" }
			| PARSER										{ $$ = "parser" }
			| PARTIAL										{ $$ = "partial" }
			| PARTITION										{ $$ = "partition" }
			| PASSING										{ $$ = "passing" }
			| PASSWORD										{ $$ = "password" }
			| PATH										{ $$ = "path" }
			| PLACING										{ $$ = "placing" }
			| PLAN										{ $$ = "plan" }
			| PLANS										{ $$ = "plans" }
			| POLICY										{ $$ = "policy" }
			| POSITION										{ $$ = "position" }
			| PRECEDING										{ $$ = "preceding" }
			| PREPARE										{ $$ = "prepare" }
			| PREPARED										{ $$ = "prepared" }
			| PRESERVE										{ $$ = "preserve" }
			| PRIMARY										{ $$ = "primary" }
			| PRIOR										{ $$ = "prior" }
			| PRIVILEGES										{ $$ = "privileges" }
			| PROCEDURAL										{ $$ = "procedural" }
			| PROCEDURE										{ $$ = "procedure" }
			| PROCEDURES										{ $$ = "procedures" }
			| PROGRAM										{ $$ = "program" }
			| PUBLICATION										{ $$ = "publication" }
			| QUOTE										{ $$ = "quote" }
			| QUOTES										{ $$ = "quotes" }
			| RANGE										{ $$ = "range" }
			| READ										{ $$ = "read" }
			| REAL										{ $$ = "real" }
			| REASSIGN										{ $$ = "reassign" }
			| RECHECK										{ $$ = "recheck" }
			| RECURSIVE										{ $$ = "recursive" }
			| REF_P										{ $$ = "ref" }
			| REFERENCES										{ $$ = "references" }
			| REFERENCING										{ $$ = "referencing" }
			| REFRESH										{ $$ = "refresh" }
			| REINDEX										{ $$ = "reindex" }
			| RELATIVE_P										{ $$ = "relative" }
			| RELEASE										{ $$ = "release" }
			| RENAME										{ $$ = "rename" }
			| REPEATABLE										{ $$ = "repeatable" }
			| REPLACE										{ $$ = "replace" }
			| REPLICA										{ $$ = "replica" }
			| RESET										{ $$ = "reset" }
			| RESTART										{ $$ = "restart" }
			| RESTRICT										{ $$ = "restrict" }
			| RETURN										{ $$ = "return" }
			| RETURNS										{ $$ = "returns" }
			| REVOKE										{ $$ = "revoke" }
			| RIGHT										{ $$ = "right" }
			| ROLE										{ $$ = "role" }
			| ROLLBACK										{ $$ = "rollback" }
			| ROLLUP										{ $$ = "rollup" }
			| ROUTINE										{ $$ = "routine" }
			| ROUTINES										{ $$ = "routines" }
			| ROW										{ $$ = "row" }
			| ROWS										{ $$ = "rows" }
			| RULE										{ $$ = "rule" }
			| SAVEPOINT										{ $$ = "savepoint" }
			| SCALAR										{ $$ = "scalar" }
			| SCHEMA										{ $$ = "schema" }
			| SCHEMAS										{ $$ = "schemas" }
			| SCROLL										{ $$ = "scroll" }
			| SEARCH										{ $$ = "search" }
			| SECURITY										{ $$ = "security" }
			| SELECT										{ $$ = "select" }
			| SEQUENCE										{ $$ = "sequence" }
			| SEQUENCES										{ $$ = "sequences" }
			| SERIALIZABLE										{ $$ = "serializable" }
			| SERVER										{ $$ = "server" }
			| SESSION										{ $$ = "session" }
			| SESSION_USER										{ $$ = "session_user" }
			| SET										{ $$ = "set" }
			| SETOF										{ $$ = "setof" }
			| SETS										{ $$ = "sets" }
			| SHARE										{ $$ = "share" }
			| SHOW										{ $$ = "show" }
			| SIMILAR										{ $$ = "similar" }
			| SIMPLE										{ $$ = "simple" }
			| SKIP										{ $$ = "skip" }
			| SMALLINT										{ $$ = "smallint" }
			| SNAPSHOT										{ $$ = "snapshot" }
			| SOME										{ $$ = "some" }
			| SOURCE										{ $$ = "source" }
			| SQL_P										{ $$ = "sql" }
			| STABLE										{ $$ = "stable" }
			| STANDALONE_P										{ $$ = "standalone" }
			| START										{ $$ = "start" }
			| STATEMENT										{ $$ = "statement" }
			| STATISTICS										{ $$ = "statistics" }
			| STDIN										{ $$ = "stdin" }
			| STDOUT										{ $$ = "stdout" }
			| STORAGE										{ $$ = "storage" }
			| STORED										{ $$ = "stored" }
			| STRICT_P										{ $$ = "strict" }
			| STRING_P										{ $$ = "string" }
			| STRIP_P										{ $$ = "strip" }
			| SUBSCRIPTION										{ $$ = "subscription" }
			| SUBSTRING										{ $$ = "substring" }
			| SUPPORT										{ $$ = "support" }
			| SYMMETRIC										{ $$ = "symmetric" }
			| SYSID										{ $$ = "sysid" }
			| SYSTEM_P										{ $$ = "system" }
			| SYSTEM_USER										{ $$ = "system_user" }
			| TABLE										{ $$ = "table" }
			| TABLES										{ $$ = "tables" }
			| TABLESAMPLE										{ $$ = "tablesample" }
			| TABLESPACE										{ $$ = "tablespace" }
			| TARGET										{ $$ = "target" }
			| TEMP										{ $$ = "temp" }
			| TEMPLATE										{ $$ = "template" }
			| TEMPORARY										{ $$ = "temporary" }
			| TEXT_P										{ $$ = "text" }
			| THEN										{ $$ = "then" }
			| TIES										{ $$ = "ties" }
			| TIME										{ $$ = "time" }
			| TIMESTAMP										{ $$ = "timestamp" }
			| TRAILING										{ $$ = "trailing" }
			| TRANSACTION										{ $$ = "transaction" }
			| TRANSFORM										{ $$ = "transform" }
			| TREAT										{ $$ = "treat" }
			| TRIGGER										{ $$ = "trigger" }
			| TRIM										{ $$ = "trim" }
			| TRUE_P										{ $$ = "true" }
			| TRUNCATE										{ $$ = "truncate" }
			| TRUSTED										{ $$ = "trusted" }
			| TYPE_P										{ $$ = "type" }
			| TYPES_P										{ $$ = "types" }
			| UESCAPE										{ $$ = "uescape" }
			| UNBOUNDED										{ $$ = "unbounded" }
			| UNCOMMITTED										{ $$ = "uncommitted" }
			| UNCONDITIONAL										{ $$ = "unconditional" }
			| UNENCRYPTED										{ $$ = "unencrypted" }
			| UNIQUE										{ $$ = "unique" }
			| UNKNOWN										{ $$ = "unknown" }
			| UNLISTEN										{ $$ = "unlisten" }
			| UNLOGGED										{ $$ = "unlogged" }
			| UNTIL										{ $$ = "until" }
			| UPDATE										{ $$ = "update" }
			| USER										{ $$ = "user" }
			| USING										{ $$ = "using" }
			| VACUUM										{ $$ = "vacuum" }
			| VALID										{ $$ = "valid" }
			| VALIDATE										{ $$ = "validate" }
			| VALIDATOR										{ $$ = "validator" }
			| VALUE_P										{ $$ = "value" }
			| VALUES										{ $$ = "values" }
			| VARCHAR										{ $$ = "varchar" }
			| VARIADIC										{ $$ = "variadic" }
			| VERBOSE										{ $$ = "verbose" }
			| VERSION_P										{ $$ = "version" }
			| VIEW										{ $$ = "view" }
			| VIEWS										{ $$ = "views" }
			| VOLATILE										{ $$ = "volatile" }
			| WHEN										{ $$ = "when" }
			| WHITESPACE_P										{ $$ = "whitespace" }
			| WORK										{ $$ = "work" }
			| WRAPPER										{ $$ = "wrapper" }
			| WRITE										{ $$ = "write" }
			| XML_P										{ $$ = "xml" }
			| XMLATTRIBUTES										{ $$ = "xmlattributes" }
			| XMLCONCAT										{ $$ = "xmlconcat" }
			| XMLELEMENT										{ $$ = "xmlelement" }
			| XMLEXISTS										{ $$ = "xmlexists" }
			| XMLFOREST										{ $$ = "xmlforest" }
			| XMLNAMESPACES										{ $$ = "xmlnamespaces" }
			| XMLPARSE										{ $$ = "xmlparse" }
			| XMLPI										{ $$ = "xmlpi" }
			| XMLROOT										{ $$ = "xmlroot" }
			| XMLSERIALIZE										{ $$ = "xmlserialize" }
			| XMLTABLE										{ $$ = "xmltable" }
			| YES_P										{ $$ = "yes" }
			| ZONE										{ $$ = "zone" }
		;

/*
 * Basic Expression Grammar (~40 rules)
 * Ported from postgres/src/backend/parser/gram.y:14773+
 */

/*
 * Expression hierarchy: a_expr -> b_expr -> c_expr
 * a_expr: Most general expressions (comparisons, operators, etc.)
 * b_expr: More restricted (no comparisons)
 * c_expr: Most restricted (constants, column refs, function calls)
 */

a_expr:		c_expr								{ $$ = $1 }
		|	a_expr TYPECAST Typename
			{
				$$ = ast.NewTypeCast($1, $3, 0)
			}
		|	a_expr COLLATE any_name
			{
				collateClause := ast.NewCollateClause($3)
				collateClause.Arg = $1
				$$ = collateClause
			}
		|	a_expr AT TIME ZONE a_expr %prec AT
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("timezone"))
				$$ = ast.NewFuncCall(funcName, ast.NewNodeList($5, $1), 0)
			}
		|	a_expr AT LOCAL %prec AT
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("timezone"))
				$$ = ast.NewFuncCall(funcName, ast.NewNodeList($1), 0)
			}
		|	'+' a_expr %prec UMINUS
			{
				name := ast.NewNodeList(ast.NewString("+"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, nil, $2, 0)
			}
		|	'-' a_expr %prec UMINUS
			{
				name := ast.NewNodeList(ast.NewString("-"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, nil, $2, 0)
			}
		|	a_expr '+' a_expr
			{
				name := ast.NewNodeList(ast.NewString("+"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '-' a_expr
			{
				name := ast.NewNodeList(ast.NewString("-"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '*' a_expr
			{
				name := ast.NewNodeList(ast.NewString("*"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '/' a_expr
			{
				name := ast.NewNodeList(ast.NewString("/"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '%' a_expr
			{
				name := ast.NewNodeList(ast.NewString("%"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '^' a_expr
			{
				name := ast.NewNodeList(ast.NewString("^"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '<' a_expr
			{
				name := ast.NewNodeList(ast.NewString("<"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '>' a_expr
			{
				name := ast.NewNodeList(ast.NewString(">"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '=' a_expr
			{
				name := ast.NewNodeList(ast.NewString("="))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr LESS_EQUALS a_expr
			{
				name := ast.NewNodeList(ast.NewString("<="))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr GREATER_EQUALS a_expr
			{
				name := ast.NewNodeList(ast.NewString(">="))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr NOT_EQUALS a_expr
			{
				name := ast.NewNodeList(ast.NewString("<>"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr qual_Op a_expr %prec Op
			{
				$$ = ast.NewA_Expr(ast.AEXPR_OP, $2, $1, $3, 0)
			}
		|	qual_Op a_expr %prec Op
			{
				$$ = ast.NewA_Expr(ast.AEXPR_OP, $1, nil, $2, 0)
			}
		|	a_expr AND a_expr
			{
				$$ = ast.NewBoolExpr(ast.AND_EXPR, ast.NewNodeList($1, $3))
			}
		|	a_expr OR a_expr
			{
				$$ = ast.NewBoolExpr(ast.OR_EXPR, ast.NewNodeList($1, $3))
			}
		|	NOT a_expr
			{
				$$ = ast.NewBoolExpr(ast.NOT_EXPR, ast.NewNodeList($2))
			}
		|	NOT_LA a_expr %prec NOT
			{
				$$ = ast.NewBoolExpr(ast.NOT_EXPR, ast.NewNodeList($2))
			}
		|	a_expr LIKE a_expr
			{
				name := ast.NewNodeList(ast.NewString("~~"))
				$$ = ast.NewA_Expr(ast.AEXPR_LIKE, name, $1, $3, 0)
			}
		|	a_expr LIKE a_expr ESCAPE a_expr %prec LIKE
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("like_escape"))
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($3, $5), 0)
				name := ast.NewNodeList(ast.NewString("~~"))
				$$ = ast.NewA_Expr(ast.AEXPR_LIKE, name, $1, escapeFunc, 0)
			}
		|	a_expr NOT_LA LIKE a_expr %prec NOT_LA
			{
				name := ast.NewNodeList(ast.NewString("!~~"))
				$$ = ast.NewA_Expr(ast.AEXPR_LIKE, name, $1, $4, 0)
			}
		|	a_expr NOT_LA LIKE a_expr ESCAPE a_expr %prec NOT_LA
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("like_escape"))
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($4, $6), 0)
				name := ast.NewNodeList(ast.NewString("!~~"))
				$$ = ast.NewA_Expr(ast.AEXPR_LIKE, name, $1, escapeFunc, 0)
			}
		|	a_expr ILIKE a_expr
			{
				name := ast.NewNodeList(ast.NewString("~~*"))
				$$ = ast.NewA_Expr(ast.AEXPR_ILIKE, name, $1, $3, 0)
			}
		|	a_expr ILIKE a_expr ESCAPE a_expr %prec ILIKE
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("like_escape"))
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($3, $5), 0)
				name := ast.NewNodeList(ast.NewString("~~*"))
				$$ = ast.NewA_Expr(ast.AEXPR_ILIKE, name, $1, escapeFunc, 0)
			}
		|	a_expr NOT_LA ILIKE a_expr %prec NOT_LA
			{
				name := ast.NewNodeList(ast.NewString("!~~*"))
				$$ = ast.NewA_Expr(ast.AEXPR_ILIKE, name, $1, $4, 0)
			}
		|	a_expr NOT_LA ILIKE a_expr ESCAPE a_expr %prec NOT_LA
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("like_escape"))
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($4, $6), 0)
				name := ast.NewNodeList(ast.NewString("!~~*"))
				$$ = ast.NewA_Expr(ast.AEXPR_ILIKE, name, $1, escapeFunc, 0)
			}
		|	a_expr SIMILAR TO a_expr %prec SIMILAR
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("similar_to_escape"))
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($4), 0)
				name := ast.NewNodeList(ast.NewString("~"))
				$$ = ast.NewA_Expr(ast.AEXPR_SIMILAR, name, $1, escapeFunc, 0)
			}
		|	a_expr SIMILAR TO a_expr ESCAPE a_expr %prec SIMILAR
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("similar_to_escape"))
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($4, $6), 0)
				name := ast.NewNodeList(ast.NewString("~"))
				$$ = ast.NewA_Expr(ast.AEXPR_SIMILAR, name, $1, escapeFunc, 0)
			}
		|	a_expr NOT_LA SIMILAR TO a_expr %prec NOT_LA
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("similar_to_escape"))
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($5), 0)
				name := ast.NewNodeList(ast.NewString("!~"))
				$$ = ast.NewA_Expr(ast.AEXPR_SIMILAR, name, $1, escapeFunc, 0)
			}
		|	a_expr NOT_LA SIMILAR TO a_expr ESCAPE a_expr %prec NOT_LA
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("similar_to_escape"))
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($5, $7), 0)
				name := ast.NewNodeList(ast.NewString("!~"))
				$$ = ast.NewA_Expr(ast.AEXPR_SIMILAR, name, $1, escapeFunc, 0)
			}
		|	a_expr IS NULL_P %prec IS
			{
				$$ = ast.NewNullTest($1.(ast.Expression), ast.IS_NULL)
			}
		|	a_expr ISNULL
			{
				$$ = ast.NewNullTest($1.(ast.Expression), ast.IS_NULL)
			}
		|	a_expr IS NOT NULL_P %prec IS
			{
				$$ = ast.NewNullTest($1.(ast.Expression), ast.IS_NOT_NULL)
			}
		|	a_expr NOTNULL
			{
				$$ = ast.NewNullTest($1.(ast.Expression), ast.IS_NOT_NULL)
			}
		|	row OVERLAPS row
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("overlaps"))
				leftList := $1
				rightList := $3
				combinedList := ast.NewNodeList()
				combinedList.Items = append(leftList.Items, rightList.Items...)
				$$ = ast.NewFuncCall(funcName, combinedList, 0)
			}
		|	a_expr IS TRUE_P %prec IS
			{
				$$ = ast.NewBooleanTest($1.(ast.Expression), ast.IS_TRUE)
			}
		|	a_expr IS NOT TRUE_P %prec IS
			{
				$$ = ast.NewBooleanTest($1.(ast.Expression), ast.IS_NOT_TRUE)
			}
		|	a_expr IS FALSE_P %prec IS
			{
				$$ = ast.NewBooleanTest($1.(ast.Expression), ast.IS_FALSE)
			}
		|	a_expr IS NOT FALSE_P %prec IS
			{
				$$ = ast.NewBooleanTest($1.(ast.Expression), ast.IS_NOT_FALSE)
			}
		|	a_expr IS UNKNOWN %prec IS
			{
				$$ = ast.NewBooleanTest($1.(ast.Expression), ast.IS_UNKNOWN)
			}
		|	a_expr IS NOT UNKNOWN %prec IS
			{
				$$ = ast.NewBooleanTest($1.(ast.Expression), ast.IS_NOT_UNKNOWN)
			}
		|	a_expr IS DISTINCT FROM a_expr %prec IS
			{
				name := ast.NewNodeList(ast.NewString("="))
				$$ = ast.NewA_Expr(ast.AEXPR_DISTINCT, name, $1, $5, 0)
			}
		|	a_expr IS NOT DISTINCT FROM a_expr %prec IS
			{
				name := ast.NewNodeList(ast.NewString("="))
				$$ = ast.NewA_Expr(ast.AEXPR_NOT_DISTINCT, name, $1, $6, 0)
			}
		|	a_expr BETWEEN opt_asymmetric b_expr AND a_expr %prec BETWEEN
			{
				name := ast.NewNodeList(ast.NewString("BETWEEN"))
				$$ = ast.NewA_Expr(ast.AEXPR_BETWEEN, name, $1, ast.NewNodeList($4, $6), 0)
			}
		|	a_expr NOT_LA BETWEEN opt_asymmetric b_expr AND a_expr %prec NOT_LA
			{
				name := ast.NewNodeList(ast.NewString("NOT BETWEEN"))
				$$ = ast.NewA_Expr(ast.AEXPR_NOT_BETWEEN, name, $1, ast.NewNodeList($5, $7), 0)
			}
		|	a_expr BETWEEN SYMMETRIC b_expr AND a_expr %prec BETWEEN
			{
				name := ast.NewNodeList(ast.NewString("BETWEEN SYMMETRIC"))
				$$ = ast.NewA_Expr(ast.AEXPR_BETWEEN_SYM, name, $1, ast.NewNodeList($4, $6), 0)
			}
		|	a_expr NOT_LA BETWEEN SYMMETRIC b_expr AND a_expr %prec NOT_LA
			{
				name := ast.NewNodeList(ast.NewString("NOT BETWEEN SYMMETRIC"))
				$$ = ast.NewA_Expr(ast.AEXPR_NOT_BETWEEN_SYM, name, $1, ast.NewNodeList($5, $7), 0)
			}
		|	a_expr IN_P in_expr
			{
				// in_expr returns a SubLink or a list of a_exprs
				if sublink, ok := $3.(*ast.SubLink); ok {
					// generate foo = ANY (subquery)
					sublink.SubLinkType = ast.ANY_SUBLINK
					sublink.SubLinkId = 0
					sublink.Testexpr = $1
					sublink.OperName = nil  // show it's IN not = ANY
					$$ = sublink
				} else {
					// generate scalar IN expression
					name := ast.NewNodeList(ast.NewString("="))
					$$ = ast.NewA_Expr(ast.AEXPR_IN, name, $1, $3, 0)
				}
			}
		|	a_expr NOT_LA IN_P in_expr %prec NOT_LA
			{
				// in_expr returns a SubLink or a list of a_exprs
				if sublink, ok := $4.(*ast.SubLink); ok {
					// generate NOT (foo = ANY (subquery))
					// Make an = ANY node
					sublink.SubLinkType = ast.ANY_SUBLINK
					sublink.SubLinkId = 0
					sublink.Testexpr = $1
					sublink.OperName = nil  // show it's IN not = ANY
					// Stick a NOT on top
					$$ = ast.NewBoolExpr(ast.NOT_EXPR, ast.NewNodeList(sublink))
				} else {
					// generate scalar NOT IN expression
					name := ast.NewNodeList(ast.NewString("<>"))
					$$ = ast.NewA_Expr(ast.AEXPR_IN, name, $1, $4, 0)
				}
			}
		|	a_expr subquery_Op sub_type select_with_parens %prec Op
			{
				subLinkType := ast.SubLinkType($3)
				operName := $2
				sublink := ast.NewSubLink(subLinkType, $4)
				sublink.Testexpr = $1
				sublink.OperName = operName
				$$ = sublink
			}
		|	a_expr subquery_Op sub_type '(' a_expr ')' %prec Op
			{
				subLinkType := ast.SubLinkType($3)
				operName := $2
				if subLinkType == ast.ANY_SUBLINK {
					$$ = ast.NewA_Expr(ast.AEXPR_OP_ANY, operName, $1, $5, 0)
				} else {
					$$ = ast.NewA_Expr(ast.AEXPR_OP_ALL, operName, $1, $5, 0)
				}
			}
		| UNIQUE opt_unique_null_treatment select_with_parens
			{
				yylex.Error("UNIQUE predicate is not yet implemented")
			}
		|	a_expr IS DOCUMENT_P %prec IS
			{
				args := ast.NewNodeList($1.(ast.Expression))
				$$ = ast.NewXmlExpr(ast.IS_DOCUMENT, "", nil, nil, args, ast.XMLOPTION_DOCUMENT, false, 0, 0, 0)
			}
		|	a_expr IS NOT DOCUMENT_P %prec IS
			{
				args := ast.NewNodeList($1.(ast.Expression))
				xmlExpr := ast.NewXmlExpr(ast.IS_DOCUMENT, "", nil, nil, args, ast.XMLOPTION_DOCUMENT, false, 0, 0, 0)
				$$ = ast.NewBoolExpr(ast.NOT_EXPR, ast.NewNodeList(xmlExpr))
			}
		|	a_expr IS NORMALIZED %prec IS
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("is_normalized"))
				args := ast.NewNodeList($1)
				$$ = ast.NewFuncCall(funcName, args, 0)
			}
		|	a_expr IS unicode_normal_form NORMALIZED %prec IS
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("is_normalized"))
				normalFormConst := ast.NewA_Const(ast.NewString($3), 0)
				args := ast.NewNodeList($1, normalFormConst)
				$$ = ast.NewFuncCall(funcName, args, 0)
			}
		|	a_expr IS NOT NORMALIZED %prec IS
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("is_normalized"))
				args := ast.NewNodeList($1)
				isNormFunc := ast.NewFuncCall(funcName, args, 0)
				$$ = ast.NewBoolExpr(ast.NOT_EXPR, ast.NewNodeList(isNormFunc))
			}
		|	a_expr IS NOT unicode_normal_form NORMALIZED %prec IS
			{
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("is_normalized"))
				normalFormConst := ast.NewA_Const(ast.NewString($4), 0)
				args := ast.NewNodeList($1, normalFormConst)
				isNormFunc := ast.NewFuncCall(funcName, args, 0)
				$$ = ast.NewBoolExpr(ast.NOT_EXPR, ast.NewNodeList(isNormFunc))
			}
		|	a_expr IS json_predicate_type_constraint json_key_uniqueness_constraint_opt %prec IS
			{
				format := ast.NewJsonFormat(ast.JS_FORMAT_DEFAULT, ast.JS_ENC_DEFAULT, 0)
				itemType := ast.JsonValueType($3)
				uniqueKeys := $4
				$$ = ast.NewJsonIsPredicate($1, format, itemType, uniqueKeys, 0)
			}
		/*
		 * Required by SQL/JSON, but there are conflicts
		|	a_expr json_format_clause IS json_predicate_type_constraint json_key_uniqueness_constraint_opt %prec IS
			{
				format := $2.(*ast.JsonFormat)
				itemType := ast.JsonValueType($4)
				uniqueKeys := $5
				$$ = ast.NewJsonIsPredicate($1, format, itemType, uniqueKeys, 0)
			}
		*/
		|	a_expr IS NOT json_predicate_type_constraint json_key_uniqueness_constraint_opt %prec IS
			{
				format := ast.NewJsonFormat(ast.JS_FORMAT_DEFAULT, ast.JS_ENC_DEFAULT, 0)
				itemType := ast.JsonValueType($4)
				uniqueKeys := $5
				jsonPredicate := ast.NewJsonIsPredicate($1, format, itemType, uniqueKeys, 0)
				$$ = ast.NewBoolExpr(ast.NOT_EXPR, ast.NewNodeList(jsonPredicate))
			}
		/*
		 * Required by SQL/JSON, but there are conflicts
		|	a_expr json_format_clause IS NOT json_predicate_type_constraint json_key_uniqueness_constraint_opt %prec IS
			{
				format := $2.(*ast.JsonFormat)
				itemType := ast.JsonValueType($5)
				uniqueKeys := $6
				jsonPredicate := ast.NewJsonIsPredicate($1, format, itemType, uniqueKeys, 0)
				$$ = ast.NewBoolExpr(ast.NOT_EXPR, ast.NewNodeList(jsonPredicate))
			}
		*/
		|	DEFAULT
			{
				$$ = ast.NewSetToDefault(0, 0, 0)
			}
		;

b_expr:		c_expr								{ $$ = $1 }
		|	b_expr TYPECAST Typename
			{
				$$ = ast.NewTypeCast($1, $3, 0)
			}
		|	'+' b_expr %prec UMINUS
			{
				name := ast.NewNodeList(ast.NewString("+"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, nil, $2, 0)
			}
		|	'-' b_expr %prec UMINUS
			{
				$$ = doNegate($2, 0)
			}
		|	b_expr '+' b_expr
			{
				name := ast.NewNodeList(ast.NewString("+"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '-' b_expr
			{
				name := ast.NewNodeList(ast.NewString("-"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '*' b_expr
			{
				name := ast.NewNodeList(ast.NewString("*"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '/' b_expr
			{
				name := ast.NewNodeList(ast.NewString("/"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '%' b_expr
			{
				name := ast.NewNodeList(ast.NewString("%"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '^' b_expr
			{
				name := ast.NewNodeList(ast.NewString("^"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '<' b_expr
			{
				name := ast.NewNodeList(ast.NewString("<"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '>' b_expr
			{
				name := ast.NewNodeList(ast.NewString(">"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '=' b_expr
			{
				name := ast.NewNodeList(ast.NewString("="))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr LESS_EQUALS b_expr
			{
				name := ast.NewNodeList(ast.NewString("<="))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr GREATER_EQUALS b_expr
			{
				name := ast.NewNodeList(ast.NewString(">="))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr NOT_EQUALS b_expr
			{
				name := ast.NewNodeList(ast.NewString("<>"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr qual_Op b_expr %prec Op
			{
				$$ = ast.NewA_Expr(ast.AEXPR_OP, $2, $1, $3, 0)
			}
		|	qual_Op b_expr %prec Op
			{
				$$ = ast.NewA_Expr(ast.AEXPR_OP, $1, nil, $2, 0)
			}
		|	b_expr IS DISTINCT FROM b_expr %prec IS
			{
				name := ast.NewNodeList(ast.NewString("="))
				$$ = ast.NewA_Expr(ast.AEXPR_DISTINCT, name, $1, $5, 0)
			}
		|	b_expr IS NOT DISTINCT FROM b_expr %prec IS
			{
				name := ast.NewNodeList(ast.NewString("="))
				$$ = ast.NewA_Expr(ast.AEXPR_NOT_DISTINCT, name, $1, $6, 0)
			}
		|	b_expr IS DOCUMENT_P %prec IS
			{
				args := ast.NewNodeList($1)
				$$ = ast.NewXmlExpr(ast.IS_DOCUMENT, "", nil, nil, args, ast.XMLOPTION_DOCUMENT, false, 0, -1, 0)
			}
		|	b_expr IS NOT DOCUMENT_P %prec IS
			{
				args := ast.NewNodeList($1)
				xmlExpr := ast.NewXmlExpr(ast.IS_DOCUMENT, "", nil, nil, args, ast.XMLOPTION_DOCUMENT, false, 0, -1, 0)
				$$ = ast.NewBoolExpr(ast.NOT_EXPR, ast.NewNodeList(xmlExpr))
			}
		;

c_expr:		columnref							{ $$ = $1 }
		|	AexprConst							{ $$ = $1 }
		|	PARAM opt_indirection
			{
				p := ast.NewParamRef($1, 0)
				if $2 != nil {
					$$ = ast.NewA_Indirection(p, $2, 0)
				} else {
					$$ = p
				}
			}
		|	'(' a_expr ')' opt_indirection
			{
				if $4 != nil {
					$$ = ast.NewA_Indirection(ast.NewParenExpr($2, 0), $4, 0)
				} else {
					$$ = ast.NewParenExpr($2,0)
				}
			}
		|	case_expr							{ $$ = $1 }
		|	func_expr							{ $$ = $1 }
		|	select_with_parens					%prec UMINUS
			{
				$$ = ast.NewExprSublink($1)
			}
		|	select_with_parens indirection
			{
				sublink := ast.NewExprSublink($1)
				$$ = ast.NewA_Indirection(sublink, $2, 0)
			}
		|	EXISTS select_with_parens
			{
				$$ = ast.NewExistsSublink($2)
			}
		|	ARRAY select_with_parens
			{
				$$ = ast.NewArraySublink($2)
			}
		|	ARRAY array_expr
			{
				$$ = $2
			}
		|	explicit_row
			{
				rowExpr := ast.NewRowConstructor($1.(*ast.NodeList))
				rowExpr.RowFormat = ast.COERCE_EXPLICIT_CALL
				$$ = rowExpr
			}
		|	implicit_row
			{
				rowExpr := ast.NewRowConstructor($1.(*ast.NodeList))
				rowExpr.RowFormat = ast.COERCE_IMPLICIT_CAST
				$$ = rowExpr
			}
		|	GROUPING '(' expr_list ')'
			{
				exprList := $3
				grouping := &ast.GroupingFunc{
					BaseExpr: ast.BaseExpr{BaseNode: ast.BaseNode{Tag: ast.T_GroupingFunc}},
					Args:     exprList,
				}
				$$ = grouping
			}
		;

/* Constants */

/* ConstTypename - matching PostgreSQL implementation structure
 * PostgreSQL reference: src/backend/parser/gram.y:14363-14369
 * Simplified to avoid reduce/reduce conflicts with existing type rules
 */
ConstTypename:
			Numeric									{ $$ = $1 }
		|	ConstBit								{ $$ = $1 }
		|	ConstCharacter							{ $$ = $1 }
		|	ConstDatetime							{ $$ = $1 }
		|	JsonType								{ $$ = $1 }
		;

AexprConst:	Iconst
			{
				$$ = ast.NewA_Const(ast.NewInteger($1), 0)
			}
		|	FCONST
			{
				$$ = ast.NewA_Const(ast.NewFloat($1), 0)
			}
		|	Sconst
			{
				$$ = ast.NewA_Const(ast.NewString($1), 0)
			}
		|	TRUE_P
			{
				$$ = ast.NewA_Const(ast.NewBoolean(true), 0)
			}
		|	FALSE_P
			{
				$$ = ast.NewA_Const(ast.NewBoolean(false), 0)
			}
		|	NULL_P
			{
				$$ = ast.NewA_ConstNull(0)
			}
		|	BCONST
			{
				$$ = ast.NewA_Const(ast.NewBitString($1), 0)
			}
		|	XCONST
			{
				// This is a bit constant per SQL99
				$$ = ast.NewA_Const(ast.NewBitString($1), 0)
			}
		|	func_name Sconst
			{
				// generic type 'literal' syntax
				typeName := makeTypeNameFromNodeList($1)
				stringConst := ast.NewA_Const(ast.NewString($2), 0)
				$$ = ast.NewTypeCast(stringConst, typeName, 0)
			}
		|	func_name '(' func_arg_list opt_sort_clause ')' Sconst
			{
				// generic syntax with a type modifier
				typeName := makeTypeNameFromNodeList($1)
				// For now, we'll skip the error checking for NamedArgExpr and ORDER BY
				// TODO: Add proper validation when needed
				typeName.Typmods = $3
				stringConst := ast.NewA_Const(ast.NewString($6), 0)
				$$ = ast.NewTypeCast(stringConst, typeName, 0)
			}
		|	ConstTypename Sconst
			{
				stringConst := ast.NewA_Const(ast.NewString($2), 0)
				$$ = ast.NewTypeCast(stringConst, $1, 0)
			}
		|	ConstInterval Sconst opt_interval
			{
				t := $1
				t.Typmods = $3
				stringConst := ast.NewA_Const(ast.NewString($2), 0)
				$$ = ast.NewTypeCast(stringConst, t, 0)
			}
		|	ConstInterval '(' Iconst ')' Sconst
			{
				t := $1
				// INTERVAL_FULL_RANGE equivalent and precision
				fullRange := ast.NewInteger(ast.INTERVAL_FULL_RANGE)
				precision := ast.NewInteger($3)
				t.Typmods = ast.NewNodeList(fullRange, precision)
				stringConst := ast.NewA_Const(ast.NewString($5), 0)
				$$ = ast.NewTypeCast(stringConst, t, 0)
			}
		;

Iconst:		ICONST								{ $$ = $1 }
		;

Sconst:		SCONST								{ $$ = $1 }
		;

SignedIconst:	Iconst							{ $$ = $1 }
		|	'+' Iconst							{ $$ = $2 }
		|	'-' Iconst							{ $$ = -$2 }
		;

ConstInterval:	INTERVAL
			{
				$$ = makeTypeNameFromNodeList(ast.NewNodeList(ast.NewString("interval")))
			}
		;

opt_interval:
			YEAR_P
				{ $$ = ast.NewNodeList(ast.NewInteger(ast.INTERVAL_MASK_YEAR)) }
		|	MONTH_P
				{ $$ = ast.NewNodeList(ast.NewInteger(ast.INTERVAL_MASK_MONTH)) }
		|	DAY_P
				{ $$ = ast.NewNodeList(ast.NewInteger(ast.INTERVAL_MASK_DAY)) }
		|	HOUR_P
				{ $$ = ast.NewNodeList(ast.NewInteger(ast.INTERVAL_MASK_HOUR)) }
		|	MINUTE_P
				{ $$ = ast.NewNodeList(ast.NewInteger(ast.INTERVAL_MASK_MINUTE)) }
		|	interval_second
				{ $$ = $1 }
		|	YEAR_P TO MONTH_P
				{
					$$ = ast.NewNodeList(ast.NewInteger(ast.INTERVAL_MASK_YEAR | ast.INTERVAL_MASK_MONTH))
				}
		|	DAY_P TO HOUR_P
				{
					$$ = ast.NewNodeList(ast.NewInteger(ast.INTERVAL_MASK_DAY | ast.INTERVAL_MASK_HOUR))
				}
		|	DAY_P TO MINUTE_P
				{
					$$ = ast.NewNodeList(ast.NewInteger(ast.INTERVAL_MASK_DAY | ast.INTERVAL_MASK_HOUR | ast.INTERVAL_MASK_MINUTE))
				}
		|	DAY_P TO interval_second
				{
					// Modify first element of interval_second result
					result := $3
					if len(result.Items) > 0 {
						if intNode, ok := result.Items[0].(*ast.Integer); ok {
							intNode.IVal = ast.INTERVAL_MASK_DAY | ast.INTERVAL_MASK_HOUR | ast.INTERVAL_MASK_MINUTE | ast.INTERVAL_MASK_SECOND
						}
					}
					$$ = result
				}
		|	HOUR_P TO MINUTE_P
				{
					$$ = ast.NewNodeList(ast.NewInteger(ast.INTERVAL_MASK_HOUR | ast.INTERVAL_MASK_MINUTE))
				}
		|	HOUR_P TO interval_second
				{
					// Modify first element of interval_second result
					result := $3
					if len(result.Items) > 0 {
						if intNode, ok := result.Items[0].(*ast.Integer); ok {
							intNode.IVal = ast.INTERVAL_MASK_HOUR | ast.INTERVAL_MASK_MINUTE | ast.INTERVAL_MASK_SECOND
						}
					}
					$$ = result
				}
		|	MINUTE_P TO interval_second
				{
					// Modify first element of interval_second result
					result := $3
					if len(result.Items) > 0 {
						if intNode, ok := result.Items[0].(*ast.Integer); ok {
							intNode.IVal = ast.INTERVAL_MASK_MINUTE | ast.INTERVAL_MASK_SECOND
						}
					}
					$$ = result
				}
		|	/* EMPTY */
				{ $$ = nil }
		;

interval_second:
			SECOND_P
			{
				$$ = ast.NewNodeList(ast.NewInteger(ast.INTERVAL_MASK_SECOND))
			}
		|	SECOND_P '(' Iconst ')'
			{
				$$ = ast.NewNodeList(
					ast.NewInteger(ast.INTERVAL_MASK_SECOND),
					ast.NewInteger($3),      // precision
				)
			}
		;

/* Column references */
columnref:	ColId
			{
				$$ = ast.NewColumnRef(ast.NewString($1))
			}
		|	ColId indirection
			{
				// Create all fields: first ColId followed by indirection items
				allFields := []ast.Node{ast.NewString($1)}
				allFields = append(allFields, $2.Items...)
				$$ = ast.NewColumnRef(allFields...)
			}
		;

/* Indirection (array subscripts, field access) */
indirection: indirection_el
			{
				$$ = ast.NewNodeList($1)
			}
		|	indirection indirection_el
			{
				$1.Append($2)
				$$ = $1
			}
		;

opt_slice_bound: a_expr							{ $$ = $1 }
		|	/* EMPTY */							{ $$ = nil }
		;

indirection_el: '.' attr_name
			{
				$$ = ast.NewString($2)
			}
		|	'.' '*'
			{
				$$ = &ast.A_Star{BaseNode: ast.BaseNode{Tag: ast.T_A_Star}}
			}
		|	'[' a_expr ']'
			{
				$$ = ast.NewA_Indices($2, 0)
			}
		|	'[' opt_slice_bound ':' opt_slice_bound ']'
			{
				$$ = ast.NewA_IndicesSlice($2, $4, 0)
			}
		;

opt_indirection: /* EMPTY */						{ $$ = nil }
		|	opt_indirection indirection_el
			{
				if $1 == nil {
					$$ = ast.NewNodeList($2)
				} else {
					$1.Append($2)
					$$ = $1
				}
			}
		;

/* Function calls */
func_expr:	func_application within_group_clause filter_clause over_clause
			{
				funcCall := $1.(*ast.FuncCall)

				// Apply within_group_clause if present
				if $2 != nil {
					// WITHIN GROUP (ORDER BY ...) - store the sort list
					funcCall.AggOrder = $2
					funcCall.AggWithinGroup = true
				}

				// Apply filter_clause if present
				if $3 != nil {
					// FILTER (WHERE condition) - store the filter expression
					funcCall.AggFilter = $3
				}

				// Apply over_clause if present (window functions)
				if $4 != nil {
					funcCall.Over = $4
				}

				$$ = funcCall
			}
		|	json_aggregate_func filter_clause over_clause
			{
				jsonAgg := $1

				// Create or get the Constructor
				var constructor *ast.JsonAggConstructor

				// Handle the filter_clause and over_clause by setting them in the Constructor
				switch jsonFunc := jsonAgg.(type) {
				case *ast.JsonObjectAgg:
					if jsonFunc.Constructor == nil {
						jsonFunc.Constructor = ast.NewJsonAggConstructor(nil)
					}
					constructor = jsonFunc.Constructor
				case *ast.JsonArrayAgg:
					if jsonFunc.Constructor == nil {
						jsonFunc.Constructor = ast.NewJsonAggConstructor(nil)
					}
					constructor = jsonFunc.Constructor
				}

				// Set filter and over clauses outside the switch (DRY)
				if constructor != nil {
					if $2 != nil {
						constructor.AggFilter = $2.(ast.Node)
					}
					if $3 != nil {
						constructor.Over = $3
					}
				}

				$$ = jsonAgg
			}
		|	func_expr_common_subexpr
			{ $$ = $1 }
		;

/* Common expressions appearing in func_expr */
func_expr_common_subexpr:
			COLLATION FOR '(' a_expr ')'
			{
				// SystemFuncName("pg_collation_for")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("pg_collation_for"))
				funcCall := ast.NewFuncCall(funcName, ast.NewNodeList($4), 0)
				funcCall.Funcformat = ast.COERCE_SQL_SYNTAX
				$$ = funcCall
			}
		|	CURRENT_DATE
			{
				// makeSQLValueFunction(SVFOP_CURRENT_DATE, -1, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_CURRENT_DATE, 0, -1, 0)
			}
		|	CURRENT_TIME
			{
				// makeSQLValueFunction(SVFOP_CURRENT_TIME, -1, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_CURRENT_TIME, 0, -1, 0)
			}
		|	CURRENT_TIME '(' Iconst ')'
			{
				// makeSQLValueFunction(SVFOP_CURRENT_TIME_N, $3, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_CURRENT_TIME_N, 0, $3, 0)
			}
		|	CURRENT_TIMESTAMP
			{
				// makeSQLValueFunction(SVFOP_CURRENT_TIMESTAMP, -1, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_CURRENT_TIMESTAMP, 0, -1, 0)
			}
		|	CURRENT_TIMESTAMP '(' Iconst ')'
			{
				// makeSQLValueFunction(SVFOP_CURRENT_TIMESTAMP_N, $3, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_CURRENT_TIMESTAMP_N, 0, $3, 0)
			}
		|	LOCALTIME
			{
				// makeSQLValueFunction(SVFOP_LOCALTIME, -1, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_LOCALTIME, 0, -1, 0)
			}
		|	LOCALTIME '(' Iconst ')'
			{
				// makeSQLValueFunction(SVFOP_LOCALTIME_N, $3, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_LOCALTIME_N, 0, $3, 0)
			}
		|	LOCALTIMESTAMP
			{
				// makeSQLValueFunction(SVFOP_LOCALTIMESTAMP, -1, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_LOCALTIMESTAMP, 0, -1, 0)
			}
		|	LOCALTIMESTAMP '(' Iconst ')'
			{
				// makeSQLValueFunction(SVFOP_LOCALTIMESTAMP_N, $3, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_LOCALTIMESTAMP_N, 0, $3, 0)
			}
		|	CURRENT_ROLE
			{
				// makeSQLValueFunction(SVFOP_CURRENT_ROLE, -1, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_CURRENT_ROLE, 0, -1, 0)
			}
		|	CURRENT_USER
			{
				// makeSQLValueFunction(SVFOP_CURRENT_USER, -1, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_CURRENT_USER, 0, -1, 0)
			}
		|	SESSION_USER
			{
				// makeSQLValueFunction(SVFOP_SESSION_USER, -1, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_SESSION_USER, 0, -1, 0)
			}
		|	SYSTEM_USER
			{
				// SystemFuncName("system_user")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("system_user"))
				funcCall := ast.NewFuncCall(funcName, nil, 0)
				funcCall.Funcformat = ast.COERCE_SQL_SYNTAX
				$$ = funcCall
			}
		|	USER
			{
				// makeSQLValueFunction(SVFOP_USER, -1, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_USER, 0, -1, 0)
			}
		|	CURRENT_CATALOG
			{
				// makeSQLValueFunction(SVFOP_CURRENT_CATALOG, -1, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_CURRENT_CATALOG, 0, -1, 0)
			}
		|	CURRENT_SCHEMA
			{
				// makeSQLValueFunction(SVFOP_CURRENT_SCHEMA, -1, @1)
				$$ = ast.NewSQLValueFunction(ast.SVFOP_CURRENT_SCHEMA, 0, -1, 0)
			}
		|	CAST '(' a_expr AS Typename ')'
			{
				$$ = ast.NewTypeCast($3, $5, 0)
			}
		|	EXTRACT '(' extract_list ')'
			{
				// SystemFuncName("extract")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("extract"))
				funcCall := ast.NewFuncCall(funcName, $3, 0)
				funcCall.Funcformat = ast.COERCE_SQL_SYNTAX
				$$ = funcCall
			}
		|	NORMALIZE '(' a_expr ')'
			{
				// SystemFuncName("normalize")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("normalize"))
				funcCall := ast.NewFuncCall(funcName, ast.NewNodeList($3), 0)
				funcCall.Funcformat = ast.COERCE_SQL_SYNTAX
				$$ = funcCall
			}
		|	NORMALIZE '(' a_expr ',' unicode_normal_form ')'
			{
				// SystemFuncName("normalize")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("normalize"))
				args := ast.NewNodeList($3)
				args.Items = append(args.Items, ast.NewA_Const(ast.NewString($5), -1))
				funcCall := ast.NewFuncCall(funcName, args, 0)
				funcCall.Funcformat = ast.COERCE_SQL_SYNTAX
				$$ = funcCall
			}
		|	OVERLAY '(' overlay_list ')'
			{
				// SystemFuncName("overlay")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("overlay"))
				funcCall := ast.NewFuncCall(funcName, $3, 0)
				funcCall.Funcformat = ast.COERCE_SQL_SYNTAX
				$$ = funcCall
			}
		|	OVERLAY '(' func_arg_list_opt ')'
			{
				// SystemFuncName("overlay")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("overlay"))
				funcCall := ast.NewFuncCall(funcName, $3, 0)
				funcCall.Funcformat = ast.COERCE_EXPLICIT_CALL
				$$ = funcCall
			}
		|	POSITION '(' position_list ')'
			{
				// SystemFuncName("position")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("position"))
				funcCall := ast.NewFuncCall(funcName, $3, 0)
				funcCall.Funcformat = ast.COERCE_SQL_SYNTAX
				$$ = funcCall
			}
		|	SUBSTRING '(' substr_list ')'
			{
				// SystemFuncName("substring")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("substring"))
				funcCall := ast.NewFuncCall(funcName, $3, 0)
				funcCall.Funcformat = ast.COERCE_SQL_SYNTAX
				$$ = funcCall
			}
		|	SUBSTRING '(' func_arg_list_opt ')'
			{
				// SystemFuncName("substring")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("substring"))
				funcCall := ast.NewFuncCall(funcName, $3, 0)
				funcCall.Funcformat = ast.COERCE_EXPLICIT_CALL
				$$ = funcCall
			}
		|	TREAT '(' a_expr AS Typename ')'
			{
				// SystemFuncName("treat")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), llast($5.Names))
				args := ast.NewNodeList($3)
				funcCall := ast.NewFuncCall(funcName, args, 0)
				funcCall.Funcformat = ast.COERCE_EXPLICIT_CALL
				$$ = funcCall
			}
		|	TRIM '(' BOTH trim_list ')'
			{
				// SystemFuncName("btrim")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("btrim"))
				funcCall := ast.NewFuncCall(funcName, $4, 0)
				funcCall.Funcformat = ast.COERCE_SQL_SYNTAX
				$$ = funcCall
			}
		|	TRIM '(' LEADING trim_list ')'
			{
				// SystemFuncName("ltrim")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("ltrim"))
				funcCall := ast.NewFuncCall(funcName, $4, 0)
				funcCall.Funcformat = ast.COERCE_SQL_SYNTAX
				$$ = funcCall
			}
		|	TRIM '(' TRAILING trim_list ')'
			{
				// SystemFuncName("rtrim")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("rtrim"))
				funcCall := ast.NewFuncCall(funcName, $4, 0)
				funcCall.Funcformat = ast.COERCE_SQL_SYNTAX
				$$ = funcCall
			}
		|	TRIM '(' trim_list ')'
			{
				// SystemFuncName("btrim")
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("btrim"))
				funcCall := ast.NewFuncCall(funcName, $3, 0)
				funcCall.Funcformat = ast.COERCE_SQL_SYNTAX
				$$ = funcCall
			}
		|	NULLIF '(' a_expr ',' a_expr ')'
			{
				// makeSimpleA_Expr(AEXPR_NULLIF, "=", $3, $5, @1)
				operName := ast.NewNodeList(ast.NewString("="))
				$$ = ast.NewA_Expr(ast.AEXPR_NULLIF, operName, $3, $5, 0)
			}
		|	COALESCE '(' expr_list ')'
			{
				// CoalesceExpr *c = makeNode(CoalesceExpr); c->args = $3; c->location = @1
				$$ = ast.NewCoalesceExpr(0, $3)
			}
		|	GREATEST '(' expr_list ')'
			{
				// MinMaxExpr *v = makeNode(MinMaxExpr); v->args = $3; v->op = IS_GREATEST; v->location = @1
				$$ = ast.NewMinMaxExpr(0, 0, 0, ast.IS_GREATEST, $3, 0)
			}
		|	LEAST '(' expr_list ')'
			{
				// MinMaxExpr *v = makeNode(MinMaxExpr); v->args = $3; v->op = IS_LEAST; v->location = @1
				$$ = ast.NewMinMaxExpr(0, 0, 0, ast.IS_LEAST, $3, 0)
			}
		|	XMLCONCAT '(' expr_list ')'
			{
				// makeXmlExpr(IS_XMLCONCAT, NULL, NIL, $3, @1)
				$$ = ast.NewXmlExpr(ast.IS_XMLCONCAT, "", nil, nil, $3, ast.XMLOPTION_DOCUMENT, false, 0, -1, 0)
			}
		|	XMLELEMENT '(' NAME_P ColLabel ')'
			{
				// makeXmlExpr(IS_XMLELEMENT, $4, NIL, NIL, @1)
				$$ = ast.NewXmlExpr(ast.IS_XMLELEMENT, $4, nil, nil, nil, ast.XMLOPTION_DOCUMENT, false, 0, -1, 0)
			}
		|	XMLELEMENT '(' NAME_P ColLabel ',' xml_attributes ')'
			{
				// makeXmlExpr(IS_XMLELEMENT, $4, $6, NIL, @1)
				$$ = ast.NewXmlExpr(ast.IS_XMLELEMENT, $4, $6, nil, nil, ast.XMLOPTION_DOCUMENT, false, 0, -1, 0)
			}
		|	XMLELEMENT '(' NAME_P ColLabel ',' expr_list ')'
			{
				// makeXmlExpr(IS_XMLELEMENT, $4, NIL, $6, @1)
				$$ = ast.NewXmlExpr(ast.IS_XMLELEMENT, $4, nil, nil, $6, ast.XMLOPTION_DOCUMENT, false, 0, -1, 0)
			}
		|	XMLELEMENT '(' NAME_P ColLabel ',' xml_attributes ',' expr_list ')'
			{
				// makeXmlExpr(IS_XMLELEMENT, $4, $6, $8, @1)
				$$ = ast.NewXmlExpr(ast.IS_XMLELEMENT, $4, $6, nil, $8, ast.XMLOPTION_DOCUMENT, false, 0, -1, 0)
			}
		|	XMLFOREST '(' xml_attribute_list ')'
			{
				// makeXmlExpr(IS_XMLFOREST, NULL, $3, NIL, @1)
				$$ = ast.NewXmlExpr(ast.IS_XMLFOREST, "", $3, nil, nil, ast.XMLOPTION_DOCUMENT, false, 0, -1, 0)
			}
		|	XMLPARSE '(' document_or_content a_expr xml_whitespace_option ')'
			{
				// makeXmlExpr(IS_XMLPARSE, NULL, NIL, list_make2($4, makeBoolAConst($5, -1)), @1)
				// x->xmloption = $3
				wsOption := ast.NewA_Const(ast.NewBoolean($5), -1)
				args := ast.NewNodeList($4, wsOption)
				xmlOption := ast.XmlOptionType($3)
				$$ = ast.NewXmlExpr(ast.IS_XMLPARSE, "", nil, nil, args, xmlOption, false, 0, -1, 0)
			}
		|	XMLEXISTS '(' c_expr xmlexists_argument ')'
			{
				// SystemFuncName("xmlexists") - xmlexists(A PASSING [BY REF] B [BY REF]) is converted to xmlexists(A, B)
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("xmlexists"))
				args := ast.NewNodeList($3, $4)
				funcCall := ast.NewFuncCall(funcName, args, 0)
				funcCall.Funcformat = ast.COERCE_SQL_SYNTAX
				$$ = funcCall
			}
		|	XMLPI '(' NAME_P ColLabel ')'
			{
				// makeXmlExpr(IS_XMLPI, $4, NIL, NIL, @1)
				$$ = ast.NewXmlExpr(ast.IS_XMLPI, $4, nil, nil, nil, ast.XMLOPTION_DOCUMENT, false, 0, -1, 0)
			}
		|	XMLPI '(' NAME_P ColLabel ',' a_expr ')'
			{
				// makeXmlExpr(IS_XMLPI, $4, NIL, list_make1($6), @1)
				args := ast.NewNodeList($6)
				$$ = ast.NewXmlExpr(ast.IS_XMLPI, $4, nil, nil, args, ast.XMLOPTION_DOCUMENT, false, 0, -1, 0)
			}
		|	XMLROOT '(' a_expr ',' xml_root_version opt_xml_root_standalone ')'
			{
				// makeXmlExpr(IS_XMLROOT, NULL, NIL, list_make3(...), @1)
				args := ast.NewNodeList($3, $5, $6)
				$$ = ast.NewXmlExpr(ast.IS_XMLROOT, "", nil, nil, args, ast.XMLOPTION_DOCUMENT, false, 0, -1, 0)
			}
		|	JSON_OBJECT '(' func_arg_list ')'
			{
				/* Support for legacy (non-standard) json_object() */
				funcName := ast.NewNodeList(ast.NewString("json_object"))
				funcCall := ast.NewFuncCall(funcName, $3, 0)
				funcCall.Funcformat = ast.COERCE_EXPLICIT_CALL
				$$ = funcCall
			}
		|	JSON_OBJECT '(' json_name_and_value_list
			json_object_constructor_null_clause_opt
			json_key_uniqueness_constraint_opt
			json_returning_clause_opt ')'
			{
				n := ast.NewJsonObjectConstructor($3, $4, $5)
				if $6 != nil {
					n.Output = $6.(*ast.JsonOutput)
				}
				$$ = n
			}
		|	JSON_OBJECT '(' json_returning_clause_opt ')'
			{
				n := ast.NewJsonObjectConstructor(nil, false, false)
				if $3 != nil {
					n.Output = $3.(*ast.JsonOutput)
				}
				$$ = n
			}
		|	JSON_ARRAY '('
			json_value_expr_list
			json_array_constructor_null_clause_opt
			json_returning_clause_opt
		')'
			{
				n := ast.NewJsonArrayConstructor($3, $4)
				if $5 != nil {
					n.Output = $5.(*ast.JsonOutput)
				}
				$$ = n
			}
		|	JSON_ARRAY '('
			select_no_parens
			json_format_clause_opt
			json_returning_clause_opt
		')'
			{
				n := ast.NewJsonArrayQueryConstructor($3, true) /* XXX: absent_on_null = true */
				if $4 != nil {
					n.Format = $4.(*ast.JsonFormat)
				}
				if $5 != nil {
					n.Output = $5.(*ast.JsonOutput)
				}
				$$ = n
			}
		|	JSON_ARRAY '('
			json_returning_clause_opt
		')'
			{
				n := ast.NewJsonArrayConstructor(nil, true)
				if $3 != nil {
					n.Output = $3.(*ast.JsonOutput)
				}
				$$ = n
			}
		|	JSON '(' json_value_expr json_key_uniqueness_constraint_opt ')'
			{
				n := ast.NewJsonParseExpr($3.(*ast.JsonValueExpr), $4)
				n.Output = nil
				$$ = n
			}
		|	JSON_SCALAR '(' a_expr ')'
			{
				n := ast.NewJsonScalarExpr($3.(ast.Expr))
				n.Output = nil
				$$ = n
			}
		|	JSON_SERIALIZE '(' json_value_expr json_returning_clause_opt ')'
			{
				n := ast.NewJsonSerializeExpr($3.(*ast.JsonValueExpr))
				if $4 != nil {
					n.Output = $4.(*ast.JsonOutput)
				}
				$$ = n
			}
		|	MERGE_ACTION '(' ')'
			{
				m := ast.NewMergeSupportFunc(ast.TEXTOID, ast.InvalidOid, 0)
				$$ = m
			}
		|	JSON_QUERY '('
			json_value_expr ',' a_expr json_passing_clause_opt
			json_returning_clause_opt
			json_wrapper_behavior
			json_quotes_clause_opt
			json_behavior_clause_opt
		')'
			{
				n := ast.NewJsonFuncExpr(ast.JSON_QUERY_OP, $3.(*ast.JsonValueExpr), $5)
				if $6 != nil {
					n.Passing = $6.(*ast.NodeList)
				}
				if $7 != nil {
					n.Output = $7.(*ast.JsonOutput)
				}
				n.Wrapper = ast.JsonWrapper($8)
				n.Quotes = ast.JsonQuotes($9)
				if $10 != nil {
					behaviors := $10.(*ast.NodeList)
					if linitial(behaviors) != nil {
						n.OnEmpty = linitial(behaviors).(*ast.JsonBehavior)
					}
					if lsecond(behaviors) != nil {
						n.OnError = lsecond(behaviors).(*ast.JsonBehavior)
					}
				}
				$$ = n
			}
		|	JSON_EXISTS '('
			json_value_expr ',' a_expr json_passing_clause_opt
			json_on_error_clause_opt
		')'
			{
				n := ast.NewJsonFuncExpr(ast.JSON_EXISTS_OP, $3.(*ast.JsonValueExpr), $5)
				if $6 != nil {
					n.Passing = $6.(*ast.NodeList)
				}
				n.Output = nil
				if $7 != nil {
					n.OnError = $7.(*ast.JsonBehavior)
				}
				$$ = n
			}
		|	JSON_VALUE '('
			json_value_expr ',' a_expr json_passing_clause_opt
			json_returning_clause_opt
			json_behavior_clause_opt
		')'
			{
				n := ast.NewJsonFuncExpr(ast.JSON_VALUE_OP, $3.(*ast.JsonValueExpr), $5)
				if $6 != nil {
					n.Passing = $6.(*ast.NodeList)
				}
				if $7 != nil {
					n.Output = $7.(*ast.JsonOutput)
				}
				if $8 != nil {
					behaviors := $8.(*ast.NodeList)
					if linitial(behaviors) != nil {
						n.OnEmpty = linitial(behaviors).(*ast.JsonBehavior)
					}
					if lsecond(behaviors) != nil {
						n.OnError = lsecond(behaviors).(*ast.JsonBehavior)
					}
				}
				$$ = n
			}
		|	XMLSERIALIZE '(' document_or_content a_expr AS SimpleTypename xml_indent_option ')'
			{
				// XmlSerialize node - n->xmloption = $3; n->expr = $4; n->typeName = $6; n->indent = $7;
				xmlOption := ast.XmlOptionType($3)
				$$ = ast.NewXmlSerialize(xmlOption, $4, $6, $7, 0)
			}
		;

/* Supporting rules for func_expr_common_subexpr */
func_arg_list_opt:
		func_arg_list
		{
			$$ = $1
		}
	|	/* EMPTY */
		{
			$$ = nil
		}
	;

substr_list:
		a_expr FROM a_expr FOR a_expr
		{
			$$ = ast.NewNodeList($1, $3, $5)
		}
	|	a_expr FOR a_expr FROM a_expr
		{
			$$ = ast.NewNodeList($1, $3, $5)
		}
	|	a_expr FROM a_expr
		{
			$$ = ast.NewNodeList($1, $3)
		}
	|	a_expr FOR a_expr
		{
			sysTypeName := &ast.TypeName{
				Names: ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("int4")),
				Typemod: -1,
			}
			tc := ast.NewTypeCast($3, sysTypeName, -1)
			$$ = ast.NewNodeList($1, ast.NewInteger(1), tc)
		}
	| 	a_expr SIMILAR a_expr ESCAPE a_expr
		{
			$$ = ast.NewNodeList($1, $3, $5)
		}
	;

trim_list:
		a_expr FROM expr_list
		{
			$3.Append($1)
			$$ = $3
		}
	|	FROM expr_list
		{
			$$ = $2
		}
	|	expr_list
		{
			$$ = $1
		}
	;

extract_list:
		extract_arg FROM a_expr
		{
			// list_make2(makeStringConst($1, @1), $3)
			$$ = ast.NewNodeList(ast.NewString($1), $3)
		}
	;

/* Allow delimited string Sconst in extract_arg as an SQL extension.
 * - thomas 2001-04-12
 */
extract_arg:
		IDENT										{ $$ = $1 }
	|	YEAR_P										{ $$ = "year" }
	|	MONTH_P										{ $$ = "month" }
	|	DAY_P										{ $$ = "day" }
	|	HOUR_P										{ $$ = "hour" }
	|	MINUTE_P									{ $$ = "minute" }
	|	SECOND_P									{ $$ = "second" }
	|	Sconst										{ $$ = $1 }
	;

overlay_list:
		a_expr PLACING a_expr FROM a_expr FOR a_expr
		{
			/* overlay(A PLACING B FROM C FOR D) is converted to overlay(A, B, C, D) */
			$$ = ast.NewNodeList($1, $3, $5, $7)
		}
	|	a_expr PLACING a_expr FROM a_expr
		{
			/* overlay(A PLACING B FROM C) is converted to overlay(A, B, C) */
			$$ = ast.NewNodeList($1, $3, $5)
		}
	;

/* position_list uses b_expr not a_expr to avoid conflict with general IN */
position_list:
		b_expr IN_P b_expr						{ $$ = ast.NewNodeList($3, $1) }
	;

xml_attributes: XMLATTRIBUTES '(' xml_attribute_list ')'	{ $$ = $3; }
		;

xml_attribute_list:
		xml_attribute_el
		{
			$$ = ast.NewNodeList($1)
		}
	|	xml_attribute_list ',' xml_attribute_el
		{
			$1.Items = append($1.Items, $3)
			$$ = $1
		}
	;

xml_attribute_el:
		a_expr AS ColLabel
		{
			$$ = ast.NewResTarget($3, $1)
		}
	|	a_expr
		{
			$$ = ast.NewResTarget("", $1)
		}
	;

xml_whitespace_option: PRESERVE WHITESPACE_P		{ $$ = true; }
		| STRIP_P WHITESPACE_P						{ $$ = false; }
		| /*EMPTY*/									{ $$ = false; }
	;

xml_indent_option: INDENT							{ $$ = true; }
		| NO INDENT									{ $$ = false; }
		| /*EMPTY*/									{ $$ = false; }
	;

xml_root_version:
		VERSION_P a_expr
		{
			$$ = $2
		}
	|	VERSION_P NO VALUE_P
		{
			$$ = ast.NewA_Const(ast.NewNull(), 0)
		}
	;

opt_xml_root_standalone:
		',' STANDALONE_P YES_P
		{
			$$ = ast.NewA_Const(ast.NewInteger(int(ast.XML_STANDALONE_YES)), 0)
		}
	|	',' STANDALONE_P NO
		{
			$$ = ast.NewA_Const(ast.NewInteger(int(ast.XML_STANDALONE_NO)), 0)
		}
	|	',' STANDALONE_P NO VALUE_P
		{
			$$ = ast.NewA_Const(ast.NewInteger(int(ast.XML_STANDALONE_NO_VALUE)), 0)
		}
	|	/* EMPTY */
		{
			$$ = ast.NewA_Const(ast.NewInteger(int(ast.XML_STANDALONE_OMITTED)), 0)
		}
	;

/* Function expression without window clause - used in table functions */
func_expr_windowless:
			func_application                    { $$ = $1 }
		| 	func_expr_common_subexpr			{ $$ = $1 }
		|	json_aggregate_func					{ $$ = $1 }
		;

func_name:	type_function_name
			{
				$$ = ast.NewNodeList(ast.NewString($1))
			}
		|	ColId indirection
			{
				// PostgreSQL uses check_func_name here
				// We implement the logic inline - prepend ColId to indirection list
				items := []ast.Node{ast.NewString($1)}
				items = append(items, $2.Items...)
				result := ast.NewNodeList(items...)
				$$ = result
			}
		;

func_application: func_name '(' ')'
			{
				$$ = ast.NewFuncCall($1, nil, 0)
			}
		|	func_name '(' '*' ')'
			{
				// Special case for aggregates like COUNT(*) - set AggStar to true
				funcCall := ast.NewFuncCall($1, nil, 0)
				funcCall.AggStar = true
				$$ = funcCall
			}
		|	func_name '(' func_arg_list opt_sort_clause ')'
			{
				funcCall := ast.NewFuncCall($1, $3, 0)
				if $4 != nil {
					funcCall.AggOrder = $4
				}
				$$ = funcCall
			}
		|	func_name '(' VARIADIC func_arg_expr opt_sort_clause ')'
			{
				funcCall := ast.NewFuncCall($1, ast.NewNodeList($4), 0)
				funcCall.FuncVariadic = true
				if $5 != nil {
					funcCall.AggOrder = $5
				}
				$$ = funcCall
			}
		|	func_name '(' func_arg_list ',' VARIADIC func_arg_expr opt_sort_clause ')'
			{
				$3.Append($6)
				args := $3
				funcCall := ast.NewFuncCall($1, args, 0)
				funcCall.FuncVariadic = true
				if $7 != nil {
					funcCall.AggOrder = $7
				}
				$$ = funcCall
			}
		|	func_name '(' ALL func_arg_list opt_sort_clause ')'
			{
				funcCall := ast.NewFuncCall($1, $4, 0)
				funcCall.AggDistinct = false  // ALL is explicit (though this is default)
				if $5 != nil {
					funcCall.AggOrder = $5
				}
				$$ = funcCall
			}
		|	func_name '(' DISTINCT func_arg_list opt_sort_clause ')'
			{
				funcCall := ast.NewFuncCall($1, $4, 0)
				funcCall.AggDistinct = true
				if $5 != nil {
					funcCall.AggOrder = $5
				}
				$$ = funcCall
			}
		;

func_arg_list: func_arg_expr
			{
				$$ = ast.NewNodeList($1)
			}
		|	func_arg_list ',' func_arg_expr
			{
				$1.Append($3)
				$$ = $1
			}
		;

func_arg_expr: a_expr
			{
				$$ = $1
			}
		|	param_name COLON_EQUALS a_expr
			{
				$$ = ast.NewNamedArgExpr($3.(ast.Expression), $1, -1, 0)
			}
		|	param_name EQUALS_GREATER a_expr
			{
				$$ = ast.NewNamedArgExpr($3.(ast.Expression), $1, -1, 0)
			}
		;

/* CASE expressions */
case_expr:	CASE case_arg when_clause_list case_default END_P
			{
				$$ = ast.NewCaseExpr(0, $2, $3, $4)
			}
		;

case_arg:	a_expr									{ $$ = $1 }
		|	/* EMPTY */							{ $$ = nil }
		;

when_clause_list:
			when_clause							{ $$ = ast.NewNodeList($1) }
		|	when_clause_list when_clause
			{
				$1.Append($2)
				$$ = $1
			}
		;

when_clause:
			WHEN a_expr THEN a_expr
			{
				$$ = ast.NewCaseWhen($2, $4)
			}
		;

case_default:
			ELSE a_expr							{ $$ = $2 }
		|	/* EMPTY */							{ $$ = nil }
		;

/* Array expressions */
array_expr:	'[' expr_list ']'
			{
				$$ = ast.NewArrayConstructor($2)
			}
		|	'[' array_expr_list ']'
			{
				$$ = ast.NewArrayConstructor($2)
			}
		|	'[' ']'
			{
				$$ = ast.NewArrayConstructor(ast.NewNodeList())
			}
		;

array_expr_list: array_expr						{ $$ = ast.NewNodeList($1) }
		|	array_expr_list ',' array_expr
			{
				$1.Append($3)
				$$ = $1
			}
		;

/* Row expressions */
row:		ROW '(' expr_list ')'				{ $$ = $3 }
		|	ROW '(' ')'							{ $$ = ast.NewNodeList() }
		|	'(' expr_list ',' a_expr ')'
			{
				nodeList := $2
				nodeList.Append($4)
				$$ = nodeList
			}
		;

explicit_row:	ROW '(' expr_list ')'			{ $$ = $3 }
		|	ROW '(' ')'							{ $$ = ast.NewNodeList() }
		;

implicit_row:	'(' expr_list ',' a_expr ')'
			{
				$2.Append($4)
				$$ = $2
			}
		;

sub_type:	ANY										{ $$ = int(ast.ANY_SUBLINK) }
		|	SOME									{ $$ = int(ast.ANY_SUBLINK) }
		|	ALL										{ $$ = int(ast.ALL_SUBLINK) }
		;

subquery_Op:
		all_Op
			{
				$$ = ast.NewNodeList(ast.NewString($1))
			}
		|	OPERATOR '(' any_operator ')'
			{
				$$ = $3
			}
		|	LIKE
			{
				$$ = ast.NewNodeList(ast.NewString("~~"))
			}
		|	NOT_LA LIKE
			{
				$$ = ast.NewNodeList(ast.NewString("!~~"))
			}
		|	ILIKE
			{
				$$ = ast.NewNodeList(ast.NewString("~~*"))
			}
		|	NOT_LA ILIKE
			{
				$$ = ast.NewNodeList(ast.NewString("!~~*"))
			}
		;

unicode_normal_form:
		NFC										{ $$ = "NFC" }
	|	NFD										{ $$ = "NFD" }
	|	NFKC									{ $$ = "NFKC" }
	|	NFKD									{ $$ = "NFKD" }
	;

json_predicate_type_constraint:
		JSON	%prec UNBOUNDED					{ $$ = int(ast.JS_TYPE_ANY) }
	|	JSON VALUE_P							{ $$ = int(ast.JS_TYPE_ANY) }
	|	JSON ARRAY								{ $$ = int(ast.JS_TYPE_ARRAY) }
	|	JSON OBJECT_P							{ $$ = int(ast.JS_TYPE_OBJECT) }
	|	JSON SCALAR								{ $$ = int(ast.JS_TYPE_SCALAR) }
	;

json_key_uniqueness_constraint_opt:
		WITH UNIQUE KEYS						{ $$ = true }
	|	WITH UNIQUE				%prec UNBOUNDED	{ $$ = true }
	|	WITHOUT UNIQUE KEYS						{ $$ = false }
	|	WITHOUT UNIQUE			%prec UNBOUNDED	{ $$ = false }
	|	/* EMPTY */				%prec UNBOUNDED	{ $$ = false }
	;

/* Expression lists */
expr_list:	a_expr
			{
				$$ = ast.NewNodeList($1)
			}
		|	expr_list ',' a_expr
			{
				nodeList := $1
				nodeList.Append($3)
				$$ = nodeList
			}
		;

/* Type specifications */
Typename:	SimpleTypename opt_array_bounds
			{
				$$ = $1
				$$.ArrayBounds = $2
			}
		|	SETOF SimpleTypename opt_array_bounds
			{
				$$ = $2
				$$.ArrayBounds = $3
				$$.Setof = true
			}
		|	SimpleTypename ARRAY '[' Iconst ']'
			{
				$$ = $1
				$$.ArrayBounds = ast.NewNodeList(ast.NewInteger($4))
			}
		|	SETOF SimpleTypename ARRAY '[' Iconst ']'
			{
				$$ = $2
				$$.ArrayBounds = ast.NewNodeList(ast.NewInteger($5))
				$$.Setof = true
			}
		|	SimpleTypename ARRAY
			{
				$$ = $1
				$$.ArrayBounds = ast.NewNodeList(ast.NewInteger(-1))
			}
		|	SETOF SimpleTypename ARRAY
			{
				$$ = $2
				$$.ArrayBounds = ast.NewNodeList(ast.NewInteger(-1))
				$$.Setof = true
			}
		;

opt_array_bounds:
			opt_array_bounds '[' ']'
			{
				if $1 == nil {
					$$ = ast.NewNodeList()
				} else {
					$$ = $1
				}
				$$.Append(ast.NewInteger(-1))
			}
		|	opt_array_bounds '[' Iconst ']'
			{
				if $1 == nil {
					$$ = ast.NewNodeList()
				} else {
					$$ = $1
				}
				$$.Append(ast.NewInteger($3))
			}
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

SimpleTypename: GenericType						{ $$ = $1 }
		|	Numeric								{ $$ = $1 }
		|	Bit									{ $$ = $1 }
		|	Character							{ $$ = $1 }
		|	ConstDatetime						{ $$ = $1 }
		| 	ConstInterval opt_interval
			{
				$$ = $1;
				$$.Typmods = $2;
			}
		| ConstInterval '(' Iconst ')'
			{
				$$ = $1;
				$$.Typmods = ast.NewNodeList(ast.NewInteger(ast.INTERVAL_FULL_RANGE), ast.NewInteger($3));
			}
		| JsonType								{ $$ = $1; }
		;

type_function_name: IDENT					{ $$ = $1 }
		|	unreserved_keyword					{ $$ = $1 }
		|	type_func_name_keyword				{ $$ = $1 }
		;

attr_name:	ColLabel						{ $$ = $1 }
		;

param_name:	type_function_name				{ $$ = $1 }
		;

file_name:	Sconst								{ $$ = $1 }
		;

attrs:		'.' attr_name
			{
				$$ = ast.NewNodeList(ast.NewString($2))
			}
		|	attrs '.' attr_name
			{
				$1.Append(ast.NewString($3))
				$$ = $1
			}
		;

opt_type_modifiers: '(' expr_list ')'
			{
				$$ = $2
			}
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

GenericType: type_function_name opt_type_modifiers
			{
				typeName := makeTypeNameFromString($1)
				typeName.Typmods = $2
				$$ = typeName
			}
		|	type_function_name attrs opt_type_modifiers
			{
				// Create qualified type name from name + attrs
				name := ast.NewString($1)
				names := &ast.NodeList{Items: append([]ast.Node{name}, $2.Items...)}
				typeName := makeTypeNameFromNodeList(names)
				typeName.Typmods = $3
				$$ = typeName
			}
		;

Numeric:	INT_P
			{
				$$ = makeTypeNameFromString("int4")
			}
		|	INTEGER
			{
				$$ = makeTypeNameFromString("int4")
			}
		|	SMALLINT
			{
				$$ = makeTypeNameFromString("int2")
			}
		|	BIGINT
			{
				$$ = makeTypeNameFromString("int8")
			}
		|	REAL
			{
				$$ = makeTypeNameFromString("float4")
			}
		|	FLOAT_P opt_float
			{
				$$ = $2
			}
		|	DOUBLE_P PRECISION
			{
				$$ = makeTypeNameFromString("float8")
			}
		|	DECIMAL_P opt_type_modifiers
			{
				$$ = makeTypeNameFromString("numeric")
				$$.Typmods = $2
			}
		|	DEC opt_type_modifiers
			{
				$$ = makeTypeNameFromString("numeric")
				$$.Typmods = $2
			}
		|	NUMERIC opt_type_modifiers
			{
				$$ = makeTypeNameFromString("numeric")
				$$.Typmods = $2
			}
		|	BOOLEAN_P
			{
				$$ = makeTypeNameFromString("bool")
			}
		;

Bit:		BitWithLength
			{
				$$ = $1
			}
		|	BitWithoutLength
			{
				$$ = $1
			}
		;

Character:	CharacterWithLength
			{
				$$ = $1
			}
		|	CharacterWithoutLength
			{
				$$ = $1
			}
		;

ConstBit:	BitWithLength
			{
				$$ = $1
			}
		|	BitWithoutLength
			{
				$$ = $1
				// Set typmods to nil for BitWithoutLength in const context
				$$.Typmods = nil
			}
		;

ConstCharacter:	CharacterWithLength
			{
				$$ = $1
			}
		|	CharacterWithoutLength
			{
				$$ = $1
				// Set typmods to nil for CharacterWithoutLength in const context
				$$.Typmods = nil
			}
		;

character:	CHARACTER opt_varying
			{
				if $2 != 0 {
					$$ = "varchar"
				} else {
					$$ = "bpchar"
				}
			}
		|	CHAR_P opt_varying
			{
				if $2 != 0 {
					$$ = "varchar"
				} else {
					$$ = "bpchar"
				}
			}
		|	VARCHAR
			{
				$$ = "varchar"
			}
		|	NATIONAL CHARACTER opt_varying
			{
				if $3 != 0 {
					$$ = "varchar"
				} else {
					$$ = "bpchar"
				}
			}
		|	NATIONAL CHAR_P opt_varying
			{
				if $3 != 0 {
					$$ = "varchar"
				} else {
					$$ = "bpchar"
				}
			}
		|	NCHAR opt_varying
			{
				if $2 != 0 {
					$$ = "varchar"
				} else {
					$$ = "bpchar"
				}
			}
		;

CharacterWithLength: character '(' Iconst ')'
			{
				typeName := makeTypeNameFromString($1)
                // Set typmods with the length parameter, similar to PostgreSQL's approach
                lengthConst := ast.NewInteger(int($3))
                typeName.Typmods = ast.NewNodeList()
                typeName.Typmods.Append(lengthConst)
                $$ = typeName
			}
		;

CharacterWithoutLength: character
			{
				typeName := makeTypeNameFromString($1)
				// char defaults to char(1), varchar to no limit
				if $1 == "bpchar" {
					// CHAR defaults to CHAR(1)
                    lengthConst := ast.NewInteger(1)
                    typeName.Typmods = ast.NewNodeList()
                    typeName.Typmods.Append(lengthConst)
				}
				$$ = typeName
			}
		;

BitWithLength: BIT opt_varying '(' expr_list ')'
			{
				var typeName string
				if $2 != 0 {
					typeName = "varbit"
				} else {
					typeName = "bit"
				}
				tn := makeTypeNameFromString(typeName)
				tn.Typmods = $4
				$$ = tn
			}
		;

BitWithoutLength: BIT opt_varying
			{
				var typeName string
				if $2 != 0 {
					typeName = "varbit"
				} else {
					typeName = "bit"
				}
				$$ = makeTypeNameFromString(typeName)
			}
		;

ConstDatetime: TIMESTAMP '(' Iconst ')' opt_timezone
			{
				var typeName string
				if $5 != 0 {
					typeName = "timestamptz"
				} else {
					typeName = "timestamp"
				}
				tn := makeTypeNameFromString(typeName)
				tn.Typmods = ast.NewNodeList(ast.NewInteger($3))
				$$ = tn
			}
		|	TIMESTAMP opt_timezone
			{
				var typeName string
				if $2 != 0 {
					typeName = "timestamptz"
				} else {
					typeName = "timestamp"
				}
				$$ = makeTypeNameFromString(typeName)
			}
		|	TIME '(' Iconst ')' opt_timezone
			{
				var typeName string
				if $5 != 0 {
					typeName = "timetz"
				} else {
					typeName = "time"
				}
				tn := makeTypeNameFromString(typeName)
				tn.Typmods = ast.NewNodeList(ast.NewInteger($3))
				$$ = tn
			}
		|	TIME opt_timezone
			{
				var typeName string
				if $2 != 0 {
					typeName = "timetz"
				} else {
					typeName = "time"
				}
				$$ = makeTypeNameFromString(typeName)
			}
		;

JsonType:	JSON
			{
				$$ = makeTypeNameFromString("json")
			}
		;

opt_timezone: WITH_LA TIME ZONE		{ $$ = 1 }
		|	WITHOUT_LA TIME ZONE	{ $$ = 0 }
		|	/* EMPTY */				{ $$ = 0 }
		;

opt_varying: VARYING			{ $$ = 1 }
		|	/* EMPTY */			{ $$ = 0 }
		;

opt_float:	'(' Iconst ')'
			{
				if $2 < 1 {
					$$ = makeTypeNameFromString("float4")
				} else if $2 <= 7 {
					$$ = makeTypeNameFromString("float4")
				} else {
					$$ = makeTypeNameFromString("float8")
				}
			}
		|	/* EMPTY */
			{
				$$ = makeTypeNameFromString("float8")
			}
		;

/* Operators - matching PostgreSQL structure */
qual_Op:	Op
			{
				$$ = ast.NewNodeList(ast.NewString($1))
			}
		|	OPERATOR '(' any_operator ')'
			{
				$$ = $3
			}
		;

qual_all_Op:
			all_Op
				{
					$$ = ast.NewNodeList(ast.NewString($1))
				}
		|	OPERATOR '(' any_operator ')'
				{
					$$ = $3
				}
		;

all_Op:		Op								{ $$ = $1 }
		|	MathOp							{ $$ = $1 }
		;

MathOp:		'+'								{ $$ = "+" }
		|	'-'								{ $$ = "-" }
		|	'*'								{ $$ = "*" }
		|	'/'								{ $$ = "/" }
		|	'%'								{ $$ = "%" }
		|	'^'								{ $$ = "^" }
		|	'<'								{ $$ = "<" }
		|	'>'								{ $$ = ">" }
		|	'='								{ $$ = "=" }
		|	LESS_EQUALS						{ $$ = "<=" }
		|	GREATER_EQUALS					{ $$ = ">=" }
		|	NOT_EQUALS						{ $$ = "<>" }
		;

any_operator: all_Op
			{
				$$ = ast.NewNodeList(ast.NewString($1))
			}
		|	ColId '.' any_operator
			{
				items := []ast.Node{ast.NewString($1)}
				items = append(items, $3.Items...)
				$$ = ast.NewNodeList(items...)
			}
		;

/* Supporting rules for expression grammar */
opt_sort_clause:
		sort_clause							{ $$ = $1 }
	|	/* EMPTY */							{ $$ = nil }
		;

within_group_clause:
		WITHIN GROUP_P '(' sort_clause ')'
			{
				// WITHIN GROUP (ORDER BY ...) for ordered-set aggregates
				$$ = $4
			}
	|	/* EMPTY */								{ $$ = nil }
		;

filter_clause:
		FILTER '(' WHERE a_expr ')'
			{
				// FILTER (WHERE condition) for aggregate filtering
				$$ = $4
			}
	|	/* EMPTY */								{ $$ = nil }
		;


opt_asymmetric:  /* EMPTY */					{ $$ = 0 }
		|	ASYMMETRIC						{ $$ = 0 } /* ASYMMETRIC is default, no-op */
		;

in_expr:		select_with_parens
			{
				subLink := ast.NewSubLink(ast.ANY_SUBLINK, $1.(*ast.SelectStmt))
				$$ = subLink
			}
		|	'(' expr_list ')'
			{
				$$ = $2
			}
		;

/*
 * Phase 3C: SELECT Statement Core Grammar Rules
 * Ported from postgres/src/backend/parser/gram.y
 */

/*
 * SelectStmt - Main SELECT statement entry point
 * From postgres/src/backend/parser/gram.y:12678+
 */
SelectStmt:
			select_no_parens		%prec UMINUS				{ $$ = $1 }
		|	select_with_parens		%prec UMINUS				{ $$ = $1 }
		;

select_with_parens:
			'(' select_no_parens ')'				{ $$ = $2 }
		|	'(' select_with_parens ')'				{ $$ = $2 }
		;


/*
 * select_clause - Core SELECT statement that can appear in set operations
 * From postgres/src/backend/parser/gram.y:12757+
 */
select_clause:
			simple_select							{ $$ = $1 }
		|	select_with_parens						{ $$ = $1 }
		;

/*
 * This rule parses the equivalent of the standard's <query expression>.
 * The locking clause (FOR UPDATE etc) may be before or after LIMIT/OFFSET.
 * From postgres/src/backend/parser/gram.y:12698+
 */
select_no_parens:
			simple_select						{ $$ = $1 }
		|	select_clause sort_clause
			{
				selectStmt := $1.(*ast.SelectStmt)
				// Use NodeList directly for SortClause
				selectStmt.SortClause = $2
				$$ = selectStmt
			}
		|	select_clause opt_sort_clause for_locking_clause opt_select_limit
			{
				selectStmt := $1.(*ast.SelectStmt)
				selectStmt.SortClause = $2
				selectStmt.LockingClause = $3  // Set the locking clause
				if $4 != nil {
					selectStmt.LimitOffset = $4.limitOffset
					selectStmt.LimitCount = $4.limitCount
					selectStmt.LimitOption = $4.limitOption
				}
				$$ = selectStmt
			}
		|	select_clause opt_sort_clause select_limit opt_for_locking_clause
			{
				selectStmt := $1.(*ast.SelectStmt)
				selectStmt.SortClause = $2
				if $3 != nil {
					selectStmt.LimitOffset = $3.limitOffset
					selectStmt.LimitCount = $3.limitCount
					selectStmt.LimitOption = $3.limitOption
				}
				selectStmt.LockingClause = $4  // Set the locking clause
				$$ = selectStmt
			}
		|	with_clause select_clause
			{
				selectStmt := $2.(*ast.SelectStmt)
				selectStmt.WithClause = $1
				$$ = selectStmt
			}
		|	with_clause select_clause sort_clause
			{
				selectStmt := $2.(*ast.SelectStmt)
				selectStmt.WithClause = $1
				selectStmt.SortClause = $3
				$$ = selectStmt
			}
		|	with_clause select_clause opt_sort_clause for_locking_clause opt_select_limit
			{
				selectStmt := $2.(*ast.SelectStmt)
				selectStmt.WithClause = $1
				selectStmt.SortClause = $3
				selectStmt.LockingClause = $4  // Set the locking clause
				if $5 != nil {
					selectStmt.LimitOffset = $5.limitOffset
					selectStmt.LimitCount = $5.limitCount
					selectStmt.LimitOption = $5.limitOption
				}
				$$ = selectStmt
			}
		|	with_clause select_clause opt_sort_clause select_limit opt_for_locking_clause
			{
				selectStmt := $2.(*ast.SelectStmt)
				selectStmt.WithClause = $1
				selectStmt.SortClause = $3
				if $4 != nil {
					selectStmt.LimitOffset = $4.limitOffset
					selectStmt.LimitCount = $4.limitCount
					selectStmt.LimitOption = $4.limitOption
				}
				selectStmt.LockingClause = $5  // Set the locking clause
				$$ = selectStmt
			}
		;

/*
 * Locking clause support (FOR UPDATE, FOR SHARE, etc.)
 * From postgres/src/backend/parser/gram.y:13362+
 */
for_locking_clause:
		for_locking_items						{ $$ = $1 }
	|	FOR READ ONLY							{ $$ = nil }  /* FOR READ ONLY means no locking */
		;

opt_for_locking_clause:
		for_locking_clause						{ $$ = $1 }
	|	/* EMPTY */								{ $$ = nil }
		;

for_locking_items:
		for_locking_item
			{
				$$ = ast.NewNodeList($1)
			}
	|	for_locking_items for_locking_item
			{
				$1.Append($2)
				$$ = $1
			}
		;

for_locking_item:
		for_locking_strength locked_rels_list opt_nowait_or_skip
			{
				lockingClause := &ast.LockingClause{
					BaseNode:   ast.BaseNode{Tag: ast.T_LockingClause},
					Strength:   ast.LockClauseStrength($1),
					LockedRels: $2,  // Store as *NodeList directly
					WaitPolicy: ast.LockWaitPolicy($3),
				}
				$$ = lockingClause
			}
		;

for_locking_strength:
		FOR UPDATE									{ $$ = int(ast.LCS_FORUPDATE) }
	|	FOR NO KEY UPDATE							{ $$ = int(ast.LCS_FORNOKEYUPDATE) }
	|	FOR SHARE									{ $$ = int(ast.LCS_FORSHARE) }
	|	FOR KEY SHARE								{ $$ = int(ast.LCS_FORKEYSHARE) }
		;

locked_rels_list:
		OF qualified_name_list						{ $$ = $2 }
	|	/* EMPTY */									{ $$ = nil }
		;


opt_nowait_or_skip:
		NOWAIT										{ $$ = int(ast.LockWaitError) }
	|	SKIP LOCKED									{ $$ = int(ast.LockWaitSkip) }
	|	/* EMPTY */									{ $$ = int(ast.LockWaitBlock) }
		;

/*
 * simple_select - Core SELECT statement structure
 * From postgres/src/backend/parser/gram.y:12790+
 */
simple_select:
			SELECT opt_all_clause opt_target_list
			into_clause from_clause where_clause
			group_clause having_clause window_clause
			{
				selectStmt := ast.NewSelectStmt()
				if $3 != nil {
					selectStmt.TargetList = $3
				}
				selectStmt.IntoClause = $4
				selectStmt.FromClause = $5
				selectStmt.WhereClause = $6
				if $7 != nil {
					selectStmt.GroupClause = $7.List
					selectStmt.GroupDistinct = $7.Distinct
				}
				selectStmt.HavingClause = $8
				selectStmt.WindowClause = $9
				$$ = selectStmt
			}
		|	SELECT distinct_clause target_list
			into_clause from_clause where_clause
			group_clause having_clause window_clause
			{
				selectStmt := ast.NewSelectStmt()
				selectStmt.DistinctClause = $2
				if $3 != nil {
					selectStmt.TargetList = $3
				}
				selectStmt.IntoClause = $4
				selectStmt.FromClause = $5
				selectStmt.WhereClause = $6
				if $7 != nil {
					selectStmt.GroupClause = $7.List
					selectStmt.GroupDistinct = $7.Distinct
				}
				selectStmt.HavingClause = $8
				selectStmt.WindowClause = $9
				$$ = selectStmt
			}
		|	TABLE relation_expr
			{
				// TABLE relation_expr is equivalent to SELECT * FROM relation_expr
				selectStmt := ast.NewSelectStmt()
				// Create a ResTarget for *
				starTarget := ast.NewResTarget("", ast.NewColumnRef(ast.NewA_Star(0)))
				selectStmt.TargetList = ast.NewNodeList(starTarget)
				selectStmt.FromClause = ast.NewNodeList($2)
				$$ = selectStmt
			}
		|	values_clause
			{
				// VALUES clause is a SelectStmt with ValuesLists
				$$ = $1
			}
		|	select_clause UNION set_quantifier select_clause
			{
				all := $3 == ast.SET_QUANTIFIER_ALL
				$$ = makeSetOp(ast.SETOP_UNION, all, $1, $4)
			}
		|	select_clause INTERSECT set_quantifier select_clause
			{
				all := $3 == ast.SET_QUANTIFIER_ALL
				$$ = makeSetOp(ast.SETOP_INTERSECT, all, $1, $4)
			}
		|	select_clause EXCEPT set_quantifier select_clause
			{
				all := $3 == ast.SET_QUANTIFIER_ALL
				$$ = makeSetOp(ast.SETOP_EXCEPT, all, $1, $4)
			}
		;

/*
 * Target list rules - what columns to select
 * From postgres/src/backend/parser/gram.y:17083+
 */
opt_target_list:
			target_list								{ $$ = $1 }
		|	/* EMPTY */								{ $$ = nil }
		;

target_list:
			target_el								{ $$ = ast.NewNodeList($1) }
		|	target_list ',' target_el				{ $1.Append($3); $$ = $1 }
		;

target_el:
			a_expr AS ColLabel
			{
				$$ = ast.NewResTarget($3, $1)
			}
		|	a_expr BareColLabel
			{
				// Implicit alias (no AS keyword)
				$$ = ast.NewResTarget($2, $1)
			}
		|	a_expr
			{
				// No alias - use default naming
				$$ = ast.NewResTarget("", $1)
			}
		|	'*'
			{
				// SELECT * - all columns
				$$ = ast.NewResTarget("", ast.NewColumnRef(ast.NewA_Star(0)))
			}
		;

/*
 * FROM clause rules
 * From postgres/src/backend/parser/gram.y:17088+
 */
from_clause:
			FROM from_list							{ $$ = $2 }
		|	/* EMPTY */								{ $$ = nil }
		;

from_list:
			table_ref								{ $$ = ast.NewNodeList($1) }
		|	from_list ',' table_ref					{ $1.Append($3); $$ = $1 }
		;

/*
 * Table reference rules
 * From postgres/src/backend/parser/gram.y:13433+
 */
table_ref:
			relation_expr opt_alias_clause
			{
				rangeVar := $1
				if $2 != nil {
					rangeVar.Alias = $2
				}
				$$ = rangeVar
			}
		|	relation_expr opt_alias_clause tablesample_clause
			{
				rangeVar := $1
				if $2 != nil {
					rangeVar.Alias = $2
				}
				rangeTableSample := $3.(*ast.RangeTableSample)
				rangeTableSample.Relation = rangeVar
				$$ = rangeTableSample
			}
		|	select_with_parens opt_alias_clause
			{
				/* Subquery in FROM clause */
				subquery := $1.(*ast.SelectStmt)
				var alias *ast.Alias
				if $2 != nil {
					alias = $2
				}
				rangeSubselect := ast.NewRangeSubselect(false, subquery, alias)
				$$ = rangeSubselect
			}
		|	LATERAL_P select_with_parens opt_alias_clause
			{
				/* LATERAL subquery in FROM clause */
				subquery := $2.(*ast.SelectStmt)
				var alias *ast.Alias
				if $3 != nil {
					alias = $3
				}
				rangeSubselect := ast.NewRangeSubselect(true, subquery, alias)
				$$ = rangeSubselect
			}
		|	joined_table
			{
				$$ = $1
			}
		|	'(' joined_table ')' alias_clause
			{
				joinExpr := $2.(*ast.JoinExpr)
				joinExpr.Alias = $4
				$$ = joinExpr
			}
		|	func_table func_alias_clause
			{
				rangeFunc := $1.(*ast.RangeFunction)
				funcAliasList := $2
				if funcAliasList != nil && funcAliasList.Len() >= 2 {
					// func_alias_clause returns [alias, coldeflist]
					if alias := linitial(funcAliasList); alias != nil {
						rangeFunc.Alias = alias.(*ast.Alias)
					}
					if coldeflist := lsecond(funcAliasList); coldeflist != nil {
						// ColDefList is now *NodeList, no conversion needed
						if nodeList, ok := coldeflist.(*ast.NodeList); ok && nodeList != nil {
							rangeFunc.ColDefList = nodeList
						}
					}
				}
				$$ = rangeFunc
			}
		|	LATERAL_P func_table func_alias_clause
			{
				rangeFunc := $2.(*ast.RangeFunction)
				rangeFunc.Lateral = true
				funcAliasList := $3
				if funcAliasList != nil && funcAliasList.Len() >= 2 {
					// func_alias_clause returns [alias, coldeflist]
					if alias := linitial(funcAliasList); alias != nil {
						rangeFunc.Alias = alias.(*ast.Alias)
					}
					if coldeflist := lsecond(funcAliasList); coldeflist != nil {
						// ColDefList is now *NodeList, no conversion needed
						if nodeList, ok := coldeflist.(*ast.NodeList); ok && nodeList != nil {
							rangeFunc.ColDefList = nodeList
						}
					}
				}
				$$ = rangeFunc
			}
		|	xmltable opt_alias_clause
			{
				rangeTableFunc := $1.(*ast.RangeTableFunc)
				if $2 != nil {
					rangeTableFunc.Alias = $2
				}
				$$ = rangeTableFunc
			}
		|	LATERAL_P xmltable opt_alias_clause
			{
				rangeTableFunc := $2.(*ast.RangeTableFunc)
				rangeTableFunc.Lateral = true
				if $3 != nil {
					rangeTableFunc.Alias = $3
				}
				$$ = rangeTableFunc
			}
		|	json_table opt_alias_clause
			{
				jsonTable := $1.(*ast.JsonTable)
				if $2 != nil {
					jsonTable.Alias = $2
				}
				$$ = jsonTable
			}
		|	LATERAL_P json_table opt_alias_clause
			{
				jsonTable := $2.(*ast.JsonTable)
				jsonTable.Lateral = true
				if $3 != nil {
					jsonTable.Alias = $3
				}
				$$ = jsonTable
			}
		;

relation_expr:
			qualified_name
			{
				rangeVar := $1
				rangeVar.Inh = true // inheritance query, implicitly
				$$ = rangeVar
			}
		|	extended_relation_expr
			{
				$$ = $1
			}
		;

extended_relation_expr:
			qualified_name '*'
			{
				rangeVar := $1
				rangeVar.Inh = true // inheritance query, explicitly
				$$ = rangeVar
			}
		|	ONLY qualified_name
			{
				rangeVar := $2
				rangeVar.Inh = false // no inheritance
				$$ = rangeVar
			}
		|	ONLY '(' qualified_name ')'
			{
				rangeVar := $3
				rangeVar.Inh = false // no inheritance
				$$ = rangeVar
			}
		;

/* Relation expression with optional alias - needed for DML statements */
relation_expr_opt_alias:
			relation_expr %prec UMINUS
			{
				$$ = $1
			}
		|	relation_expr ColId
			{
				rangeVar := $1
				rangeVar.Alias = ast.NewAlias($2, nil)
				$$ = rangeVar
			}
		|	relation_expr AS ColId
			{
				rangeVar := $1
				rangeVar.Alias = ast.NewAlias($3, nil)
				$$ = rangeVar
			}
		;

/*
 * JOIN rules - Phase 3D Implementation
 * From postgres/src/backend/parser/gram.y:13400+
 * It may seem silly to separate joined_table from table_ref, but there is
 * method in SQL's madness: if you don't do it this way you get reduce-
 * reduce conflicts, because it's not clear to the parser generator whether
 * to expect alias_clause after ')' or not.
 */
joined_table:
			'(' joined_table ')'
			{
				$$ = ast.NewParenExpr($2, 0)
			}
		|	table_ref CROSS JOIN table_ref
			{
				/* CROSS JOIN is same as unqualified inner join */
				left := $1
				right := $4
				join := ast.NewJoinExpr(ast.JOIN_INNER, left, right, nil)
				join.IsNatural = false
				$$ = join
			}
		|	table_ref join_type JOIN table_ref join_qual
			{
				left := $1
				right := $4
				joinType := ast.JoinType($2)
				joinQual := $5

				var join *ast.JoinExpr

				/* Check if join_qual is a USING clause (NodeList) or ON clause (Expression) */
				if nodeList, ok := joinQual.(*ast.NodeList); ok && nodeList.Len() == 2 {
					/* USING clause: [name_list, alias_or_null] */
					nameList := nodeList.Items[0].(*ast.NodeList)
					var alias *ast.Alias
					if nodeList.Items[1] != nil {
						alias = nodeList.Items[1].(*ast.Alias)
					}
					join = ast.NewUsingJoinExpr(joinType, left, right, nameList)
					join.JoinUsingAlias = alias
				} else {
					/* ON clause */
					join = ast.NewJoinExpr(joinType, left, right, joinQual.(ast.Expression))
				}
				join.IsNatural = false
				$$ = join
			}
		|	table_ref JOIN table_ref join_qual
			{
				/* letting join_type reduce to empty doesn't work */
				left := $1
				right := $3
				joinQual := $4

				var join *ast.JoinExpr

				/* Check if join_qual is a USING clause (NodeList) or ON clause (Expression) */
				if nodeList, ok := joinQual.(*ast.NodeList); ok && nodeList.Len() == 2 {
					/* USING clause: [name_list, alias_or_null] */
					nameList := nodeList.Items[0].(*ast.NodeList)
					var alias *ast.Alias
					if nodeList.Items[1] != nil {
						alias = nodeList.Items[1].(*ast.Alias)
					}
					join = ast.NewUsingJoinExpr(ast.JOIN_INNER, left, right, nameList)
					join.JoinUsingAlias = alias
				} else {
					/* ON clause */
					join = ast.NewJoinExpr(ast.JOIN_INNER, left, right, joinQual.(ast.Expression))
				}
				join.IsNatural = false
				$$ = join
			}
		|	table_ref NATURAL join_type JOIN table_ref
			{
				left := $1
				right := $5
				joinType := ast.JoinType($3)
				join := ast.NewNaturalJoinExpr(joinType, left, right)
				$$ = join
			}
		|	table_ref NATURAL JOIN table_ref
			{
				/* letting join_type reduce to empty doesn't work */
				left := $1
				right := $4
				join := ast.NewNaturalJoinExpr(ast.JOIN_INNER, left, right)
				$$ = join
			}
		;

join_type:
			FULL opt_outer							{ $$ = int(ast.JOIN_FULL) }
		|	LEFT opt_outer							{ $$ = int(ast.JOIN_LEFT) }
		|	RIGHT opt_outer							{ $$ = int(ast.JOIN_RIGHT) }
		|	INNER_P									{ $$ = int(ast.JOIN_INNER) }
		;

/* OUTER is just noise... */
opt_outer:
			OUTER_P								{ $$ = 1 }
		|	/* EMPTY */							{ $$ = 0 }
		;

/*
 * We return USING as a two-element List (the first item being a sub-List
 * of the common column names, and the second either an Alias item or NULL).
 * An ON-expr will not be a List, so it can be told apart that way.
 */
join_qual:
			USING '(' name_list ')' opt_alias_clause_for_join_using
			{
				/* Create a two-element list: [name_list, alias_or_null] following PostgreSQL */
				nameList := $3
				var aliasNode ast.Node = nil
				if $5 != nil {
					aliasNode = $5
				}
				usingList := ast.NewNodeList(nameList, aliasNode)
				$$ = usingList
			}
		|	ON a_expr
			{
				$$ = $2
			}
		;

/*
 * WITH clause rules - Phase 3D Implementation
 * From postgres/src/backend/parser/gram.y:13215+
 */
with_clause:
			WITH cte_list
			{
				$$ = ast.NewWithClause($2, false, 0)
			}
		|	WITH_LA cte_list
			{
				$$ = ast.NewWithClause($2, false, 0)
			}
		|	WITH RECURSIVE cte_list
			{
				$$ = ast.NewWithClause($3, true, 0)
			}
		;

opt_with_clause:
			with_clause							{ $$ = $1 }
		|	/* EMPTY */							{ $$ = nil }
		;

cte_list:
			common_table_expr
			{
				$$ = ast.NewNodeList($1)
			}
		|	cte_list ',' common_table_expr
			{
				$1.Append($3)
				$$ = $1
			}
		;

common_table_expr:
			name opt_name_list AS opt_materialized '(' PreparableStmt ')' opt_search_clause opt_cycle_clause
			{
				ctename := $1
				query := $6
				cte := ast.NewCommonTableExpr(ctename, query)

				// Set column names if provided
				if $2 != nil {
					cte.Aliascolnames = $2
				}

				// Set materialized option
				cte.Ctematerialized = ast.CTEMaterialized($4)

				// Set search clause if provided
				if $8 != nil {
					cte.SearchClause = $8.(*ast.CTESearchClause)
				}

				// Set cycle clause if provided
				if $9 != nil {
					cte.CycleClause = $9.(*ast.CTECycleClause)
				}

				$$ = cte
			}
		;

opt_materialized:
			MATERIALIZED						{ $$ = int(ast.CTEMaterializeAlways) }
		|	NOT MATERIALIZED					{ $$ = int(ast.CTEMaterializeNever) }
		|	/* EMPTY */							{ $$ = int(ast.CTEMaterializeDefault) }
		;

opt_search_clause:
			SEARCH DEPTH FIRST_P BY columnList SET ColId
			{
				searchColList := $5
				seqColumn := $7
				$$ = ast.NewCTESearchClause(searchColList, false, seqColumn)
			}
		|	SEARCH BREADTH FIRST_P BY columnList SET ColId
			{
				searchColList := $5
				seqColumn := $7
				$$ = ast.NewCTESearchClause(searchColList, true, seqColumn)
			}
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

opt_cycle_clause:
			CYCLE columnList SET ColId TO AexprConst DEFAULT AexprConst USING ColId
			{
				cycleColList := $2
				markColumn := $4
				markValue := $6.(ast.Expression)
				markDefault := $8.(ast.Expression)
				pathColumn := $10
				$$ = ast.NewCTECycleClause(cycleColList, markColumn, markValue, markDefault, pathColumn)
			}
		|	CYCLE columnList SET ColId USING ColId
			{
				cycleColList := $2
				markColumn := $4
				pathColumn := $6
				// For simple CYCLE clause, use nil for mark values to avoid TO/DEFAULT in deparsing
				$$ = ast.NewCTECycleClause(cycleColList, markColumn, nil, nil, pathColumn)
			}
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

/*
 * VALUES clause rules - Phase 3D Implementation
 * From postgres/src/backend/parser/gram.y:13255+
 */
values_clause:
			VALUES '(' expr_list ')'
			{
				/* Create a SelectStmt with VALUES clause following PostgreSQL */
				selectStmt := ast.NewSelectStmt()
				exprList := $3
				selectStmt.ValuesLists = ast.NewNodeList(exprList)
				$$ = selectStmt
			}
		|	values_clause ',' '(' expr_list ')'
			{
				/* Add additional VALUES row to existing SelectStmt */
				selectStmt := $1.(*ast.SelectStmt)
				exprList := $4
				selectStmt.ValuesLists.Append(exprList)
				$$ = selectStmt
			}
		;

/*
 * WHERE clause rules
 * From postgres/src/backend/parser/gram.y:13438+
 */
where_clause:
			WHERE a_expr							{ $$ = $2 }
		|	/* EMPTY */								{ $$ = nil }
		;

/*
 * WHERE clause variant for UPDATE and DELETE that supports CURRENT OF cursor
 * From postgres/src/backend/parser/gram.y where_or_current_clause
 */
where_or_current_clause:
			WHERE a_expr							{ $$ = $2 }
		|	WHERE CURRENT_P OF cursor_name
			{
				cursorExpr := ast.NewCurrentOfExpr(0, $4) // cvarno filled in by parse analysis
				$$ = cursorExpr
			}
		|	/* EMPTY */								{ $$ = nil }
		;

cursor_name:
			name									{ $$ = $1 }
		;

/*
 * Alias clause rules
 * From postgres/src/backend/parser/gram.y:15970+
 */
opt_alias_clause:
			alias_clause							{ $$ = $1 }
		|	/* EMPTY */								{ $$ = nil }
		;

alias_clause:
			AS ColId '(' name_list ')'
			{
				nameList := $4
				$$ = ast.NewAlias($2, nameList)
			}
		|	AS ColId
			{
				$$ = ast.NewAlias($2, nil)
			}
		|	ColId '(' name_list ')'
			{
				nameList := $3
				$$ = ast.NewAlias($1, nameList)
			}
		|	ColId
			{
				$$ = ast.NewAlias($1, nil)
			}
		;

/* func_alias_clause - used by func_table to allow column definitions
 * Returns a list with two elements: [alias, column_definitions]
 * PostgreSQL reference: src/backend/parser/gram.y:13694-13721
 */
func_alias_clause:
			alias_clause
			{
				$$ = ast.NewNodeList($1, nil)
			}
		|	AS '(' TableFuncElementList ')'
			{
				$$ = ast.NewNodeList(nil, $3)
			}
		|	AS ColId '(' TableFuncElementList ')'
			{
				alias := ast.NewAlias($2, nil)
				$$ = ast.NewNodeList(alias, $4)
			}
		|	ColId '(' TableFuncElementList ')'
			{
				alias := ast.NewAlias($1, nil)
				$$ = ast.NewNodeList(alias, $3)
			}
		|	/* EMPTY */
			{
				$$ = ast.NewNodeList(nil, nil)
			}
		;

/* Special variant of opt_alias_clause for JOIN/USING */
opt_alias_clause_for_join_using:
			AS ColId
			{
				alias := ast.NewAlias($2, nil)
				$$ = alias
			}
		|	/* EMPTY */								{ $$ = nil }
		;

/*
 * DISTINCT and ALL clause rules
 * From postgres/src/backend/parser/gram.y:15590+
 */
opt_all_clause:
			ALL										{ $$ = 1 }
		|	/* EMPTY */								{ $$ = 0 }
		;

distinct_clause:
			DISTINCT								{ $$ = ast.NewNodeList() }
		|	DISTINCT ON '(' expr_list ')'			{ $$ = $4 }
		;

opt_distinct_clause:
			distinct_clause							{ $$ = $1 }
		|	opt_all_clause							{ $$ = nil }
		;


/*
 * INTO clause rules (for SELECT INTO)
 * From postgres/src/backend/parser/gram.y:15700+
 */
into_clause:
			INTO OptTempTableName
			{
				$$ = ast.NewIntoClause($2.(*ast.RangeVar), nil, "", nil, ast.ONCOMMIT_NOOP, "", nil, false, 0)
			}
		|	/* EMPTY */								{ $$ = nil }
		;

/*
 * Table Function Rules - Phase 3D Implementation
 * Based on PostgreSQL grammar rules for table functions, XMLTABLE, and JSON_TABLE
 */

/* Table functions from PostgreSQL gram.y:13867-13893 */
func_table:
			func_expr_windowless opt_ordinality
			{
				hasOrdinality := $2 == 1
				// For a simple function, create a NodeList containing a single NodeList with the function
				funcList := ast.NewNodeList($1)
				functions := ast.NewNodeList(funcList)
				rangeFunc := ast.NewRangeFunction(false, hasOrdinality, false, functions, nil, nil)
				$$ = rangeFunc
			}
		|	ROWS FROM '(' rowsfrom_list ')' opt_ordinality
			{
				hasOrdinality := $6 == 1
				// rowsfrom_list is already a NodeList containing NodeLists
				rangeFunc := ast.NewRangeFunction(false, hasOrdinality, true, $4, nil, nil)
				$$ = rangeFunc
			}
		;

/* Optional ORDINALITY clause */
opt_ordinality:
			WITH_LA ORDINALITY						{ $$ = 1 }
		|	/* EMPTY */								{ $$ = 0 }
		;

/* PostgreSQL-style rowsfrom rules */
rowsfrom_list:
			rowsfrom_item
			{
				$$ = ast.NewNodeList($1)
			}
		|	rowsfrom_list ',' rowsfrom_item
			{
				$1.Append($3)
				$$ = $1
			}
		;

rowsfrom_item: func_expr_windowless opt_col_def_list
			{
				funcList := ast.NewNodeList($1)
				if $2 != nil {
					funcList.Append($2)
				}
				$$ = funcList
			}
		;

opt_col_def_list: AS '(' TableFuncElementList ')'  { $$ = $3 }
		|	/* EMPTY */                                     { $$ = nil }
		;

/*
 * Table function column definitions for CREATE FUNCTION ... RETURNS TABLE
 * From PostgreSQL gram.y lines 8794-8803
 * These rules match PostgreSQL exactly even though CREATE FUNCTION is not yet implemented
 */
table_func_column_list:
			table_func_column
			{
				$$ = ast.NewNodeList($1)
			}
		|	table_func_column_list ',' table_func_column
			{
				$1.Append($3)
				$$ = $1
			}
		;

/*
 * Individual table function column for RETURNS TABLE
 * From PostgreSQL gram.y lines 8785-8793
 * Creates a FunctionParameter with mode FUNC_PARAM_TABLE
 */
table_func_column: param_name func_type
			{
				$$ = &ast.FunctionParameter{
					BaseNode: ast.BaseNode{Tag: ast.T_FunctionParameter},
					Name:     $1,
					ArgType:  $2,
					Mode:     ast.FUNC_PARAM_TABLE,
					DefExpr:  nil,
				}
			}
		;

/* Optional table function element list */
OptTableFuncElementList:
			TableFuncElementList					{ $$ = $1 }
		|	/* EMPTY */								{ $$ = nil }
		;

/* Table function element list */
TableFuncElementList:
			TableFuncElement
			{
				$$ = ast.NewNodeList($1)
			}
		|	TableFuncElementList ',' TableFuncElement
			{
				$1.Append($3)
				$$ = $1
			}
		;

/* Table function element - matches PostgreSQL gram.y lines 13946-13964 */
TableFuncElement:
			ColId Typename opt_collate_clause
			{
				columnDef := ast.NewColumnDef($1, $2, 0)
				if collClause, ok := $3.(*ast.CollateClause); ok {
					columnDef.Collclause = collClause
				}
				$$ = columnDef
			}
		;

/* COLLATE clause - matches PostgreSQL gram.y lines 2973-2984 */
opt_collate_clause:
			COLLATE any_name
			{
				// Pass the NodeList directly to NewCollateClause
				nameList := $2
				$$ = ast.NewCollateClause(nameList)
			}
		|	/* EMPTY */		{ $$ = nil }
		;

/* XML passing mechanism - matches PostgreSQL gram.y lines 16180-16183 */
xml_passing_mech:
			BY REF_P	{ $$ = "BY REF" }
		|	BY VALUE_P	{ $$ = "BY VALUE" }
		;

/* XMLEXISTS argument clause - matches PostgreSQL gram.y lines 16161-16178 */
xmlexists_argument:
			PASSING c_expr
			{
				$$ = $2
			}
		|	PASSING c_expr xml_passing_mech
			{
				$$ = $2
			}
		|	PASSING xml_passing_mech c_expr
			{
				$$ = $3
			}
		|	PASSING xml_passing_mech c_expr xml_passing_mech
			{
				$$ = $3
			}
		;

/* XML namespace list - matches PostgreSQL gram.y lines 14105-14129 */
xml_namespace_list:
			xml_namespace_el
			{
				$$ = ast.NewNodeList($1)
			}
		|	xml_namespace_list ',' xml_namespace_el
			{
				$1.Append($3)
				$$ = $1
			}
		;

xml_namespace_el:
			b_expr AS ColLabel
			{
				target := ast.NewResTarget($3, $1)
				$$ = target
			}
		|	DEFAULT b_expr
			{
				target := ast.NewResTarget("", $2)
				$$ = target
			}
		;

/* XMLTABLE matches PostgreSQL gram.y lines 13970-13994 */
xmltable:
			XMLTABLE '(' c_expr xmlexists_argument COLUMNS xmltable_column_list ')'
			{
				// XMLTABLE(xpath_expr PASSING doc_expr COLUMNS ...)
				// $3 is xpath_expr (should be RowExpr), $4 is doc_expr (should be DocExpr)
				rangeTableFunc := ast.NewRangeTableFunc(false, $4.(ast.Expression), $3.(ast.Expression), nil, $6, nil, 0)
				$$ = rangeTableFunc
			}
		|	XMLTABLE '(' XMLNAMESPACES '(' xml_namespace_list ')' ','
			c_expr xmlexists_argument COLUMNS xmltable_column_list ')'
			{
				// XMLTABLE(XMLNAMESPACES(...), xpath_expr PASSING doc_expr COLUMNS ...)
				// $8 is xpath_expr (should be RowExpr), $9 is doc_expr (should be DocExpr)
				rangeTableFunc := ast.NewRangeTableFunc(false, $9.(ast.Expression), $8.(ast.Expression), $5, $11, nil, 0)
				$$ = rangeTableFunc
			}
		;

/* XMLTABLE column list */
xmltable_column_list:
			xmltable_column_el
			{
				$$ = ast.NewNodeList($1)
			}
		|	xmltable_column_list ',' xmltable_column_el
			{
				$1.Append($3)
				$$ = $1
			}
		;

/* XMLTABLE column element */
xmltable_column_el:
			ColId Typename
			{
				rangeTableFuncCol := ast.NewRangeTableFuncCol($1, $2, false, false, nil, nil, 0)
				$$ = rangeTableFuncCol
			}
		|	ColId FOR ORDINALITY
			{
				rangeTableFuncCol := ast.NewRangeTableFuncCol($1, nil, true, false, nil, nil, 0)
				$$ = rangeTableFuncCol
			}
		|	ColId Typename xmltable_column_option_list
			{
				rangeTableFuncCol := ast.NewRangeTableFuncCol($1, $2, false, false, nil, nil, 0)

				// Process column options from $3, matching PostgreSQL's implementation
				optionsList := $3
				if optionsList != nil {
					for _, option := range optionsList.Items {
						if defElem, ok := option.(*ast.DefElem); ok {
							if defElem.Defname == "path" {
								rangeTableFuncCol.ColExpr = defElem.Arg
							} else if defElem.Defname == "default" {
								rangeTableFuncCol.ColDefExpr = defElem.Arg
							} else if defElem.Defname == "is_not_null" {
								if boolVal, ok := defElem.Arg.(*ast.Boolean); ok {
									rangeTableFuncCol.IsNotNull = boolVal.BoolVal
								}
							}
						}
					}
				}

				$$ = rangeTableFuncCol
			}
		;

/* XMLTABLE column option list */
xmltable_column_option_list:
			xmltable_column_option_el
			{
				$$ = ast.NewNodeList($1)
			}
		|	xmltable_column_option_list xmltable_column_option_el
			{
				$1.Append($2)
				$$ = $1
			}
		;

/* XMLTABLE column option element */
xmltable_column_option_el:
			IDENT b_expr
			{
				$$ = ast.NewDefElem($1, $2)
			}
		|	DEFAULT b_expr
			{
				$$ = ast.NewDefElem("default", $2)
			}
		|	NOT NULL_P
			{
				$$ = ast.NewDefElem("is_not_null", ast.NewBoolean(true))
			}
		|	NULL_P
			{
				$$ = ast.NewDefElem("is_not_null", ast.NewBoolean(false))
			}
		|	PATH b_expr
			{
				$$ = ast.NewDefElem("path", $2)
			}
		;

/* JSON_TABLE from PostgreSQL gram.y:14131-14156 - Complete implementation */
json_table:
			JSON_TABLE '('
				json_value_expr ',' a_expr json_table_path_name_opt
				json_passing_clause_opt
				COLUMNS '(' json_table_column_definition_list ')'
				json_on_error_clause_opt
			')'
			{
				// Extract path name from optional path name node
				var pathName string
				if $6 != nil {
					if strNode, ok := $6.(*ast.String); ok {
						pathName = strNode.SVal
					}
				}

				// Create JsonTablePathSpec from the a_expr (path expression) and optional path name
				pathSpec := ast.NewJsonTablePathSpec($5, pathName, 0)

				// Create JsonTable with context item and path spec
				jsonTable := ast.NewJsonTable($3.(*ast.JsonValueExpr), pathSpec)
				// Set PASSING clause if present
				if $7 != nil {
					jsonTable.Passing = $7.(*ast.NodeList)
				}
				if $10 != nil {
					jsonTable.Columns = $10
				}
				// Set ON ERROR clause if present
				if $12 != nil {
					jsonTable.OnError = $12.(*ast.JsonBehavior)
				}
				$$ = jsonTable
			}
		;

/* JSON value expression - simplified for now */
json_value_expr:
			a_expr json_format_clause_opt
			{
				var format *ast.JsonFormat
				if $2 != nil {
					format = $2.(*ast.JsonFormat)
				}
				$$ = ast.NewJsonValueExpr($1, format)
			}
		;

/* Optional JSON_TABLE path name */
json_table_path_name_opt:
			AS ColId								{ $$ = ast.NewString($2) }
		|	/* EMPTY */								{ $$ = nil }
		;

/* Optional PASSING clause */
json_passing_clause_opt:
			PASSING json_arguments					{ $$ = $2 }
		|	/* EMPTY */								{ $$ = nil }
		;

json_arguments:
			json_argument							{ $$ = ast.NewNodeList($1) }
		|	json_arguments ',' json_argument		{ $1.Append($3); $$ = $1 }
		;

json_argument:
			json_value_expr AS ColLabel
			{
				jsonArg := ast.NewJsonArgument($1.(*ast.JsonValueExpr), $3)
				$$ = jsonArg
			}

json_value_expr_list:
			json_value_expr								{ $$ = ast.NewNodeList($1) }
		|	json_value_expr_list ',' json_value_expr	{ $1.Append($3); $$ = $1 }
		;

/* JSON name and value rules - exact PostgreSQL grammar match */
json_name_and_value_list:
			json_name_and_value
			{
				$$ = ast.NewNodeList($1)
			}
		|	json_name_and_value_list ',' json_name_and_value
			{
				$1.Append($3); $$ = $1
			}
		;

json_name_and_value:
/* Supporting this syntax seems to require major surgery
			KEY c_expr VALUE_P json_value_expr
				{ $$ = makeJsonKeyValue($2, $4); }
			|
*/
			c_expr VALUE_P json_value_expr
			{
				$$ = ast.NewJsonKeyValue($1.(ast.Expr),$3.(*ast.JsonValueExpr))
			}
		|	a_expr ':' json_value_expr
			{
				$$ = ast.NewJsonKeyValue($1.(ast.Expr),$3.(*ast.JsonValueExpr))
			}
		;

/* JSON constructor null clauses - exact PostgreSQL grammar match */
json_object_constructor_null_clause_opt:
			NULL_P ON NULL_P					{ $$ = false }
		|	ABSENT ON NULL_P					{ $$ = true }
		|	/* EMPTY */							{ $$ = false }
		;

json_array_constructor_null_clause_opt:
			NULL_P ON NULL_P					{ $$ = false }
		|	ABSENT ON NULL_P					{ $$ = true }
		|	/* EMPTY */							{ $$ = true }
		;

/* JSON array order by clause - exact PostgreSQL grammar match */
json_array_aggregate_order_by_clause_opt:
			ORDER BY sortby_list				{ $$ = $3 }
		|	/* EMPTY */							{ $$ = nil }
		;

/* JSON aggregate functions - exact PostgreSQL grammar match */
json_aggregate_func:
			JSON_OBJECTAGG '('
				json_name_and_value
				json_object_constructor_null_clause_opt
				json_key_uniqueness_constraint_opt
				json_returning_clause_opt
			')'
			{
				var jsonOutput *ast.JsonOutput;
				if $6 != nil {
					jsonOutput = $6.(*ast.JsonOutput)
				}
				constructor := ast.NewJsonAggConstructor(jsonOutput)

				$$ = ast.NewJsonObjectAgg(constructor, $3.(*ast.JsonKeyValue), $4, $5)
			}
		|	JSON_ARRAYAGG '('
				json_value_expr
				json_array_aggregate_order_by_clause_opt
				json_array_constructor_null_clause_opt
				json_returning_clause_opt
			')'
			{
				var jsonOutput *ast.JsonOutput;
				if $6 != nil {
					jsonOutput = $6.(*ast.JsonOutput)
				}
				constructor := ast.NewJsonAggConstructor(jsonOutput)
				constructor.AggOrder = $4
				$$ = ast.NewJsonArrayAgg(constructor, $3.(*ast.JsonValueExpr), $5)
			}
		;

json_returning_clause_opt:
			RETURNING Typename json_format_clause_opt
			{
				var jsonFormat *ast.JsonFormat
				if $3 != nil {
					jsonFormat = $3.(*ast.JsonFormat)
				}
				$$ = ast.NewJsonOutput($2, ast.NewJsonReturning(jsonFormat, 0, 0));
			}
		|	/* EMPTY */								{ $$ = nil }
		;

/* JSON behavior */
json_behavior:
			DEFAULT a_expr
			{
				$$ = &ast.JsonBehavior{
					BaseNode: ast.BaseNode{Tag: ast.T_JsonBehavior},
					Btype: ast.JSON_BEHAVIOR_DEFAULT,
					Expr: $2,
				}
			}
		|	json_behavior_type
			{
				$$ = &ast.JsonBehavior{
					BaseNode: ast.BaseNode{Tag: ast.T_JsonBehavior},
					Btype: ast.JsonBehaviorType($1),
					Expr: nil,
				}
			}
		;

/* JSON behavior type - matches PostgreSQL grammar exactly (ERROR_P, EMPTY_P, ARRAY, OBJECT_P) */
json_behavior_type:
			ERROR_P									{ $$ = int(ast.JSON_BEHAVIOR_ERROR) }
		|	NULL_P									{ $$ = int(ast.JSON_BEHAVIOR_NULL) }
		|	TRUE_P									{ $$ = int(ast.JSON_BEHAVIOR_TRUE) }
		|	FALSE_P									{ $$ = int(ast.JSON_BEHAVIOR_FALSE) }
		|	UNKNOWN									{ $$ = int(ast.JSON_BEHAVIOR_UNKNOWN) }
		|	EMPTY_P ARRAY							{ $$ = int(ast.JSON_BEHAVIOR_EMPTY_ARRAY) }
		|	EMPTY_P OBJECT_P						{ $$ = int(ast.JSON_BEHAVIOR_EMPTY_OBJECT) }
		|	EMPTY_P									{ $$ = int(ast.JSON_BEHAVIOR_EMPTY) }
		;

/* JSON behavior clause */
json_behavior_clause_opt:
			json_behavior ON EMPTY_P				{
				// Return a list with ON EMPTY behavior and nil for ON ERROR
				$$ = &ast.NodeList{Items: []ast.Node{$1, nil}}
			}
		|	json_behavior ON ERROR_P				{
				// Return a list with nil for ON EMPTY and ON ERROR behavior
				$$ = &ast.NodeList{Items: []ast.Node{nil, $1}}
			}
		|	json_behavior ON EMPTY_P json_behavior ON ERROR_P	{
				// Return a list with both ON EMPTY and ON ERROR behaviors
				$$ = &ast.NodeList{Items: []ast.Node{$1, $4}}
			}
		|	/* EMPTY */								{ $$ = nil }
		;

/* Optional ON ERROR clause */
json_on_error_clause_opt:
			json_behavior ON ERROR_P				{ $$ = $1 }
		|	/* EMPTY */								{ $$ = nil }
		;

/* JSON wrapper behavior - matches PostgreSQL grammar exactly */
json_wrapper_behavior:
			WITHOUT WRAPPER							{ $$ = int(ast.JSW_NONE) }
		|	WITHOUT ARRAY WRAPPER					{ $$ = int(ast.JSW_NONE) }
		|	WITH WRAPPER							{ $$ = int(ast.JSW_UNCONDITIONAL) }
		|	WITH ARRAY WRAPPER						{ $$ = int(ast.JSW_UNCONDITIONAL) }
		|	WITH CONDITIONAL ARRAY WRAPPER			{ $$ = int(ast.JSW_CONDITIONAL) }
		|	WITH UNCONDITIONAL ARRAY WRAPPER		{ $$ = int(ast.JSW_UNCONDITIONAL) }
		|	WITH CONDITIONAL WRAPPER				{ $$ = int(ast.JSW_CONDITIONAL) }
		|	WITH UNCONDITIONAL WRAPPER				{ $$ = int(ast.JSW_UNCONDITIONAL) }
		|	/* EMPTY */								{ $$ = int(ast.JSW_UNSPEC) }
		;

/* JSON quotes clause - matches PostgreSQL grammar exactly */
json_quotes_clause_opt:
			KEEP QUOTES ON SCALAR STRING_P			{ $$ = int(ast.JS_QUOTES_KEEP) }
		|	KEEP QUOTES								{ $$ = int(ast.JS_QUOTES_KEEP) }
		|	OMIT QUOTES ON SCALAR STRING_P			{ $$ = int(ast.JS_QUOTES_OMIT) }
		|	OMIT QUOTES								{ $$ = int(ast.JS_QUOTES_OMIT) }
		|	/* EMPTY */								{ $$ = int(ast.JS_QUOTES_UNSPEC) }
		;

/* JSON format clause - matches PostgreSQL grammar exactly (uses 'name' not 'ColId') */
json_format_clause:
			FORMAT_LA JSON ENCODING name
			{
				// Parse the encoding name and map to JsonEncoding constant
				var encoding ast.JsonEncoding
				switch $4 {
				case "utf8":
					encoding = ast.JS_ENC_UTF8
				case "utf16":
					encoding = ast.JS_ENC_UTF16
				case "utf32":
					encoding = ast.JS_ENC_UTF32
				default:
					encoding = ast.JS_ENC_DEFAULT
				}
				$$ = &ast.JsonFormat{
					BaseNode: ast.BaseNode{Tag: ast.T_JsonFormat},
					FormatType: ast.JS_FORMAT_JSON,
					Encoding: encoding,
				}
			}
		|	FORMAT_LA JSON
			{
				$$ = &ast.JsonFormat{
					BaseNode: ast.BaseNode{Tag: ast.T_JsonFormat},
					FormatType: ast.JS_FORMAT_JSON,
					Encoding: ast.JS_ENC_DEFAULT,
				}
			}
		;

json_format_clause_opt:
			json_format_clause						{ $$ = $1 }
		|	/* EMPTY */								{ $$ = nil }
		;

/* Path optional keyword */
path_opt:
			PATH									{ $$ = ast.NewString("path") }
		|	/* EMPTY */								{ $$ = nil }
		;

/* JSON_TABLE column definition list */
json_table_column_definition_list:
			json_table_column_definition
			{
				$$ = ast.NewNodeList($1)
			}
		|	json_table_column_definition_list ',' json_table_column_definition
			{
				$1.Append($3)
				$$ = $1
			}
		;

/* JSON_TABLE column definition - Complete PostgreSQL implementation */
json_table_column_definition:
			ColId FOR ORDINALITY
			{
				jsonTableCol := ast.NewJsonTableColumn(ast.JTC_FOR_ORDINALITY, $1)
				$$ = jsonTableCol
			}
		|	ColId Typename
			json_table_column_path_clause_opt
			json_wrapper_behavior
			json_quotes_clause_opt
			json_behavior_clause_opt
			{
				jsonTableCol := ast.NewJsonTableColumn(ast.JTC_REGULAR, $1)
				jsonTableCol.TypeName = $2
				if $3 != nil {
					strNode := $3.(*ast.String)
					jsonTableCol.Pathspec = ast.NewJsonTablePathSpec(strNode, "", 0)
				}
				$$ = jsonTableCol
			}
		|	ColId Typename json_format_clause
			json_table_column_path_clause_opt
			json_wrapper_behavior
			json_quotes_clause_opt
			json_behavior_clause_opt
			{
				jsonTableCol := ast.NewJsonTableColumn(ast.JTC_FORMATTED, $1)
				jsonTableCol.TypeName = $2
				if $4 != nil {
					strNode := $4.(*ast.String)
					jsonTableCol.Pathspec = ast.NewJsonTablePathSpec(strNode, "", 0)
				}
				if $3 != nil {
					jsonTableCol.Format = $3.(*ast.JsonFormat)
				}
				$$ = jsonTableCol
			}
		|	ColId Typename EXISTS json_table_column_path_clause_opt
			json_on_error_clause_opt
			{
				jsonTableCol := ast.NewJsonTableColumn(ast.JTC_EXISTS, $1)
				jsonTableCol.TypeName = $2
				if $4 != nil {
					strNode := $4.(*ast.String)
					jsonTableCol.Pathspec = ast.NewJsonTablePathSpec(strNode, "", 0)
				}
				$$ = jsonTableCol
			}
		|	NESTED path_opt SCONST
			COLUMNS '(' json_table_column_definition_list ')'
			{
				jsonTableCol := ast.NewJsonTableColumn(ast.JTC_NESTED, "")
				jsonTableCol.Columns = $6
				strNode := ast.NewString($3)
				jsonTableCol.Pathspec = ast.NewJsonTablePathSpec(strNode, "", 0)
				$$ = jsonTableCol
			}
		|	NESTED path_opt SCONST AS name
			COLUMNS '(' json_table_column_definition_list ')'
			{
				jsonTableCol := ast.NewJsonTableColumn(ast.JTC_NESTED, $5)
				jsonTableCol.Columns = $8
				strNode := ast.NewString($3)
				jsonTableCol.Pathspec = ast.NewJsonTablePathSpec(strNode, "", 0)
				$$ = jsonTableCol
			}
		;

/* JSON_TABLE column path clause - Use string constants */
json_table_column_path_clause_opt:
			PATH SCONST								{ $$ = ast.NewString($2) }
		|	/* EMPTY */								{ $$ = nil }
		;

/*
 * *** Phase 3E: Data Manipulation Language (DML) ***
 * INSERT, UPDATE, DELETE, MERGE statements
 * From postgres/src/backend/parser/gram.y DML sections
 */

/*
 * INSERT Statement
 * From postgres/src/backend/parser/gram.y:4490-4503
 */
InsertStmt:
			opt_with_clause INSERT INTO insert_target insert_rest
			opt_on_conflict returning_clause
			{
				insertStmt := $5.(*ast.InsertStmt)
				insertStmt.Relation = $4
				insertStmt.WithClause = $1
				insertStmt.OnConflictClause = $6
				insertStmt.ReturningList = $7
				$$ = insertStmt
			}
		;

insert_target:
			qualified_name
			{
				$$ = $1
			}
		|	qualified_name AS ColId
			{
				rangeVar := $1
				rangeVar.Alias = ast.NewAlias($3, nil)
				$$ = rangeVar
			}
		;

insert_rest:
			SelectStmt
			{
				insertStmt := ast.NewInsertStmt(nil)
				insertStmt.SelectStmt = $1
				$$ = insertStmt
			}
		|	OVERRIDING override_kind VALUE_P SelectStmt
			{
				insertStmt := ast.NewInsertStmt(nil)
				insertStmt.Override = ast.OverridingKind($2)
				insertStmt.SelectStmt = $4
				$$ = insertStmt
			}
		|	'(' insert_column_list ')' SelectStmt
			{
				insertStmt := ast.NewInsertStmt(nil)
				insertStmt.Cols = $2
				insertStmt.SelectStmt = $4
				$$ = insertStmt
			}
		|	'(' insert_column_list ')' OVERRIDING override_kind VALUE_P SelectStmt
			{
				insertStmt := ast.NewInsertStmt(nil)
				insertStmt.Cols = $2
				insertStmt.Override = ast.OverridingKind($5)
				insertStmt.SelectStmt = $7
				$$ = insertStmt
			}
		|	DEFAULT VALUES
			{
				insertStmt := ast.NewInsertStmt(nil)
				// For DEFAULT VALUES, SelectStmt should be nil
				insertStmt.SelectStmt = nil
				$$ = insertStmt
			}
		;

override_kind:
			USER		{ $$ = int(ast.OVERRIDING_USER_VALUE) }
		|	SYSTEM_P	{ $$ = int(ast.OVERRIDING_SYSTEM_VALUE) }
		;

insert_column_list:
			insert_column_item
			{
				$$ = ast.NewNodeList($1)
			}
		|	insert_column_list ',' insert_column_item
			{
				$1.Append($3)
				$$ = $1
			}
		;

insert_column_item:
			ColId opt_indirection
			{
				$$ = ast.NewResTargetWithIndirection($1, $2)
			}
		;

/*
 * UPDATE Statement
 * From postgres/src/backend/parser/gram.y:4538-4553
 */
UpdateStmt:
			opt_with_clause UPDATE relation_expr_opt_alias SET set_clause_list
			from_clause where_or_current_clause returning_clause
			{
				updateStmt := ast.NewUpdateStmt($3)
				updateStmt.WithClause = $1
				updateStmt.TargetList = $5
				updateStmt.FromClause = $6
				updateStmt.WhereClause = $7
				updateStmt.ReturningList = $8
				$$ = updateStmt
			}
		;

set_clause_list:
			set_clause
			{
				$$ = $1
			}
		|	set_clause_list ',' set_clause
			{
				// Concatenate the lists - equivalent to PostgreSQL's list_concat
				for _, item := range $3.Items {
					$1.Append(item)
				}
				$$ = $1
			}
		;

set_clause:
			set_target '=' a_expr
			{
				target := $1
				target.Val = $3
				$$ = ast.NewNodeList(target)
			}
		|	'(' set_target_list ')' '=' a_expr
			{
				// Multi-column assignment: (col1, col2) = (val1, val2)
				// Create MultiAssignRef nodes for each target, matching PostgreSQL exactly
				targetList := $2
				ncolumns := len(targetList.Items)

				// Create a MultiAssignRef source for each target
				for i, item := range targetList.Items {
					resCol := item.(*ast.ResTarget)
					multiAssignRef := ast.NewMultiAssignRef($5, i+1, ncolumns, 0)
					resCol.Val = multiAssignRef
				}

				// Return the entire target list
				$$ = targetList
			}
		;

set_target:
			ColId opt_indirection
			{
				$$ = ast.NewResTargetWithIndirection($1, $2)
			}
		;

set_target_list:
			set_target								{ $$ = ast.NewNodeList($1) }
		|	set_target_list ',' set_target			{ $1.Append($3); $$ = $1 }
		;

/*
 * DELETE Statement
 * From postgres/src/backend/parser/gram.y:4585-4598
 */
DeleteStmt:
			opt_with_clause DELETE_P FROM relation_expr_opt_alias
			using_clause where_or_current_clause returning_clause
			{
				deleteStmt := ast.NewDeleteStmt($4)
				deleteStmt.WithClause = $1
				deleteStmt.UsingClause = $5
				deleteStmt.WhereClause = $6
				deleteStmt.ReturningList = $7
				$$ = deleteStmt
			}
		;

using_clause:
			USING from_list
			{
				$$ = $2
			}
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

/*
 * MERGE Statement (PostgreSQL 15+)
 * From postgres/src/backend/parser/gram.y:4630-4644
 */
MergeStmt:
			opt_with_clause MERGE INTO relation_expr_opt_alias
			USING table_ref
			ON a_expr
			merge_when_list
			returning_clause
			{
				mergeStmt := &ast.MergeStmt{
					BaseNode: ast.BaseNode{Tag: ast.T_MergeStmt},
				}
				mergeStmt.WithClause = $1
				mergeStmt.Relation = $4
				mergeStmt.SourceRelation = $6
				mergeStmt.JoinCondition = $8
				mergeStmt.MergeWhenClauses = $9
				mergeStmt.ReturningList = $10
				$$ = mergeStmt
			}
		;


merge_when_list:
			merge_when_clause
			{
				$$ = ast.NewNodeList($1)
			}
		|	merge_when_list merge_when_clause
			{
				$1.Append($2); $$ = $1
			}
		;

/*
 * COPY Statement - matches PostgreSQL exactly
 * Based on postgres/src/backend/parser/gram.y:3341-3397
 */
CopyStmt:
			COPY '(' PreparableStmt ')' TO opt_program copy_file_name opt_with copy_options
			{
				copyStmt := &ast.CopyStmt{
					BaseNode:  ast.BaseNode{Tag: ast.T_CopyStmt},
					Query:     $3,
					IsFrom:    false,
					IsProgram: $6 != 0,
					Filename:  $7,
				}
				// Initialize Options as empty NodeList
				copyStmt.Options = ast.NewNodeList()
				if $8 != nil {
					copyStmt.Options.Append($8)
				}
				if $9 != nil {
					nodeList := $9
					for _, node := range nodeList.Items {
						copyStmt.Options.Append(node)
					}
				}
				$$ = copyStmt
			}
		|	COPY opt_binary qualified_name opt_column_list
			copy_from opt_program copy_file_name copy_delimiter opt_with
			copy_options where_clause
			{
				copyStmt := &ast.CopyStmt{
					BaseNode: ast.BaseNode{Tag: ast.T_CopyStmt},
					Relation: $3,
					IsFrom:   $5 != 0,
					IsProgram: $6 != 0,
					Filename:  $7,
				}
				// Assign column list directly as NodeList
				copyStmt.Attlist = $4

				// Initialize Options as empty NodeList and add options
				copyStmt.Options = ast.NewNodeList()
				if $2 != nil {
					copyStmt.Options.Append($2)
				}
				if $8 != nil {
					copyStmt.Options.Append($8)
				}
				if $10 != nil {
					nodeList := $10
					for _, node := range nodeList.Items {
						copyStmt.Options.Append(node)
					}
				}
				if $11 != nil {
					copyStmt.WhereClause = $11
				}
				$$ = copyStmt
			}
		;

/*
 * COPY statement supporting rules
 */
copy_from:
			FROM					{ $$ = 1 }
		|	TO						{ $$ = 0 }
		;

opt_program:
			PROGRAM					{ $$ = 1 }
		|	/* EMPTY */				{ $$ = 0 }
		;

copy_file_name:
			Sconst					{ $$ = $1 }
		|	STDIN					{ $$ = "" }  /* Use empty string for stdin */
		|	STDOUT					{ $$ = "" }  /* Use empty string for stdout */
		;

opt_binary:
			BINARY
			{
				$$ = ast.NewDefElem("format", ast.NewString("binary"))
			}
		|	/* EMPTY */				{ $$ = nil }
		;

copy_delimiter:
			opt_using DELIMITERS Sconst
			{
				$$ = ast.NewDefElem("delimiter", ast.NewString($3))
			}
		|	/* EMPTY */				{ $$ = nil }
		;

copy_options:
			copy_opt_list			{ $$ = $1 }
		|	'(' copy_generic_opt_list ')'			{ $$ = $2 }
		;

copy_opt_list:
			copy_opt_list copy_opt_item
			{
				if $1 == nil {
				$$ = ast.NewNodeList($2)
			} else {
				$1.Append($2); $$ = $1
			}
			}
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

copy_opt_item:
			BINARY
			{
				$$ = ast.NewDefElem("format", ast.NewString("binary"))
			}
		|	FREEZE
			{
				$$ = ast.NewDefElem("freeze", ast.NewString("true"))
			}
		|	DELIMITER opt_as Sconst
			{
				$$ = ast.NewDefElem("delimiter", ast.NewString($3))
			}
		|	NULL_P opt_as Sconst
			{
				$$ = ast.NewDefElem("null", ast.NewString($3))
			}
		|	CSV
			{
				$$ = ast.NewDefElem("format", ast.NewString("csv"))
			}
		|	HEADER_P
			{
				$$ = ast.NewDefElem("header", ast.NewString("true"))
			}
		|	QUOTE opt_as Sconst
			{
				$$ = ast.NewDefElem("quote", ast.NewString($3))
			}
		|	ESCAPE opt_as Sconst
			{
				$$ = ast.NewDefElem("escape", ast.NewString($3))
			}
		|	FORCE QUOTE columnList
			{
				$$ = ast.NewDefElem("force_quote", $3)
			}
		|	FORCE QUOTE '*'
			{
				$$ = ast.NewDefElem("force_quote", &ast.A_Star{BaseNode: ast.BaseNode{Tag: ast.T_A_Star}})
			}
		|	FORCE NOT NULL_P columnList
			{
				$$ = ast.NewDefElem("force_not_null", $4)
			}
		|	FORCE NOT NULL_P '*'
			{
				$$ = ast.NewDefElem("force_not_null", &ast.A_Star{BaseNode: ast.BaseNode{Tag: ast.T_A_Star}})
			}
		|	FORCE NULL_P columnList
			{
				$$ = ast.NewDefElem("force_null", $3)
			}
		|	FORCE NULL_P '*'
			{
				$$ = ast.NewDefElem("force_null", &ast.A_Star{BaseNode: ast.BaseNode{Tag: ast.T_A_Star}})
			}
		|	ENCODING Sconst
			{
				$$ = ast.NewDefElem("encoding", ast.NewString($2))
			}
		;

/* new COPY option syntax */
copy_generic_opt_list:
		copy_generic_opt_elem
		{
			$$ = ast.NewNodeList($1)
		}
	|	copy_generic_opt_list ',' copy_generic_opt_elem
		{
			$1.Append($3); $$ = $1
		}
	;

copy_generic_opt_elem:
		ColLabel copy_generic_opt_arg
		{
			$$ = ast.NewDefElem($1, $2)
		}
	;

copy_generic_opt_arg:
		opt_boolean_or_string			{ $$ = ast.NewString($1) }
	|	NumericOnly						{ $$ = $1 }
	|	'*'								{ $$ = ast.NewA_Star(0) }
	|	DEFAULT							{ $$ = ast.NewString("default") }
	|	'(' copy_generic_opt_arg_list ')'		{ $$ = $2 }
	|	/* EMPTY */						{ $$ = nil }
	;

copy_generic_opt_arg_list:
		copy_generic_opt_arg_list_item
		{
			$$ = ast.NewNodeList($1)
		}
	|	copy_generic_opt_arg_list ',' copy_generic_opt_arg_list_item
		{
			$1.Append($3); $$ = $1
		}
	;

copy_generic_opt_arg_list_item:
		opt_boolean_or_string			{ $$ = ast.NewString($1) }
	;

opt_boolean_or_string:
		TRUE_P							{ $$ = "true" }
	|	FALSE_P							{ $$ = "false" }
	|	ON								{ $$ = "on" }
	|	NonReservedWord_or_Sconst		{ $$ = $1 }
	;

NumericOnly:
		FCONST							{ $$ = ast.NewFloat($1) }
	|	'+' FCONST						{ $$ = ast.NewFloat($2) }
	|	'-' FCONST
		{
			f := ast.NewFloat($2)
			doNegateFloat(f)
			$$ = f
		}
	|	SignedIconst					{ $$ = ast.NewInteger($1) }
	;

set_statistics_value:
		SignedIconst					{ $$ = ast.NewInteger($1) }
	|	DEFAULT							{ $$ = nil }
	;

drop_type_name:
		ACCESS METHOD						{ $$ = ast.OBJECT_ACCESS_METHOD }
	|	EVENT TRIGGER						{ $$ = ast.OBJECT_EVENT_TRIGGER }
	|	EXTENSION							{ $$ = ast.OBJECT_EXTENSION }
	|	FOREIGN DATA_P WRAPPER				{ $$ = ast.OBJECT_FDW }
	|	opt_procedural LANGUAGE				{ $$ = ast.OBJECT_LANGUAGE }
	|	PUBLICATION							{ $$ = ast.OBJECT_PUBLICATION }
	|	SCHEMA								{ $$ = ast.OBJECT_SCHEMA }
	|	SERVER								{ $$ = ast.OBJECT_FOREIGN_SERVER }
	;

object_type_name_on_any_name:
		POLICY								{ $$ = ast.OBJECT_POLICY }
	|	RULE								{ $$ = ast.OBJECT_RULE }
	|	TRIGGER								{ $$ = ast.OBJECT_TRIGGER }
	;

object_type_name:
		drop_type_name						{ $$ = $1 }
	|	DATABASE							{ $$ = ast.OBJECT_DATABASE }
	|	ROLE								{ $$ = ast.OBJECT_ROLE }
	|	SUBSCRIPTION						{ $$ = ast.OBJECT_SUBSCRIPTION }
	|	TABLESPACE							{ $$ = ast.OBJECT_TABLESPACE }
	;

type_name_list:
		Typename
			{
				$$ = ast.NewNodeList()
				$$.Append($1)
			}
	|	type_name_list ',' Typename
			{
				$1.Append($3)
				$$ = $1
			}
	;

opt_procedural:
		PROCEDURAL						{ $$ = true }
	|	/* EMPTY */						{ $$ = false }
	;

opt_set_data:
		SET DATA_P							{ $$ = 1 }
	|	/* EMPTY */							{ $$ = 0 }
	;

/*
 * Optional USING clause for copy_delimiter
 */
opt_using:
			USING
		|	/* EMPTY */
		;

/*
 * Optional AS clause for copy options
 */
opt_as:
			AS
				{ $$ = nil }
		|	/* EMPTY */
				{ $$ = nil }
		;

/*
 * Utility options for various statements
 * Based on postgres/src/backend/parser/gram.y utility options
 */
opt_freeze:
			FREEZE					{ $$ = 1 }
		|	/* EMPTY */				{ $$ = 0 }
		;

opt_verbose:
			VERBOSE					{ $$ = 1 }
		|	/* EMPTY */				{ $$ = 0 }
		;

opt_analyze:
			ANALYZE					{ $$ = 1 }
		|	/* EMPTY */				{ $$ = 0 }
		;

opt_full:
			FULL					{ $$ = 1 }
		|	/* EMPTY */				{ $$ = 0 }
		;

/*
 * MERGE WHEN clauses - matches PostgreSQL exactly
 * Based on postgres/src/backend/parser/gram.y:12459-12501
 */
merge_when_clause:
			merge_when_tgt_matched opt_merge_when_condition THEN merge_update
			{
				$4.(*ast.MergeWhenClause).MatchKind = ast.MergeMatchKind($1)
				$4.(*ast.MergeWhenClause).Condition = $2
				$$ = $4
			}
		|	merge_when_tgt_matched opt_merge_when_condition THEN merge_delete
			{
				$4.(*ast.MergeWhenClause).MatchKind = ast.MergeMatchKind($1)
				$4.(*ast.MergeWhenClause).Condition = $2
				$$ = $4
			}
		|	merge_when_tgt_not_matched opt_merge_when_condition THEN merge_insert
			{
				$4.(*ast.MergeWhenClause).MatchKind = ast.MergeMatchKind($1)
				$4.(*ast.MergeWhenClause).Condition = $2
				$$ = $4
			}
		|	merge_when_tgt_matched opt_merge_when_condition THEN DO NOTHING
			{
				mergeWhen := ast.NewMergeWhenClause(ast.MergeMatchKind($1), ast.CMD_NOTHING)
				mergeWhen.Condition = $2
				$$ = mergeWhen
			}
		|	merge_when_tgt_not_matched opt_merge_when_condition THEN DO NOTHING
			{
				mergeWhen := ast.NewMergeWhenClause(ast.MergeMatchKind($1), ast.CMD_NOTHING)
				mergeWhen.Condition = $2
				$$ = mergeWhen
			}
		;

merge_when_tgt_matched:
			WHEN MATCHED						{ $$ = int(ast.MERGE_WHEN_MATCHED) }
		|	WHEN NOT MATCHED BY SOURCE			{ $$ = int(ast.MERGE_WHEN_NOT_MATCHED_BY_SOURCE) }
		;

merge_when_tgt_not_matched:
			WHEN NOT MATCHED					{ $$ = int(ast.MERGE_WHEN_NOT_MATCHED_BY_TARGET) }
		|	WHEN NOT MATCHED BY TARGET			{ $$ = int(ast.MERGE_WHEN_NOT_MATCHED_BY_TARGET) }
		;

opt_merge_when_condition:
			AND a_expr				{ $$ = $2 }
		|	/* EMPTY */				{ $$ = nil }
		;

merge_update:
		UPDATE SET set_clause_list
		{
			mergeWhen := ast.NewMergeWhenClause(ast.MERGE_WHEN_MATCHED, ast.CMD_UPDATE)
			mergeWhen.Override = ast.OVERRIDING_NOT_SET
			if $3 != nil {
				nodeList := $3
				for _, node := range nodeList.Items {
					mergeWhen.TargetList = append(mergeWhen.TargetList, node.(*ast.ResTarget))
				}
			}
			$$ = mergeWhen
		}
	;

merge_delete:
		DELETE_P
		{
			mergeWhen := ast.NewMergeWhenClause(ast.MERGE_WHEN_MATCHED, ast.CMD_DELETE)
			mergeWhen.Override = ast.OVERRIDING_NOT_SET
			$$ = mergeWhen
		}
	;

merge_insert:
		INSERT merge_values_clause
		{
			mergeWhen := ast.NewMergeWhenClause(ast.MERGE_WHEN_NOT_MATCHED_BY_TARGET, ast.CMD_INSERT)
			mergeWhen.Override = ast.OVERRIDING_NOT_SET
			mergeWhen.Values = $2
			$$ = mergeWhen
		}
	|	INSERT OVERRIDING override_kind VALUE_P merge_values_clause
		{
			mergeWhen := ast.NewMergeWhenClause(ast.MERGE_WHEN_NOT_MATCHED_BY_TARGET, ast.CMD_INSERT)
			mergeWhen.Override = ast.OverridingKind($3)
			mergeWhen.Values = $5
			$$ = mergeWhen
		}
	|	INSERT '(' insert_column_list ')' merge_values_clause
		{
			mergeWhen := ast.NewMergeWhenClause(ast.MERGE_WHEN_NOT_MATCHED_BY_TARGET, ast.CMD_INSERT)
			mergeWhen.Override = ast.OVERRIDING_NOT_SET
			if $3 != nil {
				nodeList := $3
				for _, node := range nodeList.Items {
					mergeWhen.TargetList = append(mergeWhen.TargetList, node.(*ast.ResTarget))
				}
			}
			mergeWhen.Values = $5
			$$ = mergeWhen
		}
	|	INSERT '(' insert_column_list ')' OVERRIDING override_kind VALUE_P merge_values_clause
		{
			mergeWhen := ast.NewMergeWhenClause(ast.MERGE_WHEN_NOT_MATCHED_BY_TARGET, ast.CMD_INSERT)
			mergeWhen.Override = ast.OverridingKind($6)
			if $3 != nil {
				nodeList := $3
				for _, node := range nodeList.Items {
					mergeWhen.TargetList = append(mergeWhen.TargetList, node.(*ast.ResTarget))
				}
			}
			mergeWhen.Values = $8
			$$ = mergeWhen
		}
	|	INSERT DEFAULT VALUES
		{
			mergeWhen := ast.NewMergeWhenClause(ast.MERGE_WHEN_NOT_MATCHED_BY_TARGET, ast.CMD_INSERT)
			mergeWhen.Override = ast.OVERRIDING_NOT_SET
			$$ = mergeWhen
		}
	;

merge_values_clause:
		VALUES '(' expr_list ')'
		{
			$$ = $3
		}
	;

/*
 * Common DML clauses
 */

/*
 * ON CONFLICT clause for INSERT statements
 * Based on postgres/src/backend/parser/gram.y:12219-12237
 */
opt_on_conflict:
			ON CONFLICT opt_conf_expr DO UPDATE SET set_clause_list where_clause
			{
				onConflict := ast.NewOnConflictClause(ast.ONCONFLICT_UPDATE)
				if $3 != nil {
					onConflict.Infer = $3.(*ast.InferClause)
				}
				onConflict.TargetList = $7
				onConflict.WhereClause = $8
				$$ = onConflict
			}
		|	ON CONFLICT opt_conf_expr DO NOTHING
			{
				onConflict := ast.NewOnConflictClause(ast.ONCONFLICT_NOTHING)
				if $3 != nil {
					onConflict.Infer = $3.(*ast.InferClause)
				}
				$$ = onConflict
			}
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

/*
 * Optional conflict expression for ON CONFLICT clause
 * Based on postgres/src/backend/parser/gram.y opt_conf_expr rule
 */
opt_conf_expr:
			'(' index_params ')' where_clause
			{
				// Create InferClause for column-based conflict detection
				infer := ast.NewInferClause()
				// Assign IndexElems directly as NodeList
				infer.IndexElems = $2
				infer.WhereClause = $4
				$$ = infer
			}
		|	ON CONSTRAINT name
			{
				// Create InferClause for constraint-based conflict detection
				infer := ast.NewInferClause()
				infer.Conname = $3
				$$ = infer
			}
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

index_params:
		index_elem
		{
			$$ = ast.NewNodeList($1)
		}
	|	index_params ',' index_elem
		{
			$1.Append($3); $$ = $1
		}
	;

index_elem:
		ColId index_elem_options
		{
			$$ = $2
			$$.(*ast.IndexElem).Name = $1
		}
	|	func_expr_windowless index_elem_options
		{
			$$ = $2
			$$.(*ast.IndexElem).Expr = $1
		}
	|	'(' a_expr ')' index_elem_options
		{
			$$ = $4
			$$.(*ast.IndexElem).Expr = $2
		}
	;

index_elem_options:
		opt_collate opt_qualified_name opt_asc_desc opt_nulls_order
		{
			indexElem := &ast.IndexElem{
				BaseNode: ast.BaseNode{Tag: ast.T_IndexElem},
			}
			indexElem.Collation = $1
			indexElem.Opclass = $2
			indexElem.Ordering = ast.SortByDir($3)
			indexElem.NullsOrdering = ast.SortByNulls($4)
			$$ = indexElem
		}
	|	opt_collate any_name reloptions opt_asc_desc opt_nulls_order
		{
			indexElem := &ast.IndexElem{
				BaseNode: ast.BaseNode{Tag: ast.T_IndexElem},
			}
			indexElem.Collation = $1
			indexElem.Opclass = $2
			indexElem.Opclassopts = $3
			indexElem.Ordering = ast.SortByDir($4)
			indexElem.NullsOrdering = ast.SortByNulls($5)
			$$ = indexElem
		}
	;

opt_collate:
		COLLATE any_name			{ $$ = $2 }
	|	/* EMPTY */					{ $$ = nil }
	;

opt_asc_desc:
		ASC							{ $$ = int(ast.SORTBY_ASC) }
	|	DESC						{ $$ = int(ast.SORTBY_DESC) }
	|	/* EMPTY */					{ $$ = int(ast.SORTBY_DEFAULT) }
	;

opt_nulls_order:
		NULLS_LA FIRST_P				{ $$ = int(ast.SORTBY_NULLS_FIRST) }
	|	NULLS_LA LAST_P				{ $$ = int(ast.SORTBY_NULLS_LAST) }
	|	/* EMPTY */					{ $$ = int(ast.SORTBY_NULLS_DEFAULT) }
	;

returning_clause:
			RETURNING target_list
			{
				$$ = $2
			}
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

/*****************************************************************************
 *
 * PHASE 3F: BASIC DDL - TABLES & INDEXES
 * Ported from postgres/src/backend/parser/gram.y
 *
 *****************************************************************************/

/*****************************************************************************
 *
 *		QUERY:
 *				CREATE TABLE relname
 *
 *****************************************************************************/

CreateStmt: CREATE OptTemp TABLE qualified_name '(' OptTableElementList ')'
			OptInherit OptPartitionSpec table_access_method_clause OptWith OnCommitOption OptTableSpace
				{
					rangeVar := $4
					rangeVar.RelPersistence = $2
					createStmt := ast.NewCreateStmt(rangeVar)
					createStmt.TableElts = $6
					createStmt.InhRelations = $8
					createStmt.PartSpec = $9
					createStmt.AccessMethod = $10
					createStmt.Options = $11
					createStmt.OnCommit = $12
					createStmt.TableSpaceName = $13
					$$ = createStmt
				}
		|	CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name '('
			OptTableElementList ')' OptInherit OptPartitionSpec table_access_method_clause
			OptWith OnCommitOption OptTableSpace
				{
					rangeVar := $7
					rangeVar.RelPersistence = $2
					createStmt := ast.NewCreateStmt(rangeVar)
					createStmt.TableElts = $9
					createStmt.InhRelations = $11
					createStmt.PartSpec = $12
					createStmt.AccessMethod = $13
					createStmt.Options = $14
					createStmt.OnCommit = $15
					createStmt.TableSpaceName = $16
					createStmt.IfNotExists = true
					$$ = createStmt
				}
		|	CREATE OptTemp TABLE qualified_name OF any_name
			OptTypedTableElementList OptPartitionSpec table_access_method_clause
			OptWith OnCommitOption OptTableSpace
				{
					rangeVar := $4
					rangeVar.RelPersistence = $2
					createStmt := ast.NewCreateStmt(rangeVar)
					createStmt.OfTypename = makeTypeNameFromNodeList($6)
					createStmt.TableElts = $7
					createStmt.PartSpec = $8
					createStmt.AccessMethod = $9
					createStmt.Options = $10
					createStmt.OnCommit = $11
					createStmt.TableSpaceName = $12
					$$ = createStmt
				}
		|	CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name OF any_name
			OptTypedTableElementList OptPartitionSpec table_access_method_clause
			OptWith OnCommitOption OptTableSpace
				{
					rangeVar := $7
					rangeVar.RelPersistence = $2
					createStmt := ast.NewCreateStmt(rangeVar)
					createStmt.OfTypename = makeTypeNameFromNodeList($9)
					createStmt.TableElts = $10
					createStmt.PartSpec = $11
					createStmt.AccessMethod = $12
					createStmt.Options = $13
					createStmt.OnCommit = $14
					createStmt.TableSpaceName = $15
					createStmt.IfNotExists = true
					$$ = createStmt
				}
		|	CREATE OptTemp TABLE qualified_name PARTITION OF qualified_name
			OptTypedTableElementList PartitionBoundSpec OptPartitionSpec table_access_method_clause
			OptWith OnCommitOption OptTableSpace
				{
					rangeVar := $4
					rangeVar.RelPersistence = $2
					createStmt := ast.NewCreateStmt(rangeVar)
					createStmt.InhRelations = ast.NewNodeList($7)
					createStmt.TableElts = $8
					createStmt.PartBound = $9
					createStmt.PartSpec = $10
					createStmt.AccessMethod = $11
					createStmt.Options = $12
					createStmt.OnCommit = $13
					createStmt.TableSpaceName = $14
					$$ = createStmt
				}
		|	CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name PARTITION OF qualified_name
			OptTypedTableElementList PartitionBoundSpec OptPartitionSpec table_access_method_clause
			OptWith OnCommitOption OptTableSpace
				{
					rangeVar := $7
					rangeVar.RelPersistence = $2
					createStmt := ast.NewCreateStmt(rangeVar)
					createStmt.InhRelations = ast.NewNodeList($10)
					createStmt.TableElts = $11
					createStmt.PartBound = $12
					createStmt.PartSpec = $13
					createStmt.AccessMethod = $14
					createStmt.Options = $15
					createStmt.OnCommit = $16
					createStmt.TableSpaceName = $17
					createStmt.IfNotExists = true
					$$ = createStmt
				}
		;

OptTemp:
			TEMPORARY						{ $$ = ast.RELPERSISTENCE_TEMP }
		|	TEMP						{ $$ = ast.RELPERSISTENCE_TEMP }
		|	LOCAL TEMPORARY					{ $$ = ast.RELPERSISTENCE_TEMP }
		|	LOCAL TEMP					{ $$ = ast.RELPERSISTENCE_TEMP }
		|	GLOBAL TEMPORARY				{ $$ = ast.RELPERSISTENCE_TEMP }
		|	GLOBAL TEMP					{ $$ = ast.RELPERSISTENCE_TEMP }
		|	UNLOGGED					{ $$ = ast.RELPERSISTENCE_UNLOGGED }
		|	/* EMPTY */					{ $$ = ast.RELPERSISTENCE_PERMANENT }
		;

OptTableElementList:
			TableElementList				{ $$ = $1 }
		|	/* EMPTY */					{ $$ = nil }
		;

TableElementList:
			TableElement
				{
					$$ = ast.NewNodeList()
					$$.Append($1)
				}
		|	TableElementList ',' TableElement
				{
					$1.Append($3)
					$$ = $1
				}
		;

TableElement:
			columnDef					{ $$ = $1 }
		|	TableLikeClause				{ $$ = $1 }
		|	TableConstraint				{ $$ = $1 }
		;

TableLikeClause:
			LIKE qualified_name TableLikeOptionList
				{
					$$ = ast.NewTableLikeClause($2, ast.TableLikeOption($3))
				}
		;

TableLikeOptionList:
			TableLikeOptionList INCLUDING TableLikeOption
				{
					$$ = $1 | $3
				}
		|	TableLikeOptionList EXCLUDING TableLikeOption
				{
					$$ = $1 & ^$3
				}
		|	/* EMPTY */
				{
					$$ = 0
				}
		;

TableLikeOption:
			COMMENTS					{ $$ = int(ast.CREATE_TABLE_LIKE_COMMENTS) }
		|	COMPRESSION					{ $$ = int(ast.CREATE_TABLE_LIKE_COMPRESSION) }
		|	CONSTRAINTS					{ $$ = int(ast.CREATE_TABLE_LIKE_CONSTRAINTS) }
		|	DEFAULTS					{ $$ = int(ast.CREATE_TABLE_LIKE_DEFAULTS) }
		|	IDENTITY_P					{ $$ = int(ast.CREATE_TABLE_LIKE_IDENTITY) }
		|	GENERATED					{ $$ = int(ast.CREATE_TABLE_LIKE_GENERATED) }
		|	INDEXES						{ $$ = int(ast.CREATE_TABLE_LIKE_INDEXES) }
		|	STATISTICS					{ $$ = int(ast.CREATE_TABLE_LIKE_STATISTICS) }
		|	STORAGE						{ $$ = int(ast.CREATE_TABLE_LIKE_STORAGE) }
		|	ALL							{ $$ = int(ast.CREATE_TABLE_LIKE_ALL) }
		;

/* Column storage and compression options */
column_compression:
			COMPRESSION ColId				{ $$ = $2 }
		|	COMPRESSION DEFAULT				{ $$ = "default" }
		;

opt_column_compression:
			column_compression				{ $$ = $1 }
		|	/* EMPTY */						{ $$ = "" }
		;

column_storage:
			STORAGE ColId					{ $$ = $2 }
		|	STORAGE DEFAULT					{ $$ = "default" }
		;

opt_column_storage:
			column_storage					{ $$ = $1 }
		|	/* EMPTY */						{ $$ = "" }
		;

/* Typed table elements - for CREATE TABLE ... OF typename */
OptTypedTableElementList:
			'(' TypedTableElementList ')'		{ $$ = $2 }
		|	/* EMPTY */						{ $$ = nil }
		;

TypedTableElementList:
			TypedTableElement
				{
					list := ast.NewNodeList()
					list.Append($1)
					$$ = list
				}
		|	TypedTableElementList ',' TypedTableElement
				{
					$1.Append($3)
					$$ = $1
				}
		;

TypedTableElement:
			columnOptions					{ $$ = $1 }
		|	TableConstraint					{ $$ = $1 }
		;

columnOptions:
			ColId ColQualList
				{
					colDef := ast.NewColumnDef($1, nil, 0)
					colDef.Constraints = $2
					$$ = colDef
				}
		|	ColId WITH OPTIONS ColQualList
				{
					colDef := ast.NewColumnDef($1, nil, 0)
					colDef.Constraints = $4
					$$ = colDef
				}
		;

/* Partition boundary specifications - for CREATE TABLE ... PARTITION OF */
PartitionBoundSpec:
			/* a HASH partition */
			FOR VALUES WITH '(' hash_partbound ')'
				{
					hashSpec := ast.NewPartitionBoundSpec(ast.PARTITION_STRATEGY_HASH)
					hashSpec.IsDefault = false

					// Parse hash partition bounds (modulus and remainder)
					hashOptions := $5
					for _, optNode := range hashOptions.Items {
						if defElem, ok := optNode.(*ast.DefElem); ok {
							if defElem.Defname == "modulus" {
								if intVal, ok := defElem.Arg.(*ast.A_Const); ok && intVal.Isnull == false {
									if integerVal, ok := intVal.Val.(*ast.Integer); ok {
										hashSpec.Modulus = integerVal.IVal
									}
								}
							} else if defElem.Defname == "remainder" {
								if intVal, ok := defElem.Arg.(*ast.A_Const); ok && intVal.Isnull == false {
									if integerVal, ok := intVal.Val.(*ast.Integer); ok {
										hashSpec.Remainder = integerVal.IVal
									}
								}
							}
						}
					}

					$$ = hashSpec
				}

			/* a LIST partition */
		|	FOR VALUES IN_P '(' expr_list ')'
				{
					listSpec := ast.NewPartitionBoundSpec(ast.PARTITION_STRATEGY_LIST)
					listSpec.IsDefault = false
					listSpec.ListDatums = $5
					$$ = listSpec
				}

			/* a RANGE partition */
		|	FOR VALUES FROM '(' expr_list ')' TO '(' expr_list ')'
				{
					rangeSpec := ast.NewPartitionBoundSpec(ast.PARTITION_STRATEGY_RANGE)
					rangeSpec.IsDefault = false
					rangeSpec.LowDatums = $5
					rangeSpec.HighDatums = $9
					$$ = rangeSpec
				}

			/* a DEFAULT partition */
		|	DEFAULT
				{
					defaultSpec := ast.NewPartitionBoundSpec(ast.PARTITION_STRATEGY_LIST)
					defaultSpec.IsDefault = true
					$$ = defaultSpec
				}
		;

hash_partbound_elem:
		NonReservedWord Iconst
			{
				$$ = ast.NewDefElem($1, ast.NewA_Const(ast.NewInteger($2), 0))
			}
		;

hash_partbound:
		hash_partbound_elem
			{
				$$ = ast.NewNodeList($1)
			}
	|	hash_partbound ',' hash_partbound_elem
			{
				nodeList := $1
				nodeList.Append($3)
				$$ = nodeList
			}
		;

create_generic_options:
			OPTIONS '(' generic_option_list ')'	{ $$ = $3 }
		|	/* EMPTY */							{ $$ = nil }
		;

generic_option_list:
			generic_option_elem
				{
					$$ = ast.NewNodeList($1)
				}
		|	generic_option_list ',' generic_option_elem
				{
					$1.Append($3)
					$$ = $1
				}
		;

generic_option_elem:
			generic_option_name generic_option_arg
				{
					$$ = ast.NewDefElem($1, $2)
				}
		;

generic_option_name:
			ColLabel			{ $$ = $1 }
		;

/* We could use def_arg here, but the spec only requires string literals */
generic_option_arg:
			Sconst							{ $$ = ast.NewString($1) }
		;

columnDef:
			ColId Typename opt_column_storage opt_column_compression create_generic_options ColQualList
				{
					colDef := ast.NewColumnDef($1, $2, 0)
					colDef.StorageName = $3
					colDef.Compression = $4
					colDef.Fdwoptions = $5
					// Use SplitColQualList to separate constraints and collate clause
					constraints, collClause := SplitColQualList($6)
					colDef.Constraints = constraints
					colDef.Collclause = collClause
					$$ = colDef
				}
		;

ColQualList:
			ColQualList ColConstraint
				{
					$1.Append($2)
					$$ = $1
				}
		|	/* EMPTY */
				{
					$$ = ast.NewNodeList()
				}
		;

ColConstraint:
			CONSTRAINT name ColConstraintElem
				{
					constraint := $3.(*ast.Constraint)
					constraint.Conname = $2
					$$ = constraint
				}
		|	ColConstraintElem			{ $$ = $1 }
		|	ConstraintAttr				{ $$ = $1 }
		|	COLLATE any_name
				{
					/*
					 * Note: the CollateClause is momentarily included in
					 * the list built by ColQualList, but we split it out
					 * again in SplitColQualList.
					 */
					collateClause := ast.NewCollateClause($2)
					$$ = collateClause
				}
		;

ConstraintAttr:
		DEFERRABLE
			{
				constraint := ast.NewConstraint(ast.CONSTR_ATTR_DEFERRABLE)
				$$ = constraint
			}
	|	NOT DEFERRABLE
			{
				constraint := ast.NewConstraint(ast.CONSTR_ATTR_NOT_DEFERRABLE)
				$$ = constraint
			}
	|	INITIALLY DEFERRED
			{
				constraint := ast.NewConstraint(ast.CONSTR_ATTR_DEFERRED)
				$$ = constraint
			}
	|	INITIALLY IMMEDIATE
			{
				constraint := ast.NewConstraint(ast.CONSTR_ATTR_IMMEDIATE)
				$$ = constraint
			}
	;

/* Supporting rules for constraints */
opt_unique_null_treatment:
			NULLS_P DISTINCT				{ $$ = true }
		|	NULLS_P NOT DISTINCT			{ $$ = false }
		|	/* EMPTY */						{ $$ = true }
		;

generated_when:
			ALWAYS							{ $$ = ast.ATTRIBUTE_IDENTITY_ALWAYS }
		|	BY DEFAULT						{ $$ = ast.ATTRIBUTE_IDENTITY_BY_DEFAULT }
		;

alter_identity_column_option_list:
			alter_identity_column_option
				{ $$ = ast.NewNodeList($1) }
		|	alter_identity_column_option_list alter_identity_column_option
				{ $1.Append($2); $$ = $1 }
		;

alter_identity_column_option:
			RESTART
				{
					$$ = ast.NewDefElem("restart", nil)
				}
		|	RESTART opt_with NumericOnly
				{
					$$ = ast.NewDefElem("restart", $3)
				}
		|	SET SeqOptElem
				{
					// SeqOptElem already returns a DefElem, so we can use it directly
					// Check for invalid options as per PostgreSQL
					defElem := $2
					if defElem.Defname == "as" || defElem.Defname == "restart" || defElem.Defname == "owned_by" {
						yylex.Error("sequence option not supported here")
					}
					$$ = $2
				}
		|	SET GENERATED generated_when
				{
					$$ = ast.NewDefElem("generated", ast.NewInteger(int($3)))
				}
		;

SeqOptList:
			SeqOptElem						{ $$ = ast.NewNodeList($1) }
		|	SeqOptList SeqOptElem			{ $1.Append($2); $$ = $1 }
		;

SeqOptElem:
			AS SimpleTypename
				{
					$$ = ast.NewDefElem("as", $2)
				}
		|	CACHE NumericOnly
				{
					$$ = ast.NewDefElem("cache", $2)
				}
		|	CYCLE
				{
					$$ = ast.NewDefElem("cycle", ast.NewBoolean(true))
				}
		|	NO CYCLE
				{
					$$ = ast.NewDefElem("cycle", ast.NewBoolean(false))
				}
		|	INCREMENT opt_by NumericOnly
				{
					$$ = ast.NewDefElem("increment", $3)
				}
		|	LOGGED
				{
					$$ = ast.NewDefElem("logged", nil)
				}
		|	MAXVALUE NumericOnly
				{
					$$ = ast.NewDefElem("maxvalue", $2)
				}
		|	MINVALUE NumericOnly
				{
					$$ = ast.NewDefElem("minvalue", $2)
				}
		|	NO MAXVALUE
				{
					$$ = ast.NewDefElem("maxvalue", nil)
				}
		|	NO MINVALUE
				{
					$$ = ast.NewDefElem("minvalue", nil)
				}
		|	OWNED BY any_name
				{
					$$ = ast.NewDefElem("owned_by", $3)
				}
		|	SEQUENCE NAME_P any_name
				{
					$$ = ast.NewDefElem("sequence_name", $3)
				}
		|	START opt_with NumericOnly
				{
					$$ = ast.NewDefElem("start", $3)
				}
		|	RESTART
				{
					$$ = ast.NewDefElem("restart", nil)
				}
		|	RESTART opt_with NumericOnly
				{
					$$ = ast.NewDefElem("restart", $3)
				}
		|	UNLOGGED
				{
					$$ = ast.NewDefElem("unlogged", nil)
				}
		;

opt_by:
		BY			{ }
	|	/* EMPTY */	{ }
	;

opt_with:
		WITH		{ }
	|	WITH_LA		{ }
	|	/* EMPTY */	{ }
	;

opt_recheck:
		RECHECK
			{
				// RECHECK no longer does anything in opclass definitions,
				// but we still accept it to ease porting of old database dumps.
				// When this is used, we should emit a notice.
				// For now, just return true to indicate RECHECK was present
				$$ = true
			}
	|	/* EMPTY */	{ $$ = false }
	;

OptParenthesizedSeqOptList:
			'(' SeqOptList ')'				{ $$ = $2 }
		|	/* EMPTY */						{ $$ = nil }
		;

ColConstraintElem:
			NOT NULL_P
				{
					$$ = ast.NewConstraint(ast.CONSTR_NOTNULL)
				}
		|	NULL_P
				{
					$$ = ast.NewConstraint(ast.CONSTR_NULL)
				}
		|	UNIQUE opt_unique_null_treatment opt_definition OptConsTableSpace
				{
					constraint := ast.NewConstraint(ast.CONSTR_UNIQUE)
					constraint.Keys = nil // Will be filled by the parser
					constraint.NullsNotDistinct = !$2
					constraint.Options = $3
					constraint.Indexspace = $4
					$$ = constraint
				}
		|	PRIMARY KEY opt_definition OptConsTableSpace
				{
					constraint := ast.NewConstraint(ast.CONSTR_PRIMARY)
					constraint.Keys = nil // Will be filled by the parser
					$$ = constraint
				}
		|	CHECK '(' a_expr ')' opt_no_inherit
				{
					constraint := ast.NewConstraint(ast.CONSTR_CHECK)
					constraint.RawExpr = $3
					$$ = constraint
				}
		|	DEFAULT b_expr
				{
					constraint := ast.NewConstraint(ast.CONSTR_DEFAULT)
					constraint.RawExpr = $2
					$$ = constraint
				}
		|	REFERENCES qualified_name opt_column_list key_match key_actions
				{
					constraint := ast.NewConstraint(ast.CONSTR_FOREIGN)
					constraint.Pktable = $2
					constraint.PkAttrs = $3
					constraint.FkMatchtype = $4
					if actions := $5; actions != nil {
						constraint.FkUpdAction = actions.UpdateAction.Action
						constraint.FkDelAction = actions.DeleteAction.Action
						// Copy column list for SET NULL/SET DEFAULT on DELETE
						if actions.DeleteAction.Action == ast.FKCONSTR_ACTION_SETNULL ||
						   actions.DeleteAction.Action == ast.FKCONSTR_ACTION_SETDEFAULT {
							constraint.FkDelSetCols = actions.DeleteAction.Cols
						}
					}
					$$ = constraint
				}
		|	GENERATED generated_when AS IDENTITY_P OptParenthesizedSeqOptList
				{
					constraint := ast.NewConstraint(ast.CONSTR_IDENTITY)
					constraint.GeneratedWhen = $2
					constraint.Options = $5
					$$ = constraint
				}
		|	GENERATED generated_when AS '(' a_expr ')' STORED
				{
					constraint := ast.NewConstraint(ast.CONSTR_GENERATED)
					constraint.GeneratedWhen = $2
					constraint.RawExpr = $5
					$$ = constraint
				}
		;

TableConstraint:
			CONSTRAINT name ConstraintElem
				{
					constraint := $3.(*ast.Constraint)
					constraint.Conname = $2
					$$ = constraint
				}
		|	ConstraintElem				{ $$ = $1 }
		;

ConstraintElem:
			CHECK '(' a_expr ')' ConstraintAttributeSpec
				{
					constraint := ast.NewConstraint(ast.CONSTR_CHECK)
					constraint.RawExpr = $3
					processConstraintAttributeSpec($5, constraint)
					$$ = constraint
				}
		|	UNIQUE opt_unique_null_treatment '(' columnList ')' opt_c_include opt_definition OptConsTableSpace
			ConstraintAttributeSpec
				{
					constraint := ast.NewConstraint(ast.CONSTR_UNIQUE)
					constraint.NullsNotDistinct = !$2
					constraint.Keys = $4
					constraint.Including = $6
					constraint.Options = $7
					constraint.Indexspace = $8
					processConstraintAttributeSpec($9, constraint)
					$$ = constraint
				}
		|	UNIQUE ExistingIndex ConstraintAttributeSpec
				{
					constraint := ast.NewConstraint(ast.CONSTR_UNIQUE)
					constraint.Indexname = $2
					// Clear lists for existing index
					constraint.Keys = nil
					constraint.Including = nil
					processConstraintAttributeSpec($3, constraint)
					$$ = constraint
				}
		|	PRIMARY KEY '(' columnList ')' opt_c_include opt_definition OptConsTableSpace
			ConstraintAttributeSpec
				{
					constraint := ast.NewConstraint(ast.CONSTR_PRIMARY)
					constraint.Keys = $4
					constraint.Including = $6
					constraint.Options = $7
					constraint.Indexspace = $8
					processConstraintAttributeSpec($9, constraint)
					$$ = constraint
				}
		|	PRIMARY KEY ExistingIndex ConstraintAttributeSpec
				{
					constraint := ast.NewConstraint(ast.CONSTR_PRIMARY)
					constraint.Indexname = $3
					// Clear lists for existing index
					constraint.Keys = nil
					constraint.Including = nil
					processConstraintAttributeSpec($4, constraint)
					$$ = constraint
				}
		|	FOREIGN KEY '(' columnList ')' REFERENCES qualified_name
			opt_column_list key_match key_actions ConstraintAttributeSpec
				{
					constraint := ast.NewConstraint(ast.CONSTR_FOREIGN)
					constraint.FkAttrs = $4
					constraint.Pktable = $7
					constraint.PkAttrs = $8
					constraint.FkMatchtype = $9
					if actions := $10; actions != nil {
						constraint.FkUpdAction = actions.UpdateAction.Action
						constraint.FkDelAction = actions.DeleteAction.Action
						// Copy column list for SET NULL/SET DEFAULT on DELETE
						if actions.DeleteAction.Action == ast.FKCONSTR_ACTION_SETNULL ||
						   actions.DeleteAction.Action == ast.FKCONSTR_ACTION_SETDEFAULT {
							constraint.FkDelSetCols = actions.DeleteAction.Cols
						}
					}
					processConstraintAttributeSpec($11, constraint)
					$$ = constraint
				}
		|	EXCLUDE access_method_clause '(' ExclusionConstraintList ')'
			opt_c_include opt_definition OptConsTableSpace where_clause
			ConstraintAttributeSpec
				{
					constraint := ast.NewConstraint(ast.CONSTR_EXCLUSION)
					constraint.AccessMethod = $2
					constraint.Exclusions = $4
					constraint.Including = $6
					constraint.Options = $7
					constraint.Indexspace = $8
					constraint.WhereClause = $9
					processConstraintAttributeSpec($10, constraint)
					$$ = constraint
				}
		;

opt_no_inherit:
			NO INHERIT					{ $$ = true }
		|	/* EMPTY */					{ $$ = false }
		;

/* EXCLUSION constraint support */
ExclusionConstraintList:
			ExclusionConstraintElem
				{
					list := ast.NewNodeList()
					list.Append($1)
					$$ = list
				}
		|	ExclusionConstraintList ',' ExclusionConstraintElem
				{
					$1.Append($3)
					$$ = $1
				}
		;

ExclusionConstraintElem:
			index_elem WITH any_operator
				{
					// Create a NodeList with index_elem and operator (matching PostgreSQL's list_make2 approach)
					$$ = ast.NewNodeList($1, $3)
				}
		|	index_elem WITH OPERATOR '(' any_operator ')'
				{
					// Create a NodeList with index_elem and operator (matching PostgreSQL's list_make2 approach)
					$$ = ast.NewNodeList($1, $5)
				}
		;

opt_c_include:
			INCLUDE '(' columnList ')'
				{
					$$ = $3
				}
		|	/* EMPTY */
				{
					$$ = nil
				}
		;

key_match:
			MATCH FULL
				{
					$$ = ast.FKCONSTR_MATCH_FULL
				}
		|	MATCH PARTIAL
				{
					$$ = ast.FKCONSTR_MATCH_PARTIAL
				}
		|	MATCH SIMPLE
				{
					$$ = ast.FKCONSTR_MATCH_SIMPLE
				}
		|	/* EMPTY */
				{
					$$ = ast.FKCONSTR_MATCH_SIMPLE
				}
		;

key_actions:
			key_update
				{
					n := &ast.KeyActions{}
					n.UpdateAction = $1
					n.DeleteAction = &ast.KeyAction{
						Action: ast.FKCONSTR_ACTION_NOACTION,
						Cols:   nil,
					}
					$$ = n
				}
		|	key_delete
				{
					n := &ast.KeyActions{}
					n.UpdateAction = &ast.KeyAction{
						Action: ast.FKCONSTR_ACTION_NOACTION,
						Cols:   nil,
					}
					n.DeleteAction = $1
					$$ = n
				}
		|	key_update key_delete
				{
					n := &ast.KeyActions{}
					n.UpdateAction = $1
					n.DeleteAction = $2
					$$ = n
				}
		|	key_delete key_update
				{
					n := &ast.KeyActions{}
					n.UpdateAction = $2
					n.DeleteAction = $1
					$$ = n
				}
		|	/* EMPTY */
				{
					n := &ast.KeyActions{}
					n.UpdateAction = &ast.KeyAction{
						Action: ast.FKCONSTR_ACTION_NOACTION,
						Cols:   nil,
					}
					n.DeleteAction = &ast.KeyAction{
						Action: ast.FKCONSTR_ACTION_NOACTION,
						Cols:   nil,
					}
					$$ = n
				}
		;

key_update: ON UPDATE key_action
			{
				// Check for unsupported column lists on UPDATE actions
				keyAction := $3
				if keyAction.Cols != nil {
					if len(keyAction.Cols.Items) > 0 {
						yylex.Error("column list with SET NULL/SET DEFAULT is only supported for ON DELETE actions")
					}
				}
				$$ = keyAction
			}
		;

key_delete: ON DELETE_P key_action
			{
				$$ = $3
			}
		;

key_action:
			NO ACTION
				{
					n := &ast.KeyAction{}
					n.Action = ast.FKCONSTR_ACTION_NOACTION
					n.Cols = nil
					$$ = n
				}
		|	RESTRICT
				{
					n := &ast.KeyAction{}
					n.Action = ast.FKCONSTR_ACTION_RESTRICT
					n.Cols = nil
					$$ = n
				}
		|	CASCADE
				{
					n := &ast.KeyAction{}
					n.Action = ast.FKCONSTR_ACTION_CASCADE
					n.Cols = nil
					$$ = n
				}
		|	SET NULL_P opt_column_list
				{
					n := &ast.KeyAction{}
					n.Action = ast.FKCONSTR_ACTION_SETNULL
					n.Cols = $3
					$$ = n
				}
		|	SET DEFAULT opt_column_list
				{
					n := &ast.KeyAction{}
					n.Action = ast.FKCONSTR_ACTION_SETDEFAULT
					n.Cols = $3
					$$ = n
				}
		;

OptInherit:
			INHERITS '(' qualified_name_list ')'	{ $$ = $3 }
		|	/* EMPTY */							{ $$ = nil }
		;

OptPartitionSpec:
			PartitionSpec				{ $$ = $1 }
		|	/* EMPTY */					{ $$ = nil }
		;

PartitionSpec:
			PARTITION BY ColId '(' part_params ')'
				{
					partitionSpec := ast.NewPartitionSpec(ast.PartitionStrategy($3), $5)
					$$ = partitionSpec
				}
		;

part_params:
			part_elem
				{
					$$ = ast.NewNodeList()
					$$.Append($1)
				}
		|	part_params ',' part_elem
				{
					$1.Append($3)
					$$ = $1
				}
		;

part_elem:
			ColId opt_collate opt_qualified_name
				{
					partElem := ast.NewPartitionElem($1, nil, 0)
					partElem.Collation = $2
					partElem.Opclass = $3
					$$ = partElem
				}
		|	func_expr_windowless opt_collate opt_qualified_name
				{
					partElem := ast.NewPartitionElem("", $1, 0)
					partElem.Collation = $2
					partElem.Opclass = $3
					$$ = partElem
				}
		|	'(' a_expr ')' opt_collate opt_qualified_name
				{
					// Wrap the expression in ParenExpr to preserve parentheses
					parenExpr := ast.NewParenExpr($2, 0)
					partElem := ast.NewPartitionElem("", parenExpr, 0)
					partElem.Collation = $4
					partElem.Opclass = $5
					$$ = partElem
				}
		;

table_access_method_clause:
			USING name					{ $$ = $2 }
		|	/* EMPTY */					{ $$ = "" }
		;

OnCommitOption:
			ON COMMIT DROP				{ $$ = ast.ONCOMMIT_DROP }
		|	ON COMMIT DELETE_P ROWS		{ $$ = ast.ONCOMMIT_DELETE_ROWS }
		|	ON COMMIT PRESERVE ROWS		{ $$ = ast.ONCOMMIT_PRESERVE_ROWS }
		|	/* EMPTY */					{ $$ = ast.ONCOMMIT_NOOP }
		;

OptTableSpace:
			TABLESPACE name				{ $$ = $2 }
		|	/* EMPTY */					{ $$ = "" }
		;

OptConsTableSpace:
			USING INDEX TABLESPACE name	{ $$ = $4 }
		|	/* EMPTY */					{ $$ = "" }
		;

ExistingIndex:
			USING INDEX name			{ $$ = $3 }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				CREATE INDEX
 *
 *****************************************************************************/

IndexStmt:	CREATE opt_unique INDEX opt_concurrently opt_single_name
			ON relation_expr access_method_clause '(' index_params ')'
			opt_include opt_unique_null_treatment opt_reloptions OptTableSpace where_clause
				{
					indexStmt := ast.NewIndexStmt($5, $7, $10)
					indexStmt.Unique = $2
					indexStmt.Concurrent = $4
					indexStmt.AccessMethod = $8
					indexStmt.IndexIncludingParams = $12
					indexStmt.NullsNotDistinct = !$13
					indexStmt.Options = $14
					indexStmt.TableSpace = $15
					indexStmt.WhereClause = $16
					$$ = indexStmt
				}
		|	CREATE opt_unique INDEX opt_concurrently IF_P NOT EXISTS name
			ON relation_expr access_method_clause '(' index_params ')'
			opt_include opt_unique_null_treatment opt_reloptions OptTableSpace where_clause
				{
					indexStmt := ast.NewIndexStmt($8, $10, $13)
					indexStmt.Unique = $2
					indexStmt.Concurrent = $4
					indexStmt.IfNotExists = true
					indexStmt.AccessMethod = $11
					indexStmt.IndexIncludingParams = $15
					indexStmt.NullsNotDistinct = !$16
					indexStmt.Options = $17
					indexStmt.TableSpace = $18
					indexStmt.WhereClause = $19
					$$ = indexStmt
				}
		;

opt_unique:
			UNIQUE						{ $$ = true }
		|	/* EMPTY */					{ $$ = false }
		;

access_method_clause:
			USING name					{ $$ = $2 }
		|	/* EMPTY */					{ $$ = "btree" }
		;

opt_include:
			INCLUDE '(' index_including_params ')'	{ $$ = $3 }
		|	/* EMPTY */							{ $$ = nil }
		;

index_including_params:
			index_elem
				{
					$$ = ast.NewNodeList()
					$$.Append($1)
				}
		|	index_including_params ',' index_elem
				{
					$1.Append($3)
					$$ = $1
				}
		;


opt_reloptions:
			WITH reloptions				{ $$ = $2 }
		|	/* EMPTY */					{ $$ = nil }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				ALTER TABLE
 *
 *****************************************************************************/

AlterTableStmt:
			ALTER TABLE relation_expr alter_table_cmds
				{
					alterStmt := ast.NewAlterTableStmt($3, $4)
					alterStmt.Objtype = ast.OBJECT_TABLE
					$$ = alterStmt
				}
		|	ALTER TABLE IF_P EXISTS relation_expr alter_table_cmds
				{
					alterStmt := ast.NewAlterTableStmt($5, $6)
					alterStmt.Objtype = ast.OBJECT_TABLE
					alterStmt.MissingOk = true
					$$ = alterStmt
				}
		|	ALTER INDEX qualified_name alter_table_cmds
				{
					alterStmt := ast.NewAlterTableStmt($3, $4)
					alterStmt.Objtype = ast.OBJECT_INDEX
					$$ = alterStmt
				}
		|	ALTER INDEX IF_P EXISTS qualified_name alter_table_cmds
				{
					alterStmt := ast.NewAlterTableStmt($5, $6)
					alterStmt.Objtype = ast.OBJECT_INDEX
					alterStmt.MissingOk = true
					$$ = alterStmt
				}
		|	ALTER INDEX qualified_name index_partition_cmd
				{
					// Index partition attachment - dedicated rule
					cmdList := ast.NewNodeList()
					cmdList.Append($4.(*ast.AlterTableCmd))
					alterStmt := ast.NewAlterTableStmt($3, cmdList)
					alterStmt.Objtype = ast.OBJECT_INDEX
					$$ = alterStmt
				}
		|	ALTER SEQUENCE qualified_name alter_table_cmds
				{
					alterStmt := ast.NewAlterTableStmt($3, $4)
					alterStmt.Objtype = ast.OBJECT_SEQUENCE
					$$ = alterStmt
				}
		|	ALTER SEQUENCE IF_P EXISTS qualified_name alter_table_cmds
				{
					alterStmt := ast.NewAlterTableStmt($5, $6)
					alterStmt.Objtype = ast.OBJECT_SEQUENCE
					alterStmt.MissingOk = true
					$$ = alterStmt
				}
		|	ALTER VIEW qualified_name alter_table_cmds
				{
					alterStmt := ast.NewAlterTableStmt($3, $4)
					alterStmt.Objtype = ast.OBJECT_VIEW
					$$ = alterStmt
				}
		|	ALTER VIEW IF_P EXISTS qualified_name alter_table_cmds
				{
					alterStmt := ast.NewAlterTableStmt($5, $6)
					alterStmt.Objtype = ast.OBJECT_VIEW
					alterStmt.MissingOk = true
					$$ = alterStmt
				}
		|	ALTER MATERIALIZED VIEW qualified_name alter_table_cmds
				{
					alterStmt := ast.NewAlterTableStmt($4, $5)
					alterStmt.Objtype = ast.OBJECT_MATVIEW
					$$ = alterStmt
				}
		|	ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name alter_table_cmds
				{
					alterStmt := ast.NewAlterTableStmt($6, $7)
					alterStmt.Objtype = ast.OBJECT_MATVIEW
					alterStmt.MissingOk = true
					$$ = alterStmt
				}
		|	ALTER FOREIGN TABLE relation_expr alter_table_cmds
				{
					alterStmt := ast.NewAlterTableStmt($4, $5)
					alterStmt.Objtype = ast.OBJECT_FOREIGN_TABLE
					$$ = alterStmt
				}
		|	ALTER FOREIGN TABLE IF_P EXISTS relation_expr alter_table_cmds
				{
					alterStmt := ast.NewAlterTableStmt($6, $7)
					alterStmt.Objtype = ast.OBJECT_FOREIGN_TABLE
					alterStmt.MissingOk = true
					$$ = alterStmt
				}
		|	ALTER TABLE relation_expr partition_cmd
				{
					// Partition commands - dedicated rule for partition-only operations
					cmdList := ast.NewNodeList()
					cmdList.Append($4.(*ast.AlterTableCmd))
					alterStmt := ast.NewAlterTableStmt($3, cmdList)
					alterStmt.Objtype = ast.OBJECT_TABLE
					$$ = alterStmt
				}
		|	ALTER TABLE IF_P EXISTS relation_expr partition_cmd
				{
					// Partition commands with IF EXISTS
					cmdList := ast.NewNodeList()
					cmdList.Append($6.(*ast.AlterTableCmd))
					alterStmt := ast.NewAlterTableStmt($5, cmdList)
					alterStmt.Objtype = ast.OBJECT_TABLE
					alterStmt.MissingOk = true
					$$ = alterStmt
				}
		|	ALTER TABLE ALL IN_P TABLESPACE name SET TABLESPACE name opt_nowait
				{
					// Bulk tablespace move for tables
					moveStmt := ast.NewAlterTableMoveAllStmt($6, ast.OBJECT_TABLE, $9)
					moveStmt.Nowait = $10
					$$ = moveStmt
				}
		|	ALTER TABLE ALL IN_P TABLESPACE name OWNED BY role_list SET TABLESPACE name opt_nowait
				{
					// Bulk tablespace move for tables owned by specific roles
					moveStmt := ast.NewAlterTableMoveAllStmt($6, ast.OBJECT_TABLE, $12)
					moveStmt.Roles = $9
					moveStmt.Nowait = $13
					$$ = moveStmt
				}
		|	ALTER INDEX ALL IN_P TABLESPACE name SET TABLESPACE name opt_nowait
				{
					// Bulk tablespace move for indexes
					moveStmt := ast.NewAlterTableMoveAllStmt($6, ast.OBJECT_INDEX, $9)
					moveStmt.Nowait = $10
					$$ = moveStmt
				}
		|	ALTER INDEX ALL IN_P TABLESPACE name OWNED BY role_list SET TABLESPACE name opt_nowait
				{
					// Bulk tablespace move for indexes owned by specific roles
					moveStmt := ast.NewAlterTableMoveAllStmt($6, ast.OBJECT_INDEX, $12)
					moveStmt.Roles = $9
					moveStmt.Nowait = $13
					$$ = moveStmt
				}
		|	ALTER MATERIALIZED VIEW ALL IN_P TABLESPACE name SET TABLESPACE name opt_nowait
				{
					// Bulk tablespace move for materialized views
					moveStmt := ast.NewAlterTableMoveAllStmt($7, ast.OBJECT_MATVIEW, $10)
					moveStmt.Nowait = $11
					$$ = moveStmt
				}
		|	ALTER MATERIALIZED VIEW ALL IN_P TABLESPACE name OWNED BY role_list SET TABLESPACE name opt_nowait
				{
					// Bulk tablespace move for materialized views owned by specific roles
					moveStmt := ast.NewAlterTableMoveAllStmt($7, ast.OBJECT_MATVIEW, $13)
					moveStmt.Roles = $10
					moveStmt.Nowait = $14
					$$ = moveStmt
				}
		;

alter_table_cmds:
			alter_table_cmd
				{
					$$ = ast.NewNodeList()
					$$.Append($1)
				}
		|	alter_table_cmds ',' alter_table_cmd
				{
					$1.Append($3)
					$$ = $1
				}
		;

/* Optional NOWAIT - for ALTER TABLE */
opt_nowait:
			NOWAIT				{ $$ = true }
		|	/* EMPTY */			{ $$ = false }
		;

role_list:
			RoleSpec
				{
					$$ = ast.NewNodeList()
					$$.Append($1)
				}
		|	role_list ',' RoleSpec
				{
					$1.Append($3)
					$$ = $1
				}
		;

RoleId:
			RoleSpec
				{
					roleSpec := $1

					switch roleSpec.Roletype {
					case ast.ROLESPEC_CSTRING:
						$$ = roleSpec.Rolename
					case ast.ROLESPEC_PUBLIC:
						// PostgreSQL throws an error for "public" role name
						yylex.Error(`role name "public" is reserved`)
						return 1
					case ast.ROLESPEC_SESSION_USER:
						// PostgreSQL throws an error: SESSION_USER cannot be used as a role name here
						yylex.Error(`SESSION_USER cannot be used as a role name here`)
						return 1
					case ast.ROLESPEC_CURRENT_USER:
						// PostgreSQL throws an error: CURRENT_USER cannot be used as a role name here
						yylex.Error(`CURRENT_USER cannot be used as a role name here`)
						return 1
					case ast.ROLESPEC_CURRENT_ROLE:
						// PostgreSQL throws an error: CURRENT_ROLE cannot be used as a role name here
						yylex.Error(`CURRENT_ROLE cannot be used as a role name here`)
						return 1
					default:
						yylex.Error(`invalid role specification`)
						return 1
					}
				}
		;

RoleSpec:
			NonReservedWord
				{
					// Handle special role names: "public" and "none"
					var roleSpec *ast.RoleSpec

					if $1 == "public" {
						roleSpec = &ast.RoleSpec{
							BaseNode: ast.BaseNode{Tag: ast.T_RoleSpec},
							Roletype: ast.ROLESPEC_PUBLIC,
						}
					} else if $1 == "none" {
						// PostgreSQL throws an error for "none" - role name "none" is reserved
						yylex.Error(`role name "none" is reserved`)
						return 1
					} else {
						roleSpec = &ast.RoleSpec{
							BaseNode: ast.BaseNode{Tag: ast.T_RoleSpec},
							Roletype: ast.ROLESPEC_CSTRING,
							Rolename: $1,
						}
					}
					$$ = roleSpec
				}
		|	CURRENT_ROLE
				{
					$$ = &ast.RoleSpec{
						BaseNode: ast.BaseNode{Tag: ast.T_RoleSpec},
						Roletype: ast.ROLESPEC_CURRENT_ROLE,
					}
				}
		|	CURRENT_USER
				{
					$$ = &ast.RoleSpec{
						BaseNode: ast.BaseNode{Tag: ast.T_RoleSpec},
						Roletype: ast.ROLESPEC_CURRENT_USER,
					}
				}
		|	SESSION_USER
				{
					$$ = &ast.RoleSpec{
						BaseNode: ast.BaseNode{Tag: ast.T_RoleSpec},
						Roletype: ast.ROLESPEC_SESSION_USER,
					}
				}
		;

alter_table_cmd:
			/* ALTER TABLE <name> ADD <coldef> */
			ADD_P columnDef
				{
					cmd := ast.NewAlterTableCmd(ast.AT_AddColumn, "", $2)
					cmd.MissingOk = false
					$$ = cmd
				}
			/* ALTER TABLE <name> ADD IF NOT EXISTS <coldef> */
		|	ADD_P IF_P NOT EXISTS columnDef
				{
					cmd := ast.NewAlterTableCmd(ast.AT_AddColumn, "", $5)
					cmd.MissingOk = true
					$$ = cmd
				}
			/* ALTER TABLE <name> ADD COLUMN <coldef> */
		|	ADD_P COLUMN columnDef
				{
					cmd := ast.NewAlterTableCmd(ast.AT_AddColumn, "", $3)
					cmd.MissingOk = false
					$$ = cmd
				}
			/* ALTER TABLE <name> ADD COLUMN IF NOT EXISTS <coldef> */
		|	ADD_P COLUMN IF_P NOT EXISTS columnDef
				{
					cmd := ast.NewAlterTableCmd(ast.AT_AddColumn, "", $6)
					cmd.MissingOk = true
					$$ = cmd
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> {SET DEFAULT <expr>|DROP DEFAULT} */
		|	ALTER opt_column ColId alter_column_default
				{
					$$ = ast.NewAlterTableCmd(ast.AT_ColumnDefault, $3, $4)
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP NOT NULL */
		|	ALTER opt_column ColId DROP NOT NULL_P
				{
					$$ = ast.NewAlterTableCmd(ast.AT_DropNotNull, $3, nil)
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET NOT NULL */
		|	ALTER opt_column ColId SET NOT NULL_P
				{
					$$ = ast.NewAlterTableCmd(ast.AT_SetNotNull, $3, nil)
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET EXPRESSION AS <expr> */
		|	ALTER opt_column ColId SET EXPRESSION AS '(' a_expr ')'
				{
					$$ = ast.NewAlterTableCmd(ast.AT_SetExpression, $3, $8)
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP EXPRESSION */
		|	ALTER opt_column ColId DROP EXPRESSION
				{
					$$ = ast.NewAlterTableCmd(ast.AT_DropExpression, $3, nil)
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP EXPRESSION IF EXISTS */
		|	ALTER opt_column ColId DROP EXPRESSION IF_P EXISTS
				{
					cmd := ast.NewAlterTableCmd(ast.AT_DropExpression, $3, nil)
					cmd.MissingOk = true
					$$ = cmd
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET STATISTICS */
		|	ALTER opt_column ColId SET STATISTICS set_statistics_value
				{
					$$ = ast.NewAlterTableCmd(ast.AT_SetStatistics, $3, $6)
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colnum> SET STATISTICS */
		|	ALTER opt_column Iconst SET STATISTICS set_statistics_value
				{
					cmd := ast.NewAlterTableCmd(ast.AT_SetStatistics, "", $6)
					cmd.Num = int16($3)
					$$ = cmd
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET ( column_parameter = value [, ... ] ) */
		|	ALTER opt_column ColId SET reloptions
				{
					$$ = ast.NewAlterTableCmd(ast.AT_SetOptions, $3, $5)
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> RESET ( column_parameter [, ... ] ) */
		|	ALTER opt_column ColId RESET reloptions
				{
					$$ = ast.NewAlterTableCmd(ast.AT_ResetOptions, $3, $5)
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET STORAGE <storagemode> */
		|	ALTER opt_column ColId SET column_storage
				{
					$$ = ast.NewAlterTableCmd(ast.AT_SetStorage, $3, ast.NewString($5))
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET COMPRESSION <cm> */
		|	ALTER opt_column ColId SET column_compression
				{
					$$ = ast.NewAlterTableCmd(ast.AT_SetCompression, $3, ast.NewString($5))
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> ADD GENERATED ... AS IDENTITY ... */
		|	ALTER opt_column ColId ADD_P GENERATED generated_when AS IDENTITY_P OptParenthesizedSeqOptList
				{
					cmd := ast.NewAlterTableCmd(ast.AT_AddIdentity, $3, nil)
					constraint := ast.NewConstraint(ast.CONSTR_IDENTITY)
					constraint.GeneratedWhen = $6
					constraint.Options = $9
					cmd.Def = constraint
					$$ = cmd
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET <sequence options>/RESET */
		|	ALTER opt_column ColId alter_identity_column_option_list
				{
					$$ = ast.NewAlterTableCmd(ast.AT_SetIdentity, $3, $4)
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP IDENTITY */
		|	ALTER opt_column ColId DROP IDENTITY_P
				{
					cmd := ast.NewAlterTableCmd(ast.AT_DropIdentity, $3, nil)
					cmd.MissingOk = false
					$$ = cmd
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP IDENTITY IF EXISTS */
		|	ALTER opt_column ColId DROP IDENTITY_P IF_P EXISTS
				{
					cmd := ast.NewAlterTableCmd(ast.AT_DropIdentity, $3, nil)
					cmd.MissingOk = true
					$$ = cmd
				}
			/* ALTER TABLE <name> DROP [COLUMN] IF EXISTS <colname> [RESTRICT|CASCADE] */
		|	DROP opt_column IF_P EXISTS ColId opt_drop_behavior
				{
					cmd := ast.NewAlterTableCmd(ast.AT_DropColumn, $5, nil)
					cmd.Behavior = $6
					cmd.MissingOk = true
					$$ = cmd
				}
			/* ALTER TABLE <name> DROP [COLUMN] <colname> [RESTRICT|CASCADE] */
		|	DROP opt_column ColId opt_drop_behavior
				{
					cmd := ast.NewAlterTableCmd(ast.AT_DropColumn, $3, nil)
					cmd.Behavior = $4
					cmd.MissingOk = false
					$$ = cmd
				}
			/*
			 * ALTER TABLE <name> ALTER [COLUMN] <colname> [SET DATA] TYPE <typename>
			 *		[ USING <expression> ]
			 */
		|	ALTER opt_column ColId opt_set_data TYPE_P Typename opt_collate_clause alter_using
				{
					cmd := ast.NewAlterTableCmd(ast.AT_AlterColumnType, $3, nil)
					def := ast.NewColumnDef("", $6, 0)
					if collClause, ok := $7.(*ast.CollateClause); ok {
						def.Collclause = collClause
					}
					def.RawDefault = $8
					cmd.Def = def
					$$ = cmd
				}
			/* ALTER FOREIGN TABLE <name> ALTER [COLUMN] <colname> OPTIONS */
		|	ALTER opt_column ColId alter_generic_options
				{
					$$ = ast.NewAlterTableCmd(ast.AT_AlterColumnGenericOptions, $3, $4)
				}
			/* ALTER TABLE <name> ADD CONSTRAINT ... */
		|	ADD_P TableConstraint
				{
					$$ = ast.NewAlterTableCmd(ast.AT_AddConstraint, "", $2)
				}
			/* ALTER TABLE <name> ALTER CONSTRAINT ... */
		|	ALTER CONSTRAINT name ConstraintAttributeSpec
				{
					cmd := ast.NewAlterTableCmd(ast.AT_AlterConstraint, "", nil)
					constraint := ast.NewConstraint(ast.CONSTR_FOREIGN)
					constraint.Conname = $3
					constraint.Deferrable = ($4 & ast.CAS_DEFERRABLE) != 0
					constraint.Initdeferred = ($4 & ast.CAS_INITIALLY_DEFERRED) != 0
					cmd.Def = constraint
					$$ = cmd
				}
			/* ALTER TABLE <name> VALIDATE CONSTRAINT ... */
		|	VALIDATE CONSTRAINT name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_ValidateConstraint, $3, nil)
				}
			/* ALTER TABLE <name> DROP CONSTRAINT IF EXISTS <name> [RESTRICT|CASCADE] */
		|	DROP CONSTRAINT IF_P EXISTS name opt_drop_behavior
				{
					cmd := ast.NewAlterTableCmd(ast.AT_DropConstraint, $5, nil)
					cmd.Behavior = $6
					cmd.MissingOk = true
					$$ = cmd
				}
			/* ALTER TABLE <name> DROP CONSTRAINT <name> [RESTRICT|CASCADE] */
		|	DROP CONSTRAINT name opt_drop_behavior
				{
					cmd := ast.NewAlterTableCmd(ast.AT_DropConstraint, $3, nil)
					cmd.Behavior = $4
					cmd.MissingOk = false
					$$ = cmd
				}
			/* ALTER TABLE <name> SET WITHOUT OIDS, for backward compat */
		|	SET WITHOUT OIDS
				{
					$$ = ast.NewAlterTableCmd(ast.AT_DropOids, "", nil)
				}
			/* ALTER TABLE <name> CLUSTER ON <indexname> */
		|	CLUSTER ON name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_ClusterOn, $3, nil)
				}
			/* ALTER TABLE <name> SET WITHOUT CLUSTER */
		|	SET WITHOUT CLUSTER
				{
					$$ = ast.NewAlterTableCmd(ast.AT_DropCluster, "", nil)
				}
			/* ALTER TABLE <name> SET LOGGED */
		|	SET LOGGED
				{
					$$ = ast.NewAlterTableCmd(ast.AT_SetLogged, "", nil)
				}
			/* ALTER TABLE <name> SET UNLOGGED */
		|	SET UNLOGGED
				{
					$$ = ast.NewAlterTableCmd(ast.AT_SetUnLogged, "", nil)
				}
			/* ALTER TABLE <name> ENABLE TRIGGER <trig> */
		|	ENABLE_P TRIGGER name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_EnableTrig, $3, nil)
				}
			/* ALTER TABLE <name> ENABLE ALWAYS TRIGGER <trig> */
		|	ENABLE_P ALWAYS TRIGGER name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_EnableAlwaysTrig, $4, nil)
				}
			/* ALTER TABLE <name> ENABLE REPLICA TRIGGER <trig> */
		|	ENABLE_P REPLICA TRIGGER name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_EnableReplicaTrig, $4, nil)
				}
			/* ALTER TABLE <name> ENABLE TRIGGER ALL */
		|	ENABLE_P TRIGGER ALL
				{
					$$ = ast.NewAlterTableCmd(ast.AT_EnableTrigAll, "", nil)
				}
			/* ALTER TABLE <name> ENABLE TRIGGER USER */
		|	ENABLE_P TRIGGER USER
				{
					$$ = ast.NewAlterTableCmd(ast.AT_EnableTrigUser, "", nil)
				}
			/* ALTER TABLE <name> DISABLE TRIGGER <trig> */
		|	DISABLE_P TRIGGER name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_DisableTrig, $3, nil)
				}
			/* ALTER TABLE <name> DISABLE TRIGGER ALL */
		|	DISABLE_P TRIGGER ALL
				{
					$$ = ast.NewAlterTableCmd(ast.AT_DisableTrigAll, "", nil)
				}
			/* ALTER TABLE <name> DISABLE TRIGGER USER */
		|	DISABLE_P TRIGGER USER
				{
					$$ = ast.NewAlterTableCmd(ast.AT_DisableTrigUser, "", nil)
				}
			/* ALTER TABLE <name> ENABLE RULE <rule> */
		|	ENABLE_P RULE name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_EnableRule, $3, nil)
				}
			/* ALTER TABLE <name> ENABLE ALWAYS RULE <rule> */
		|	ENABLE_P ALWAYS RULE name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_EnableAlwaysRule, $4, nil)
				}
			/* ALTER TABLE <name> ENABLE REPLICA RULE <rule> */
		|	ENABLE_P REPLICA RULE name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_EnableReplicaRule, $4, nil)
				}
			/* ALTER TABLE <name> DISABLE RULE <rule> */
		|	DISABLE_P RULE name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_DisableRule, $3, nil)
				}
			/* ALTER TABLE <name> INHERIT <parent> */
		|	INHERIT qualified_name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_AddInherit, "", $2)
				}
			/* ALTER TABLE <name> NO INHERIT <parent> */
		|	NO INHERIT qualified_name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_DropInherit, "", $3)
				}
			/* ALTER TABLE <name> OF <type_name> */
		|	OF any_name
				{
					typeName := makeTypeNameFromNodeList($2)
					$$ = ast.NewAlterTableCmd(ast.AT_AddOf, "", typeName)
				}
			/* ALTER TABLE <name> NOT OF */
		|	NOT OF
				{
					$$ = ast.NewAlterTableCmd(ast.AT_DropOf, "", nil)
				}
			/* ALTER TABLE <name> OWNER TO RoleSpec */
		|	OWNER TO RoleSpec
				{
					cmd := ast.NewAlterTableCmd(ast.AT_ChangeOwner, "", nil)
					cmd.Newowner = $3
					$$ = cmd
				}
			/* ALTER TABLE <name> SET ACCESS METHOD { <amname> | DEFAULT } */
		|	SET ACCESS METHOD set_access_method_name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_SetAccessMethod, $4, nil)
				}
			/* ALTER TABLE <name> SET TABLESPACE <tablespacename> */
		|	SET TABLESPACE name
				{
					$$ = ast.NewAlterTableCmd(ast.AT_SetTableSpace, $3, nil)
				}
			/* ALTER TABLE <name> SET (...) */
		|	SET reloptions
				{
					$$ = ast.NewAlterTableCmd(ast.AT_SetRelOptions, "", $2)
				}
			/* ALTER TABLE <name> RESET (...) */
		|	RESET reloptions
				{
					$$ = ast.NewAlterTableCmd(ast.AT_ResetRelOptions, "", $2)
				}
			/* ALTER TABLE <name> REPLICA IDENTITY */
		|	REPLICA IDENTITY_P replica_identity
				{
					$$ = ast.NewAlterTableCmd(ast.AT_ReplicaIdentity, "", $3)
				}
			/* ALTER TABLE <name> ENABLE ROW LEVEL SECURITY */
		|	ENABLE_P ROW LEVEL SECURITY
				{
					$$ = ast.NewAlterTableCmd(ast.AT_EnableRowSecurity, "", nil)
				}
			/* ALTER TABLE <name> DISABLE ROW LEVEL SECURITY */
		|	DISABLE_P ROW LEVEL SECURITY
				{
					$$ = ast.NewAlterTableCmd(ast.AT_DisableRowSecurity, "", nil)
				}
			/* ALTER TABLE <name> FORCE ROW LEVEL SECURITY */
		|	FORCE ROW LEVEL SECURITY
				{
					$$ = ast.NewAlterTableCmd(ast.AT_ForceRowSecurity, "", nil)
				}
			/* ALTER TABLE <name> NO FORCE ROW LEVEL SECURITY */
		|	NO FORCE ROW LEVEL SECURITY
				{
					$$ = ast.NewAlterTableCmd(ast.AT_NoForceRowSecurity, "", nil)
				}
		|	alter_generic_options
				{
					$$ = ast.NewAlterTableCmd(ast.AT_GenericOptions, "", $1)
				}
		;

partition_cmd:
			/* ALTER TABLE <name> ATTACH PARTITION <table_name> FOR VALUES */
			ATTACH PARTITION qualified_name PartitionBoundSpec
				{
					cmd := ast.NewAlterTableCmd(ast.AT_AttachPartition, "", nil)
					partitionCmd := ast.NewPartitionCmd($3, $4, false, 0)
					cmd.Def = partitionCmd
					$$ = cmd
				}
			/* ALTER TABLE <name> DETACH PARTITION <partition_name> [CONCURRENTLY] */
		|	DETACH PARTITION qualified_name opt_concurrently
				{
					cmd := ast.NewAlterTableCmd(ast.AT_DetachPartition, "", nil)
					partitionCmd := ast.NewPartitionCmd($3, nil, $4, 0)
					cmd.Def = partitionCmd
					$$ = cmd
				}
		|	DETACH PARTITION qualified_name FINALIZE
				{
					cmd := ast.NewAlterTableCmd(ast.AT_DetachPartitionFinalize, "", nil)
					partitionCmd := ast.NewPartitionCmd($3, nil, false, 0)
					cmd.Def = partitionCmd
					$$ = cmd
				}
		;

index_partition_cmd:
			/* ALTER INDEX <name> ATTACH PARTITION <index_name> */
			ATTACH PARTITION qualified_name
				{
					cmd := ast.NewAlterTableCmd(ast.AT_AttachPartition, "", nil)
					partitionCmd := ast.NewPartitionCmd($3, nil, false, 0)
					cmd.Def = partitionCmd
					$$ = cmd
				}
		;

alter_column_default:
			SET DEFAULT a_expr			{ $$ = $3 }
		|	DROP DEFAULT				{ $$ = nil }
		;

opt_column:
			COLUMN						{ }
		|	/* EMPTY */					{ }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				DROP TABLE / DROP INDEX etc.
 *
 *****************************************************************************/

DropStmt:	DROP object_type_any_name IF_P EXISTS any_name_list opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = $2
					n.MissingOk = true
					n.Objects = $5
					n.Behavior = $6
					n.Concurrent = false
					$$ = n
				}
			| DROP object_type_any_name any_name_list opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = $2
					n.MissingOk = false
					n.Objects = $3
					n.Behavior = $4
					n.Concurrent = false
					$$ = n
				}
			| DROP drop_type_name IF_P EXISTS name_list opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = $2
					n.MissingOk = true
					n.Objects = $5
					n.Behavior = $6
					n.Concurrent = false
					$$ = n
				}
			| DROP drop_type_name name_list opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = $2
					n.MissingOk = false
					n.Objects = $3
					n.Behavior = $4
					n.Concurrent = false
					$$ = n
				}
			| DROP object_type_name_on_any_name name ON any_name opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = $2
					objects := $5
					objects.Append(ast.NewString($3))
					n.Objects = objects
					n.Behavior = $6
					n.MissingOk = false
					n.Concurrent = false
					$$ = n
				}
			| DROP object_type_name_on_any_name IF_P EXISTS name ON any_name opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = $2
					objects := $7
					objects.Append(ast.NewString($5))
					n.Objects = objects
					n.Behavior = $8
					n.MissingOk = true
					n.Concurrent = false
					$$ = n
				}
			| DROP TYPE_P type_name_list opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = ast.OBJECT_TYPE
					n.MissingOk = false
					n.Objects = $3
					n.Behavior = $4
					n.Concurrent = false
					$$ = n
				}
			| DROP TYPE_P IF_P EXISTS type_name_list opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = ast.OBJECT_TYPE
					n.MissingOk = true
					n.Objects = $5
					n.Behavior = $6
					n.Concurrent = false
					$$ = n
				}
			| DROP DOMAIN_P type_name_list opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = ast.OBJECT_DOMAIN
					n.MissingOk = false
					n.Objects = $3
					n.Behavior = $4
					n.Concurrent = false
					$$ = n
				}
			| DROP DOMAIN_P IF_P EXISTS type_name_list opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = ast.OBJECT_DOMAIN
					n.MissingOk = true
					n.Objects = $5
					n.Behavior = $6
					n.Concurrent = false
					$$ = n
				}
			| DROP INDEX CONCURRENTLY any_name_list opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = ast.OBJECT_INDEX
					n.MissingOk = false
					n.Objects = $4
					n.Behavior = $5
					n.Concurrent = true
					$$ = n
				}
			| DROP INDEX CONCURRENTLY IF_P EXISTS any_name_list opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = ast.OBJECT_INDEX
					n.MissingOk = true
					n.Objects = $6
					n.Behavior = $7
					n.Concurrent = true
					$$ = n
				}

DropCastStmt:	DROP CAST opt_if_exists '(' Typename AS Typename ')' opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = ast.OBJECT_CAST
					n.Objects = ast.NewNodeList(ast.NewNodeList($5, $7))
					n.Behavior = $9
					n.MissingOk = ($3 == 1)
					n.Concurrent = false
					$$ = n
				}

DropOpClassStmt: DROP OPERATOR CLASS any_name USING name opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					// Create a new list with the USING name prepended to any_name
					objects := ast.NewNodeList(ast.NewString($6))
					for _, item := range $4.Items {
						objects.Append(item)
					}
					n.Objects = ast.NewNodeList(objects)
					n.RemoveType = ast.OBJECT_OPCLASS
					n.Behavior = $7
					n.MissingOk = false
					n.Concurrent = false
					$$ = n
				}
			| DROP OPERATOR CLASS IF_P EXISTS any_name USING name opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					// Create a new list with the USING name prepended to any_name
					objects := ast.NewNodeList(ast.NewString($8))
					for _, item := range $6.Items {
						objects.Append(item)
					}
					n.Objects = ast.NewNodeList(objects)
					n.RemoveType = ast.OBJECT_OPCLASS
					n.Behavior = $9
					n.MissingOk = true
					n.Concurrent = false
					$$ = n
				}

DropOpFamilyStmt: DROP OPERATOR FAMILY any_name USING name opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					// Create a new list with the USING name prepended to any_name
					objects := ast.NewNodeList(ast.NewString($6))
					for _, item := range $4.Items {
						objects.Append(item)
					}
					n.Objects = ast.NewNodeList(objects)
					n.RemoveType = ast.OBJECT_OPFAMILY
					n.Behavior = $7
					n.MissingOk = false
					n.Concurrent = false
					$$ = n
				}
			| DROP OPERATOR FAMILY IF_P EXISTS any_name USING name opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					// Create a new list with the USING name prepended to any_name
					objects := ast.NewNodeList(ast.NewString($8))
					for _, item := range $6.Items {
						objects.Append(item)
					}
					n.Objects = ast.NewNodeList(objects)
					n.RemoveType = ast.OBJECT_OPFAMILY
					n.Behavior = $9
					n.MissingOk = true
					n.Concurrent = false
					$$ = n
				}

DropTransformStmt: DROP TRANSFORM opt_if_exists FOR Typename LANGUAGE name opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = ast.OBJECT_TRANSFORM
					n.Objects = ast.NewNodeList(ast.NewNodeList($5, ast.NewString($7)))
					n.Behavior = $8
					n.MissingOk = ($3 == 1)
					n.Concurrent = false
					$$ = n
				}

DropSubscriptionStmt: DROP SUBSCRIPTION name opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = ast.OBJECT_SUBSCRIPTION
					n.Objects = ast.NewNodeList(ast.NewNodeList(ast.NewString($3)))
					n.Behavior = $4
					n.MissingOk = false
					n.Concurrent = false
					$$ = n
				}
			| DROP SUBSCRIPTION IF_P EXISTS name opt_drop_behavior
				{
					n := &ast.DropStmt{
						BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
					}

					n.RemoveType = ast.OBJECT_SUBSCRIPTION
					n.Objects = ast.NewNodeList(ast.NewNodeList(ast.NewString($5)))
					n.Behavior = $6
					n.MissingOk = true
					n.Concurrent = false
					$$ = n
				}
		;

/*
 * DROP FUNCTION/AGGREGATE/OPERATOR Statements - Phase 3J PostgreSQL Extensions
 * Ported from postgres/src/backend/parser/gram.y
 */
RemoveFuncStmt:
		DROP FUNCTION function_with_argtypes_list opt_drop_behavior
			{
				n := &ast.DropStmt{
					BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
				}
				n.RemoveType = ast.OBJECT_FUNCTION
				n.Objects = $3
				n.Behavior = $4
				n.MissingOk = false
				n.Concurrent = false
				$$ = n
			}
	|	DROP FUNCTION IF_P EXISTS function_with_argtypes_list opt_drop_behavior
			{
				n := &ast.DropStmt{
					BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
				}
				n.RemoveType = ast.OBJECT_FUNCTION
				n.Objects = $5
				n.Behavior = $6
				n.MissingOk = true
				n.Concurrent = false
				$$ = n
			}
	|	DROP PROCEDURE function_with_argtypes_list opt_drop_behavior
			{
				n := &ast.DropStmt{
					BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
				}
				n.RemoveType = ast.OBJECT_PROCEDURE
				n.Objects = $3
				n.Behavior = $4
				n.MissingOk = false
				n.Concurrent = false
				$$ = n
			}
	|	DROP PROCEDURE IF_P EXISTS function_with_argtypes_list opt_drop_behavior
			{
				n := &ast.DropStmt{
					BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
				}
				n.RemoveType = ast.OBJECT_PROCEDURE
				n.Objects = $5
				n.Behavior = $6
				n.MissingOk = true
				n.Concurrent = false
				$$ = n
			}
	|	DROP ROUTINE function_with_argtypes_list opt_drop_behavior
			{
				n := &ast.DropStmt{
					BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
				}
				n.RemoveType = ast.OBJECT_ROUTINE
				n.Objects = $3
				n.Behavior = $4
				n.MissingOk = false
				n.Concurrent = false
				$$ = n
			}
	|	DROP ROUTINE IF_P EXISTS function_with_argtypes_list opt_drop_behavior
			{
				n := &ast.DropStmt{
					BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
				}
				n.RemoveType = ast.OBJECT_ROUTINE
				n.Objects = $5
				n.Behavior = $6
				n.MissingOk = true
				n.Concurrent = false
				$$ = n
			}
	;

RemoveAggrStmt:
		DROP AGGREGATE aggregate_with_argtypes_list opt_drop_behavior
			{
				n := &ast.DropStmt{
					BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
				}
				n.RemoveType = ast.OBJECT_AGGREGATE
				n.Objects = $3
				n.Behavior = $4
				n.MissingOk = false
				n.Concurrent = false
				$$ = n
			}
	|	DROP AGGREGATE IF_P EXISTS aggregate_with_argtypes_list opt_drop_behavior
			{
				n := &ast.DropStmt{
					BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
				}
				n.RemoveType = ast.OBJECT_AGGREGATE
				n.Objects = $5
				n.Behavior = $6
				n.MissingOk = true
				n.Concurrent = false
				$$ = n
			}
	;

RemoveOperStmt:
		DROP OPERATOR operator_with_argtypes_list opt_drop_behavior
			{
				n := &ast.DropStmt{
					BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
				}
				n.RemoveType = ast.OBJECT_OPERATOR
				n.Objects = $3
				n.Behavior = $4
				n.MissingOk = false
				n.Concurrent = false
				$$ = n
			}
	|	DROP OPERATOR IF_P EXISTS operator_with_argtypes_list opt_drop_behavior
			{
				n := &ast.DropStmt{
					BaseNode: ast.BaseNode{Tag: ast.T_DropStmt},
				}
				n.RemoveType = ast.OBJECT_OPERATOR
				n.Objects = $5
				n.Behavior = $6
				n.MissingOk = true
				n.Concurrent = false
				$$ = n
			}
	;

VariableSetStmt:
			SET set_rest
				{
					stmt := $2
					stmt.IsLocal = false
					$$ = stmt
				}
		|	SET LOCAL set_rest
				{
					stmt := $3
					stmt.IsLocal = true
					$$ = stmt
				}
		|	SET SESSION set_rest
				{
					stmt := $3
					stmt.IsLocal = false
					$$ = stmt
				}
		;

set_rest:
		TRANSACTION transaction_mode_list
			{
				$$ = ast.NewVariableSetStmt(ast.VAR_SET_MULTI, "TRANSACTION", $2, false)
			}
	|	SESSION CHARACTERISTICS AS TRANSACTION transaction_mode_list
			{
				$$ = ast.NewVariableSetStmt(ast.VAR_SET_MULTI, "SESSION CHARACTERISTICS AS TRANSACTION", $5, false)
			}
	|	set_rest_more						{ $$ = $1 }
	;

reset_rest:
		generic_reset							{ $$ = $1 }
	|	TIME ZONE
			{
				$$ = ast.NewVariableSetStmt(ast.VAR_RESET, "timezone", nil, false)
			}
	|	TRANSACTION ISOLATION LEVEL
			{
				$$ = ast.NewVariableSetStmt(ast.VAR_RESET, "transaction_isolation", nil, false)
			}
	|	SESSION AUTHORIZATION
			{
				$$ = ast.NewVariableSetStmt(ast.VAR_RESET, "session_authorization", nil, false)
			}
	;

generic_reset:
		var_name
			{
				$$ = ast.NewVariableSetStmt(ast.VAR_RESET, $1, nil, false)
			}
	|	ALL
			{
				$$ = ast.NewVariableSetStmt(ast.VAR_RESET_ALL, "", nil, false)
			}
	;

SetResetClause:
		SET set_rest							{ $$ = $2 }
	|	VariableResetStmt						{ $$ = $1.(*ast.VariableSetStmt) }
	;

VariableResetStmt:
		RESET reset_rest						{ $$ = ast.Stmt($2) }
	;

/*
 * SET CONSTRAINTS Statement - Phase 3J PostgreSQL Extensions
 * Ported from postgres/src/backend/parser/gram.y
 */
ConstraintsSetStmt:
		SET CONSTRAINTS constraints_set_list constraints_set_mode
			{
				$$ = ast.NewConstraintsSetStmt($3, $4)
			}
	;

constraints_set_list:
		ALL						{ $$ = nil }
	|	qualified_name_list		{ $$ = $1 }
	;

constraints_set_mode:
		DEFERRED				{ $$ = true }
	|	IMMEDIATE				{ $$ = false }
	;

/*
 * PL/pgSQL Assignment Statement - Phase 3J PostgreSQL Extensions
 * Ported from postgres/src/backend/parser/gram.y
 */
PLpgSQL_Expr:
		opt_distinct_clause opt_target_list
		from_clause where_clause
		group_clause having_clause window_clause
		opt_sort_clause opt_select_limit opt_for_locking_clause
			{
				n := ast.NewSelectStmt()
				n.DistinctClause = $1
				n.TargetList = $2
				n.FromClause = $3
				n.WhereClause = $4
				if $5 != nil {
					n.GroupClause = $5.List
					n.GroupDistinct = $5.Distinct
				}
				n.HavingClause = $6
				n.WindowClause = $7
				n.SortClause = $8
				if $9 != nil {
					n.LimitOffset = $9.limitOffset
					n.LimitCount = $9.limitCount
					n.LimitOption = $9.limitOption
				}
				n.LockingClause = $10
				$$ = n
			}
	;

PLAssignStmt:
		plassign_target opt_indirection plassign_equals PLpgSQL_Expr
			{
				n := ast.NewPLAssignStmt($1, $4.(*ast.SelectStmt))
				n.Indirection = $2
				$$ = n
			}
	;

plassign_target:
		ColId							{ $$ = $1 }
	|	PARAM							{ $$ = fmt.Sprintf("$%d", $1) }
	;

plassign_equals:
		COLON_EQUALS
	|	'='
	;

/*
 * EXPLAIN Statement
 * Ported from postgres/src/backend/parser/gram.y
 */
ExplainStmt:
		EXPLAIN ExplainableStmt
			{
				$$ = ast.NewExplainStmt($2, nil)
			}
	|	EXPLAIN analyze_keyword opt_verbose ExplainableStmt
			{
				options := ast.NewNodeList(ast.NewDefElem("analyze", nil))
				if $3 != 0 {
					options.Append(ast.NewDefElem("verbose", nil))
				}
				$$ = ast.NewExplainStmt($4, options)
			}
	|	EXPLAIN VERBOSE ExplainableStmt
			{
				options := ast.NewNodeList(ast.NewDefElem("verbose", nil))
				$$ = ast.NewExplainStmt($3, options)
			}
	|	EXPLAIN '(' utility_option_list ')' ExplainableStmt
			{
				$$ = ast.NewExplainStmt($5, $3)
			}
	;

utility_option_list:
		utility_option_elem
			{
				$$ = ast.NewNodeList($1)
			}
	|	utility_option_list ',' utility_option_elem
			{
				$1.Append($3)
				$$ = $1
			}
	;

utility_option_elem:
		utility_option_name utility_option_arg
			{
				$$ = ast.NewDefElem($1, $2)
			}
	;

utility_option_name:
		NonReservedWord						{ $$ = $1 }
	|	analyze_keyword						{ $$ = "analyze" }
	| 	FORMAT_LA							{ $$ = "format" }
	;

utility_option_arg:
		opt_boolean_or_string				{ $$ = ast.NewString($1) }
	|	NumericOnly							{ $$ = $1 }
	|	/* EMPTY */							{ $$ = nil }
	;

ExplainableStmt:
		SelectStmt							{ $$ = $1 }
	|	InsertStmt							{ $$ = $1 }
	|	UpdateStmt							{ $$ = $1 }
	|	DeleteStmt							{ $$ = $1 }
	|	MergeStmt							{ $$ = $1 }
	| 	DeclareCursorStmt					{ $$ = $1 }
	| 	CreateAsStmt						{ $$ = $1 }
	|	CreateMatViewStmt					{ $$ = $1 }
	|	RefreshMatViewStmt					{ $$ = $1 }
	| 	ExecuteStmt							{ $$ = $1 }
	;

/*
 * VACUUM/ANALYZE Statement
 * Ported from postgres/src/backend/parser/gram.y
 */
VacuumStmt:
		VACUUM opt_full opt_freeze opt_verbose opt_analyze opt_vacuum_relation_list
			{
				var optionsList *ast.NodeList
				if $2 != 0 || $3 != 0 || $4 != 0 || $5 != 0 {
					optionsList = ast.NewNodeList()
					if $2 != 0 {
						optionsList.Append(ast.NewDefElem("full", ast.NewBoolean(true)))
					}
					if $3 != 0 {
						optionsList.Append(ast.NewDefElem("freeze", ast.NewBoolean(true)))
					}
					if $4 != 0 {
						optionsList.Append(ast.NewDefElem("verbose", ast.NewBoolean(true)))
					}
					if $5 != 0 {
						optionsList.Append(ast.NewDefElem("analyze", ast.NewBoolean(true)))
					}
				}

				$$ = ast.NewVacuumStmt(optionsList, $6)
			}
	|	VACUUM '(' utility_option_list ')' opt_vacuum_relation_list
			{
				$$ = ast.NewVacuumStmt($3, $5)
			}
	;

AnalyzeStmt:
		analyze_keyword opt_verbose opt_vacuum_relation_list
			{
				var optionsList *ast.NodeList
				if $2 != 0 {
					optionsList = ast.NewNodeList(ast.NewDefElem("verbose", ast.NewBoolean(true)))
				}

				$$ = ast.NewAnalyzeStmt(optionsList, $3)
			}
	|	analyze_keyword '(' utility_option_list ')' opt_vacuum_relation_list
			{
				$$ = ast.NewAnalyzeStmt($3, $5)
			}
	;

opt_vacuum_relation_list:
		vacuum_relation_list				{ $$ = $1 }
	|	/* EMPTY */							{ $$ = nil }
	;

vacuum_relation_list:
		vacuum_relation
			{
				$$ = ast.NewNodeList($1)
			}
	|	vacuum_relation_list ',' vacuum_relation
			{
				$1.Append($3)
				$$ = $1
			}
	;

vacuum_relation:
		qualified_name opt_name_list
			{
				$$ = ast.NewVacuumRelation($1, $2)
			}
	;

analyze_keyword:
		ANALYZE								{ $$ = "analyze" }
		| ANALYSE /* British */				{ $$ = "analyse" }
	;

/*
 * SHOW Statement
 * Ported from postgres/src/backend/parser/gram.y
 */
VariableShowStmt:
		SHOW var_name
			{
				$$ = ast.NewVariableShowStmt($2)
			}
	|	SHOW TIME ZONE
			{
				$$ = ast.NewVariableShowStmt("timezone")
			}
	|	SHOW TRANSACTION ISOLATION LEVEL
			{
				$$ = ast.NewVariableShowStmt("transaction_isolation")
			}
	|	SHOW SESSION AUTHORIZATION
			{
				$$ = ast.NewVariableShowStmt("session_authorization")
			}
	|	SHOW ALL
			{
				$$ = ast.NewVariableShowStmt("all")
			}
	;

/*
 * ALTER SYSTEM Statement
 * Ported from postgres/src/backend/parser/gram.y
 */
AlterSystemStmt:
		ALTER SYSTEM_P SET generic_set
			{
				$$ = ast.NewAlterSystemStmt($4)
			}
	|	ALTER SYSTEM_P RESET generic_reset
			{
				$$ = ast.NewAlterSystemStmt($4)
			}
	;

/*
 * CLUSTER statement
 * Ported from postgres/src/backend/parser/gram.y
 */
ClusterStmt:
			CLUSTER '(' utility_option_list ')' qualified_name cluster_index_specification
				{
					$$ = ast.NewClusterStmt($5, $6, $3)
				}
		|	CLUSTER '(' utility_option_list ')'
				{
					$$ = ast.NewClusterStmt(nil, "", $3)
				}
		|	CLUSTER opt_verbose qualified_name cluster_index_specification
				{
					var params *ast.NodeList
					if $2 != 0 {
						verboseDefElem := ast.NewDefElem("verbose", nil)
						params = ast.NewNodeList(verboseDefElem)
					}
					$$ = ast.NewClusterStmt($3, $4, params)
				}
		|	CLUSTER opt_verbose
				{
					var params *ast.NodeList
					if $2 != 0 {
						verboseDefElem := ast.NewDefElem("verbose", nil)
						params = ast.NewNodeList(verboseDefElem)
					}
					$$ = ast.NewClusterStmt(nil, "", params)
				}
		|	CLUSTER opt_verbose name ON qualified_name
				{
					var params *ast.NodeList
					if $2 != 0 {
						verboseDefElem := ast.NewDefElem("verbose", nil)
						params = ast.NewNodeList(verboseDefElem)
					}
					$$ = ast.NewClusterStmt($5, $3, params)
				}
		;

cluster_index_specification:
			USING name							{ $$ = $2 }
		|	/* EMPTY */							{ $$ = "" }
		;

/*
 * REINDEX statement
 * Ported from postgres/src/backend/parser/gram.y:5777-5806
 */
ReindexStmt:
			REINDEX opt_reindex_option_list reindex_target_relation opt_concurrently qualified_name
				{
					params := $2
					if $4 {
						concurrentlyDefElem := ast.NewDefElem("concurrently", nil)
						if params == nil {
							params = ast.NewNodeList(concurrentlyDefElem)
						} else {
							params.Append(concurrentlyDefElem)
						}
					}
					$$ = ast.NewReindexStmt(ast.ReindexObjectType($3), $5, "", params)
				}
		|	REINDEX opt_reindex_option_list SCHEMA opt_concurrently name
				{
					params := $2
					if $4 {
						concurrentlyDefElem := ast.NewDefElem("concurrently", nil)
						if params == nil {
							params = ast.NewNodeList(concurrentlyDefElem)
						} else {
							params.Append(concurrentlyDefElem)
						}
					}
					$$ = ast.NewReindexStmt(ast.REINDEX_OBJECT_SCHEMA, nil, $5, params)
				}
		|	REINDEX opt_reindex_option_list reindex_target_all opt_concurrently opt_single_name
				{
					params := $2
					if $4 {
						concurrentlyDefElem := ast.NewDefElem("concurrently", nil)
						if params == nil {
							params = ast.NewNodeList(concurrentlyDefElem)
						} else {
							params.Append(concurrentlyDefElem)
						}
					}
					$$ = ast.NewReindexStmt(ast.ReindexObjectType($3), nil, $5, params)
				}
		;

reindex_target_relation:
			INDEX								{ $$ = int(ast.REINDEX_OBJECT_INDEX) }
		|	TABLE								{ $$ = int(ast.REINDEX_OBJECT_TABLE) }
		;

reindex_target_all:
			SYSTEM_P							{ $$ = int(ast.REINDEX_OBJECT_SYSTEM) }
		|	DATABASE							{ $$ = int(ast.REINDEX_OBJECT_DATABASE) }
		;

opt_reindex_option_list:
			'(' utility_option_list ')'			{ $$ = $2 }
		|	/* EMPTY */							{ $$ = nil }
		;

/*
 * CHECKPOINT statement
 * Ported from postgres/src/backend/parser/gram.y:5808-5815
 */
CheckPointStmt:
			CHECKPOINT
				{
					$$ = ast.NewCheckPointStmt()
				}
		;

/*
 * DISCARD statement
 * Ported from postgres/src/backend/parser/gram.y:5824-5863
 */
DiscardStmt:
			DISCARD ALL
				{
					$$ = ast.NewDiscardStmt(ast.DISCARD_ALL)
				}
		|	DISCARD TEMP
				{
					$$ = ast.NewDiscardStmt(ast.DISCARD_TEMP)
				}
		|	DISCARD TEMPORARY
				{
					$$ = ast.NewDiscardStmt(ast.DISCARD_TEMP)
				}
		|	DISCARD PLANS
				{
					$$ = ast.NewDiscardStmt(ast.DISCARD_PLANS)
				}
		|	DISCARD SEQUENCES
				{
					$$ = ast.NewDiscardStmt(ast.DISCARD_SEQUENCES)
				}
		;

/* object types taking any_name/any_name_list */
object_type_any_name:
			TABLE						{ $$ = ast.OBJECT_TABLE }
		|	SEQUENCE					{ $$ = ast.OBJECT_SEQUENCE }
		|	VIEW						{ $$ = ast.OBJECT_VIEW }
		|	MATERIALIZED VIEW			{ $$ = ast.OBJECT_MATVIEW }
		|	INDEX						{ $$ = ast.OBJECT_INDEX }
		|	FOREIGN TABLE				{ $$ = ast.OBJECT_FOREIGN_TABLE }
		|	COLLATION					{ $$ = ast.OBJECT_COLLATION }
		|	CONVERSION_P				{ $$ = ast.OBJECT_CONVERSION }
		|	STATISTICS					{ $$ = ast.OBJECT_STATISTIC_EXT }
		|	TEXT_P SEARCH PARSER		{ $$ = ast.OBJECT_TSPARSER }
		|	TEXT_P SEARCH DICTIONARY	{ $$ = ast.OBJECT_TSDICTIONARY }
		|	TEXT_P SEARCH TEMPLATE		{ $$ = ast.OBJECT_TSTEMPLATE }
		|	TEXT_P SEARCH CONFIGURATION	{ $$ = ast.OBJECT_TSCONFIGURATION }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				RENAME
 *
 *****************************************************************************/

/* For now, simplified function/aggregate specs until we implement full argument support */

func_args:
		'(' func_args_list ')' { $$ = $2 }
	|	'(' ')' { $$ = ast.NewNodeList() }
	;

func_args_list:
		func_arg
			{
				$$ = ast.NewNodeList($1)
			}
	|	func_args_list ',' func_arg
			{
				$1.Append($3)
				$$ = $1
			}
	;

operator_with_argtypes:
		any_operator oper_argtypes
			{
				objWithArgs := &ast.ObjectWithArgs{
					BaseNode: ast.BaseNode{Tag: ast.T_ObjectWithArgs},
					Objname: $1,
					Objargs: $2,
				}
				$$ = objWithArgs
			}
		;

oper_argtypes:
		'(' Typename ')'
			{
				yylex.Error("Use NONE to denote the missing argument of a unary operator.")
				return 1
			}
		| '(' Typename ',' Typename ')'
				{ $$ = ast.NewNodeList($2, $4) }
		| '(' NONE ',' Typename ')'					/* left unary */
				{ $$ = ast.NewNodeList(nil, $4) }
		| '(' Typename ',' NONE ')'					/* right unary */
				{ $$ = ast.NewNodeList($2, nil) }

aggregate_with_argtypes:
		func_name aggr_args
			{
				var objfuncArgs *ast.NodeList
				if firstElem := linitial($2); firstElem != nil {
					if nodeList, ok := firstElem.(*ast.NodeList); ok {
						objfuncArgs = nodeList
					}
				}
				objWithArgs := &ast.ObjectWithArgs{
					BaseNode: ast.BaseNode{Tag: ast.T_ObjectWithArgs},
					Objname: $1,
					Objargs: extractAggrArgTypes($2),
					ObjfuncArgs: objfuncArgs, // linitial($2) like PostgreSQL
					ArgsUnspecified: false,
				}
				$$ = objWithArgs
			}
	;

aggregate_with_argtypes_list:
		aggregate_with_argtypes					{ $$ = ast.NewNodeList($1) }
		| aggregate_with_argtypes_list ',' aggregate_with_argtypes
												{ $1.Append($3); $$ = $1 }

RenameStmt:
		ALTER AGGREGATE aggregate_with_argtypes RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_AGGREGATE,
					Object:       $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER COLLATION any_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_COLLATION,
					Object:       $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER CONVERSION_P any_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_CONVERSION,
					Object:       $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER DATABASE name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_DATABASE,
					Subname:      $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER DOMAIN_P any_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_DOMAIN,
					Object:       $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER DOMAIN_P any_name RENAME CONSTRAINT name TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_DOMCONSTRAINT,
					Object:       $3,
					Subname:      $6,
					Newname:      $8,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER FOREIGN DATA_P WRAPPER name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_FDW,
					Object:       ast.NewString($5),
					Newname:      $8,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER FUNCTION function_with_argtypes RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_FUNCTION,
					Object:       $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER GROUP_P RoleId RENAME TO RoleId
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_ROLE,
					Subname:      $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER opt_procedural LANGUAGE name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_LANGUAGE,
					Object:       ast.NewString($4),
					Newname:      $7,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER OPERATOR CLASS any_name USING name RENAME TO name
			{
				/* lcons equivalent - create NodeList with string name prepended to qualified_name list */
				list := ast.NewNodeList()
				list.Append(ast.NewString($6))
				for _, item := range $4.Items {
					list.Append(item)
				}
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_OPCLASS,
					Object:       list,
					Newname:      $9,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER OPERATOR FAMILY any_name USING name RENAME TO name
			{
				/* lcons equivalent - create NodeList with string name prepended to qualified_name list */
				list := ast.NewNodeList()
				list.Append(ast.NewString($6))
				for _, item := range $4.Items {
					list.Append(item)
				}
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_OPFAMILY,
					Object:       list,
					Newname:      $9,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER POLICY name ON qualified_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_POLICY,
					Relation:     $5,
					Subname:      $3,
					Newname:      $8,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER POLICY IF_P EXISTS name ON qualified_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_POLICY,
					Relation:     $7,
					Subname:      $5,
					Newname:      $10,
					MissingOk:    true,
				}
				$$ = renameStmt
			}
	|	ALTER PROCEDURE function_with_argtypes RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_PROCEDURE,
					Object:       $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER PUBLICATION name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_PUBLICATION,
					Object:       ast.NewString($3),
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER ROUTINE function_with_argtypes RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_ROUTINE,
					Object:       $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER SCHEMA name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_SCHEMA,
					Subname:      $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER SERVER name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_FOREIGN_SERVER,
					Object:       ast.NewString($3),
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER SUBSCRIPTION name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_SUBSCRIPTION,
					Object:       ast.NewString($3),
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER TABLE relation_expr RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_TABLE,
					Relation:     $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER TABLE IF_P EXISTS relation_expr RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_TABLE,
					Relation:     $5,
					Newname:      $8,
					MissingOk:    true,
				}
				$$ = renameStmt
			}
	|	ALTER SEQUENCE qualified_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_SEQUENCE,
					Relation:     $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER SEQUENCE IF_P EXISTS qualified_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_SEQUENCE,
					Relation:     $5,
					Newname:      $8,
					MissingOk:    true,
				}
				$$ = renameStmt
			}
	|	ALTER VIEW qualified_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_VIEW,
					Relation:     $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER VIEW IF_P EXISTS qualified_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_VIEW,
					Relation:     $5,
					Newname:      $8,
					MissingOk:    true,
				}
				$$ = renameStmt
			}
	|	ALTER MATERIALIZED VIEW qualified_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_MATVIEW,
					Relation:     $4,
					Newname:      $7,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_MATVIEW,
					Relation:     $6,
					Newname:      $9,
					MissingOk:    true,
				}
				$$ = renameStmt
			}
	|	ALTER INDEX qualified_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_INDEX,
					Relation:     $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER INDEX IF_P EXISTS qualified_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_INDEX,
					Relation:     $5,
					Newname:      $8,
					MissingOk:    true,
				}
				$$ = renameStmt
			}
	|	ALTER FOREIGN TABLE relation_expr RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_FOREIGN_TABLE,
					Relation:     $4,
					Newname:      $7,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER FOREIGN TABLE IF_P EXISTS relation_expr RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_FOREIGN_TABLE,
					Relation:     $6,
					Newname:      $9,
					MissingOk:    true,
				}
				$$ = renameStmt
			}
	|	ALTER TABLE relation_expr RENAME opt_column name TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_COLUMN,
					RelationType: ast.OBJECT_TABLE,
					Relation:     $3,
					Subname:      $6,
					Newname:      $8,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER TABLE IF_P EXISTS relation_expr RENAME opt_column name TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_COLUMN,
					RelationType: ast.OBJECT_TABLE,
					Relation:     $5,
					Subname:      $8,
					Newname:      $10,
					MissingOk:    true,
				}
				$$ = renameStmt
			}
	|	ALTER VIEW qualified_name RENAME opt_column name TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_COLUMN,
					RelationType: ast.OBJECT_VIEW,
					Relation:     $3,
					Subname:      $6,
					Newname:      $8,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER VIEW IF_P EXISTS qualified_name RENAME opt_column name TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_COLUMN,
					RelationType: ast.OBJECT_VIEW,
					Relation:     $5,
					Subname:      $8,
					Newname:      $10,
					MissingOk:    true,
				}
				$$ = renameStmt
			}
	|	ALTER MATERIALIZED VIEW qualified_name RENAME opt_column name TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_COLUMN,
					RelationType: ast.OBJECT_MATVIEW,
					Relation:     $4,
					Subname:      $7,
					Newname:      $9,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name RENAME opt_column name TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_COLUMN,
					RelationType: ast.OBJECT_MATVIEW,
					Relation:     $6,
					Subname:      $9,
					Newname:      $11,
					MissingOk:    true,
				}
				$$ = renameStmt
			}
	|	ALTER TABLE relation_expr RENAME CONSTRAINT name TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_TABCONSTRAINT,
					Relation:     $3,
					Subname:      $6,
					Newname:      $8,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER TABLE IF_P EXISTS relation_expr RENAME CONSTRAINT name TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_TABCONSTRAINT,
					Relation:     $5,
					Subname:      $8,
					Newname:      $10,
					MissingOk:    true,
				}
				$$ = renameStmt
			}
	|	ALTER FOREIGN TABLE relation_expr RENAME opt_column name TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_COLUMN,
					RelationType: ast.OBJECT_FOREIGN_TABLE,
					Relation:     $4,
					Subname:      $7,
					Newname:      $9,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER FOREIGN TABLE IF_P EXISTS relation_expr RENAME opt_column name TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_COLUMN,
					RelationType: ast.OBJECT_FOREIGN_TABLE,
					Relation:     $6,
					Subname:      $9,
					Newname:      $11,
					MissingOk:    true,
				}
				$$ = renameStmt
			}
	|	ALTER RULE name ON qualified_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_RULE,
					Relation:     $5,
					Subname:      $3,
					Newname:      $8,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER TRIGGER name ON qualified_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_TRIGGER,
					Relation:     $5,
					Subname:      $3,
					Newname:      $8,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER EVENT TRIGGER name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_EVENT_TRIGGER,
					Object:       ast.NewString($4),
					Newname:      $7,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER ROLE RoleId RENAME TO RoleId
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_ROLE,
					Subname:      $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER USER RoleId RENAME TO RoleId
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_ROLE,
					Subname:      $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER TABLESPACE name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_TABLESPACE,
					Subname:      $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER STATISTICS any_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_STATISTIC_EXT,
					Object:       $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER TEXT_P SEARCH PARSER any_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_TSPARSER,
					Object:       $5,
					Newname:      $8,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER TEXT_P SEARCH DICTIONARY any_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_TSDICTIONARY,
					Object:       $5,
					Newname:      $8,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER TEXT_P SEARCH TEMPLATE any_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_TSTEMPLATE,
					Object:       $5,
					Newname:      $8,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER TEXT_P SEARCH CONFIGURATION any_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_TSCONFIGURATION,
					Object:       $5,
					Newname:      $8,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER TYPE_P any_name RENAME TO name
			{
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_TYPE,
					Object:       $3,
					Newname:      $6,
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	|	ALTER TYPE_P any_name RENAME ATTRIBUTE name TO name opt_drop_behavior
			{
			    rv, err := makeRangeVarFromAnyName($3, 0)
				if err != nil {
				    yylex.Error(err.Error())
				}
				renameStmt := &ast.RenameStmt{
					BaseNode:     ast.BaseNode{Tag: ast.T_RenameStmt},
					RenameType:   ast.OBJECT_ATTRIBUTE,
					RelationType: ast.OBJECT_TYPE,
					Relation:     rv,
					Subname:      $6,
					Newname:      $8,
					Behavior:     ast.DropBehavior($9),
					MissingOk:    false,
				}
				$$ = renameStmt
			}
	;

/*****************************************************************************
 *
 *		SUPPORTING GRAMMAR
 *
 *****************************************************************************/

ConstraintAttributeSpec:
			/* EMPTY */					{ $$ = 0 }
		|	ConstraintAttributeSpec ConstraintAttributeElem
			{
				// Combine constraint attribute bits
				newspec := $1 | $2

				// Check for conflicts (NOT DEFERRABLE + INITIALLY DEFERRED)
				if (newspec & (ast.CAS_NOT_DEFERRABLE | ast.CAS_INITIALLY_DEFERRED)) == (ast.CAS_NOT_DEFERRABLE | ast.CAS_INITIALLY_DEFERRED) {
					// This would be an error in real PostgreSQL parser
				}

				$$ = newspec
			}
		;

ConstraintAttributeElem:
			NOT DEFERRABLE				{ $$ = ast.CAS_NOT_DEFERRABLE }
		|	DEFERRABLE					{ $$ = ast.CAS_DEFERRABLE }
		|	INITIALLY IMMEDIATE			{ $$ = ast.CAS_INITIALLY_IMMEDIATE }
		|	INITIALLY DEFERRED			{ $$ = ast.CAS_INITIALLY_DEFERRED }
		|	NOT VALID					{ $$ = ast.CAS_NOT_VALID }
		|	NO INHERIT					{ $$ = ast.CAS_NO_INHERIT }
		;

reloptions:
			'(' reloption_list ')'		{ $$ = $2 }
		;

reloption_list:
			reloption_elem
				{
					$$ = ast.NewNodeList()
					$$.Append($1)
				}
		|	reloption_list ',' reloption_elem
				{
					$1.Append($3)
					$$ = $1
				}
		;

reloption_elem:
			ColLabel '=' def_arg
				{
					$$ = ast.NewDefElem($1, $3)
				}
		|	ColLabel
				{
					$$ = ast.NewDefElem($1, nil)
				}
		|	ColLabel '.' ColLabel '=' def_arg
				{
					$$ = ast.NewDefElemExtended($1, $3, $5, ast.DEFELEM_UNSPEC)
				}
		|	ColLabel '.' ColLabel
				{
					$$ = ast.NewDefElemExtended($1, $3, nil, ast.DEFELEM_UNSPEC)
				}
		;

/*****************************************************************************
 *
 *		CREATE FUNCTION/PROCEDURE statements
 *
 *****************************************************************************/

CreateFunctionStmt:
			CREATE opt_or_replace FUNCTION func_name func_args_with_defaults
			RETURNS func_return opt_createfunc_opt_list opt_routine_body
				{
					stmt := &ast.CreateFunctionStmt{
						IsProcedure: false,
						Replace: $2,
						FuncName: $4,
						Parameters: $5,
						ReturnType: $7,
						Options: $8,
						SQLBody: $9,
					}
					$$ = stmt
				}
		|	CREATE opt_or_replace FUNCTION func_name func_args_with_defaults
			RETURNS TABLE '(' table_func_column_list ')' opt_createfunc_opt_list opt_routine_body
				{
					// Handle RETURNS TABLE variant
					stmt := &ast.CreateFunctionStmt{
						IsProcedure: false,
						Replace: $2,
						FuncName: $4,
						Parameters: $5,
						ReturnType: nil, // TODO: Handle table return type
						Options: $11,
						SQLBody: $12,
					}
					// TODO: Process table_func_column_list into appropriate return type
					$$ = stmt
				}
		|	CREATE opt_or_replace FUNCTION func_name func_args_with_defaults
			opt_createfunc_opt_list opt_routine_body
				{
					// No explicit return type (for procedures disguised as functions)
					stmt := &ast.CreateFunctionStmt{
						IsProcedure: false,
						Replace: $2,
						FuncName: $4,
						Parameters: $5,
						ReturnType: nil,
						Options: $6,
						SQLBody: $7,
					}
					$$ = stmt
				}
		|	CREATE opt_or_replace PROCEDURE func_name func_args_with_defaults
			opt_createfunc_opt_list opt_routine_body
				{
					stmt := &ast.CreateFunctionStmt{
						IsProcedure: true,
						Replace: $2,
						FuncName: $4,
						Parameters: $5,
						ReturnType: nil,
						Options: $6,
						SQLBody: $7,
					}
					$$ = stmt
				}
		;

func_args_with_defaults:
			'(' func_args_with_defaults_list ')'		{ $$ = $2 }
		|	'(' ')'										{ $$ = nil }
		;

func_args_with_defaults_list:
			func_arg_with_default
				{
					$$ = ast.NewNodeList($1)
				}
		|	func_args_with_defaults_list ',' func_arg_with_default
				{
					$1.Append($3)
					$$ = $1
				}
		;

func_arg_with_default:
			func_arg
				{
					$$ = $1
				}
		|	func_arg DEFAULT a_expr
				{
					$1.DefExpr = $3
					$$ = $1
				}
		|	func_arg '=' a_expr
				{
					$1.DefExpr = $3
					$$ = $1
				}
		;

func_arg:
			arg_class param_name func_type
				{
					param := &ast.FunctionParameter{
						Mode: $1,
						Name: $2,
						ArgType: $3,
					}
					$$ = param
				}
		|	param_name arg_class func_type
				{
					param := &ast.FunctionParameter{
						Mode: $2,
						Name: $1,
						ArgType: $3,
					}
					$$ = param
				}
		|	param_name func_type
				{
					param := &ast.FunctionParameter{
						Mode: ast.FUNC_PARAM_DEFAULT,
						Name: $1,
						ArgType: $2,
					}
					$$ = param
				}
		|	arg_class func_type
				{
					param := &ast.FunctionParameter{
						Mode: $1,
						Name: "",
						ArgType: $2,
					}
					$$ = param
				}
		|	func_type
				{
					param := &ast.FunctionParameter{
						Mode: ast.FUNC_PARAM_DEFAULT,
						Name: "",
						ArgType: $1,
					}
					$$ = param
				}
		;

arg_class:
			IN_P								{ $$ = ast.FUNC_PARAM_IN }
		|	OUT_P								{ $$ = ast.FUNC_PARAM_OUT }
		|	INOUT								{ $$ = ast.FUNC_PARAM_INOUT }
		|	IN_P OUT_P							{ $$ = ast.FUNC_PARAM_INOUT }
		|	VARIADIC							{ $$ = ast.FUNC_PARAM_VARIADIC }
		;

func_return:
			func_type							{ $$ = $1 }
		;

func_type:
			Typename							{ $$ = $1 }
		|	type_function_name attrs '%' TYPE_P
				{
					// Handle %TYPE reference
					list := ast.NewNodeList()
					list.Append(ast.NewString($1))
					// Add attrs to the names list
					for _, attr := range $2.Items {
						list.Append(attr)
					}
					$$ = &ast.TypeName{
						BaseNode: ast.BaseNode{Tag: ast.T_TypeName},
						Names: list,
						PctType: true,
					}
				}
		|	SETOF type_function_name attrs '%' TYPE_P
				{
					// Handle SETOF %TYPE reference
					list := ast.NewNodeList()
					list.Append(ast.NewString($2))
					// Add attrs to the names list
					for _, attr := range $3.Items {
						list.Append(attr)
					}
					$$ = &ast.TypeName{
						BaseNode: ast.BaseNode{Tag: ast.T_TypeName},
						Names: list,
						Setof: true,
						PctType: true,
					}
				}
		;

opt_createfunc_opt_list:
			createfunc_opt_list					{ $$ = $1 }
		|	/* EMPTY */							{ $$ = nil }
		;

createfunc_opt_list:
			createfunc_opt_item
				{
					list := ast.NewNodeList()
					list.Append($1)
					$$ = list
				}
		|	createfunc_opt_list createfunc_opt_item
				{
					$1.Append($2)
					$$ = $1
				}
		;

createfunc_opt_item:
			AS func_as
				{
					$$ = ast.NewDefElem("as", $2)
				}
		|	LANGUAGE NonReservedWord_or_Sconst
				{
					$$ = ast.NewDefElem("language", ast.NewString($2))
				}
		|	TRANSFORM transform_type_list
				{
					$$ = ast.NewDefElem("transform", $2)
				}
		|	WINDOW
				{
					$$ = ast.NewDefElem("window", ast.NewBoolean(true))
				}
		|	common_func_opt_item
				{
					$$ = $1
				}
		;

func_as:
			Sconst
				{
					list := ast.NewNodeList()
					list.Append(ast.NewString($1))
					$$ = list
				}
		|	Sconst ',' Sconst
				{
					list := ast.NewNodeList()
					list.Append(ast.NewString($1))
					list.Append(ast.NewString($3))
					$$ = list
				}
		;

transform_type_list:
			FOR TYPE_P Typename
				{
					list := ast.NewNodeList()
					list.Append($3)
					$$ = list
				}
		|	transform_type_list ',' FOR TYPE_P Typename
				{
					$1.Append($5)
					$$ = $1
				}
		;

opt_routine_body:
			ReturnStmt
				{
					$$ = $1
				}
		|	BEGIN_P ATOMIC routine_body_stmt_list END_P
				{
					/*
					 * A compound statement is stored as a single-item list
					 * containing the list of statements as its member.  That
					 * way, the parse analysis code can tell apart an empty
					 * body from no body at all.
					 */
					list := ast.NewNodeList()
					list.Append($3)
					$$ = list
				}
		|	/* EMPTY */
				{
					$$ = nil
				}
		;

ReturnStmt:
		RETURN a_expr
			{
				$$ = ast.NewReturnStmt($2)
			}
		;

routine_body_stmt_list:
			routine_body_stmt_list routine_body_stmt ';'
				{
					/* As in stmtmulti, discard empty statements */
					if $2 != nil {
						$1.Append($2)
						$$ = $1
					} else {
						$$ = $1
					}
				}
		|	/* EMPTY */
				{
					$$ = ast.NewNodeList()
				}
		;

routine_body_stmt:
		stmt								{ $$ = $1 }
		|	ReturnStmt							{ $$ = $1 }
		;

FunctionSetResetClause:
			SET set_rest_more
				{
					$$ = $2
				}
			| VariableResetStmt
				{
					$$ = $1.(*ast.VariableSetStmt)
				}
		;

/*****************************************************************************
 *
 *		CREATE TRIGGER statements - Phase 3G
 *
 * Exactly matches PostgreSQL CREATE TRIGGER grammar from gram.y
 *****************************************************************************/

CreateTrigStmt:
			CREATE opt_or_replace TRIGGER name TriggerActionTime TriggerEvents ON
			qualified_name TriggerReferencing TriggerForSpec TriggerWhen
			EXECUTE FUNCTION_or_PROCEDURE func_name '(' TriggerFuncArgs ')'
				{
					// Extract events and columns from TriggerEvents list [events, columns]
					eventsList := $6
					events := eventsList.Items[0].(*ast.Integer).IVal
					var columns *ast.NodeList
					if eventsList.Items[1] != nil {
						columns = eventsList.Items[1].(*ast.NodeList)
					}

					stmt := &ast.CreateTriggerStmt{
						Replace: $2,
						IsConstraint: false,
						Trigname: $4,
						Relation: $8,
						Funcname: $14,
						Args: $16,
						Row: $10,
						Timing: int16($5),
						Events: int16(events),
						Columns: columns,
						WhenClause: $11,
						Transitions: $9,
						Deferrable: false,
						Initdeferred: false,
						Constrrel: nil,
					}
					$$ = stmt
				}
		|	CREATE opt_or_replace CONSTRAINT TRIGGER name AFTER TriggerEvents ON
			qualified_name OptConstrFromTable ConstraintAttributeSpec
			FOR EACH ROW TriggerWhen
			EXECUTE FUNCTION_or_PROCEDURE func_name '(' TriggerFuncArgs ')'
				{
					// Extract events and columns from TriggerEvents list [events, columns]
					eventsList := $7
					events := eventsList.Items[0].(*ast.Integer).IVal
					var columns *ast.NodeList
					if eventsList.Items[1] != nil {
						columns = eventsList.Items[1].(*ast.NodeList)
					}

					stmt := &ast.CreateTriggerStmt{
						Replace: $2,
						IsConstraint: true,
						Trigname: $5,
						Relation: $9,
						Funcname: $18,
						Args: $20,
						Row: true,
						Timing: int16(ast.TRIGGER_TIMING_AFTER),
						Events: int16(events),
						Columns: columns,
						WhenClause: $15,
						Transitions: nil,
						Deferrable: true, // Default for constraint triggers
						Initdeferred: false,
						Constrrel: $10,
					}
					$$ = stmt
				}
		;

TriggerActionTime:
			BEFORE								{ $$ = ast.TRIGGER_TIMING_BEFORE }
		|	AFTER								{ $$ = ast.TRIGGER_TIMING_AFTER }
		|	INSTEAD OF							{ $$ = ast.TRIGGER_TIMING_INSTEAD }
		;

TriggerEvents:
			TriggerOneEvent
				{ $$ = $1 }
		|	TriggerEvents OR TriggerOneEvent
				{
					// Extract event types and column lists from both sides
					events1 := $1.Items[0].(*ast.Integer).IVal
					events2 := $3.Items[0].(*ast.Integer).IVal
					columns1 := $1.Items[1]
					columns2 := $3.Items[1]

					// Check for duplicate events
					if events1 & events2 != 0 {
						// TODO: Generate parse error for duplicate trigger events
					}

					// Create combined result [events1|events2, combined_columns]
					list := ast.NewNodeList()
					list.Append(ast.NewInteger(events1 | events2))

					// Concatenate column lists (if any)
					if columns1 != nil && columns2 != nil {
						// Both have columns - concatenate
						combinedCols := columns1.(*ast.NodeList)
						if columns2List, ok := columns2.(*ast.NodeList); ok {
							for _, item := range columns2List.Items {
								combinedCols.Append(item)
							}
						}
						list.Append(combinedCols)
					} else if columns1 != nil {
						list.Append(columns1)
					} else if columns2 != nil {
						list.Append(columns2)
					} else {
						list.Append(nil)
					}

					$$ = list
				}
		;

TriggerOneEvent:
			INSERT
				{
					list := ast.NewNodeList()
					list.Append(ast.NewInteger(int(ast.TRIGGER_TYPE_INSERT)))
					list.Append(nil) // No columns for INSERT
					$$ = list
				}
		|	DELETE_P
				{
					list := ast.NewNodeList()
					list.Append(ast.NewInteger(int(ast.TRIGGER_TYPE_DELETE)))
					list.Append(nil) // No columns for DELETE
					$$ = list
				}
		|	UPDATE
				{
					list := ast.NewNodeList()
					list.Append(ast.NewInteger(int(ast.TRIGGER_TYPE_UPDATE)))
					list.Append(nil) // No columns for UPDATE
					$$ = list
				}
		|	UPDATE OF columnList
				{
					list := ast.NewNodeList()
					list.Append(ast.NewInteger(int(ast.TRIGGER_TYPE_UPDATE)))
					list.Append($3) // Column list for UPDATE OF
					$$ = list
				}
		|	TRUNCATE
				{
					list := ast.NewNodeList()
					list.Append(ast.NewInteger(int(ast.TRIGGER_TYPE_TRUNCATE)))
					list.Append(nil) // No columns for TRUNCATE
					$$ = list
				}
		;

TriggerReferencing:
			REFERENCING TriggerTransitions		{ $$ = $2 }
		|	/* EMPTY */							{ $$ = nil }
		;

TriggerTransitions:
			TriggerTransition					{
				list := ast.NewNodeList()
				list.Append($1)
				$$ = list
			}
		|	TriggerTransitions TriggerTransition	{
				$1.Append($2)
				$$ = $1
			}
		;

TriggerTransition:
			TransitionOldOrNew TransitionRowOrTable opt_as TransitionRelName
				{
					trans := &ast.TriggerTransition{
						Name: $4,
						IsNew: $1,
						IsTable: $2,
					}
					$$ = trans
				}
		;

TransitionOldOrNew:
			NEW			{ $$ = true }
		|	OLD			{ $$ = false }
		;

TransitionRowOrTable:
			TABLE		{ $$ = true }
		|	ROW			{ $$ = false }
		;

TransitionRelName:
			ColId		{ $$ = $1 }
		;

TriggerForSpec:
			FOR TriggerForOptEach TriggerForType
				{
					$$ = $3
				}
		|	/* EMPTY */
				{
					// If ROW/STATEMENT not specified, default to STATEMENT
					$$ = false
				}
		;

TriggerForOptEach:
			EACH
		|	/* EMPTY */
		;

TriggerForType:
			ROW					{ $$ = true }
		|	STATEMENT			{ $$ = false }
		;

TriggerWhen:
			WHEN '(' a_expr ')'				{ $$ = $3 }
		|	/* EMPTY */						{ $$ = nil }
		;

FUNCTION_or_PROCEDURE:
			FUNCTION
		|	PROCEDURE
		;

TriggerFuncArgs:
			TriggerFuncArg					{
				list := ast.NewNodeList()
				list.Append($1)
				$$ = list
			}
		|	TriggerFuncArgs ',' TriggerFuncArg	{
				$1.Append($3)
				$$ = $1
			}
		|	/* EMPTY */						{ $$ = nil }
		;

TriggerFuncArg:
			Iconst
				{
					$$ = ast.NewString(fmt.Sprintf("%d", $1))
				}
		|	FCONST								{ $$ = ast.NewString($1) }
		|	Sconst								{ $$ = ast.NewString($1) }
		|	ColLabel							{ $$ = ast.NewString($1) }
		;

OptConstrFromTable:
			FROM qualified_name				{ $$ = $2 }
		|	/* EMPTY */						{ $$ = nil }
		;

/*****************************************************************************
 *
 *		CREATE VIEW statements - Phase 3G
 *
 * Exactly matches PostgreSQL CREATE VIEW grammar from gram.y
 *****************************************************************************/

ViewStmt:
			CREATE OptTemp VIEW qualified_name opt_column_list opt_reloptions
			AS SelectStmt opt_check_option
				{
					// Apply OptTemp persistence to the view RangeVar
					view := $4
					view.RelPersistence = rune($2)
					stmt := &ast.ViewStmt{
						View: view,
						Aliases: $5,
						Query: $8,
						Replace: false,
						Options: $6,
						WithCheckOption: ast.ViewCheckOption($9),
					}
					$$ = stmt
				}
		|	CREATE OR REPLACE OptTemp VIEW qualified_name opt_column_list opt_reloptions
			AS SelectStmt opt_check_option
				{
					// Apply OptTemp persistence to the view RangeVar
					view := $6
					view.RelPersistence = rune($4)
					stmt := &ast.ViewStmt{
						View: view,
						Aliases: $7,
						Query: $10,
						Replace: true,
						Options: $8,
						WithCheckOption: ast.ViewCheckOption($11),
					}
					$$ = stmt
				}
		|	CREATE OptTemp RECURSIVE VIEW qualified_name '(' columnList ')' opt_reloptions
			AS SelectStmt opt_check_option
				{
					// RECURSIVE VIEW requires explicit column list
					view := $5
					view.RelPersistence = rune($2)
					stmt := &ast.ViewStmt{
						View: view,
						Aliases: $7,
						Query: $11,
						Replace: false,
						Options: $9,
						WithCheckOption: ast.ViewCheckOption($12),
					}
					$$ = stmt
				}
		|	CREATE OR REPLACE OptTemp RECURSIVE VIEW qualified_name '(' columnList ')' opt_reloptions
			AS SelectStmt opt_check_option
				{
					// RECURSIVE VIEW requires explicit column list
					view := $7
					view.RelPersistence = rune($4)
					stmt := &ast.ViewStmt{
						View: view,
						Aliases: $9,
						Query: $13,
						Replace: true,
						Options: $11,
						WithCheckOption: ast.ViewCheckOption($14),
					}
					$$ = stmt
				}
		;

opt_column_list:
			'(' columnList ')'					{ $$ = $2 }
		|	/* EMPTY */							{ $$ = nil }
		;

opt_check_option:
			WITH CHECK OPTION					{ $$ = int(ast.CASCADED_CHECK_OPTION) }
		|	WITH CASCADED CHECK OPTION			{ $$ = int(ast.CASCADED_CHECK_OPTION) }
		|	WITH LOCAL CHECK OPTION				{ $$ = int(ast.LOCAL_CHECK_OPTION) }
		|	/* EMPTY */							{ $$ = int(ast.NO_CHECK_OPTION) }
		;

/*****************************************************************************
 *
 *		CREATE SCHEMA statements - Phase 3G
 *
 *****************************************************************************/

CreateSchemaStmt:
			CREATE SCHEMA opt_single_name AUTHORIZATION RoleSpec OptSchemaEltList
				{
					n := ast.NewCreateSchemaStmt($3, false)
					n.Authrole = $5
					n.SchemaElts = $6
					$$ = n
				}
			| CREATE SCHEMA ColId OptSchemaEltList
				{
					n := ast.NewCreateSchemaStmt($3, false)
					n.Authrole = nil
					n.SchemaElts = $4
					$$ = n
				}
			| CREATE SCHEMA IF_P NOT EXISTS opt_single_name AUTHORIZATION RoleSpec OptSchemaEltList
				{
					n := ast.NewCreateSchemaStmt($6, true)
					n.Authrole = $8
					n.SchemaElts = $9
					$$ = n
				}
			| CREATE SCHEMA IF_P NOT EXISTS ColId OptSchemaEltList
				{
					n := ast.NewCreateSchemaStmt($6, true)
					n.Authrole = nil
					n.SchemaElts = $7
					$$ = n
				}
		;

/*****************************************************************************
 *
 *		CREATE DATABASE statements - Phase 3J
 *
 *****************************************************************************/

CreatedbStmt:
		CREATE DATABASE name opt_with createdb_opt_list
			{
				n := ast.NewCreatedbStmt($3, $5)
				$$ = n
			}
		;

/*****************************************************************************
 *
 *		DROP DATABASE statements - Phase 3J
 *
 *****************************************************************************/

DropdbStmt: DROP DATABASE name
			{
				n := ast.NewDropdbStmt($3, false, nil)
				$$ = n
			}
		| DROP DATABASE IF_P EXISTS name
			{
				n := ast.NewDropdbStmt($5, true, nil)
				$$ = n
			}
		| DROP DATABASE name opt_with '(' drop_option_list ')'
			{
				n := ast.NewDropdbStmt($3, false, $6)
				$$ = n
			}
		| DROP DATABASE IF_P EXISTS name opt_with '(' drop_option_list ')'
			{
				n := ast.NewDropdbStmt($5, true, $8)
				$$ = n
			}
		;

drop_option_list:
		drop_option							{ $$ = ast.NewNodeList($1) }
		| drop_option_list ',' drop_option	{ $$ = $1; $$.Append($3) }
		;

drop_option:
		FORCE									{ $$ = ast.NewDefElem("force", nil) }
		;

/*****************************************************************************
 *
 *		DROP TABLESPACE statements - Phase 3J
 *
 *****************************************************************************/

DropTableSpaceStmt: DROP TABLESPACE name
			{
				n := ast.NewDropTableSpaceStmt($3, false)
				$$ = n
			}
		| DROP TABLESPACE IF_P EXISTS name
			{
				n := ast.NewDropTableSpaceStmt($5, true)
				$$ = n
			}
		;

/*****************************************************************************
 *
 *		DROP OWNED / REASSIGN OWNED statements - Phase 3J
 *
 *****************************************************************************/

DropOwnedStmt:
		DROP OWNED BY role_list opt_drop_behavior
			{
				n := ast.NewDropOwnedStmt($4, $5)
				$$ = n
			}
		;

ReassignOwnedStmt:
		REASSIGN OWNED BY role_list TO RoleSpec
			{
				n := ast.NewReassignOwnedStmt($4, $6)
				$$ = n
			}
		;

/*****************************************************************************
 *
 *		CREATE DOMAIN statements - Phase 3G
 *
 *****************************************************************************/

CreateDomainStmt:
		CREATE DOMAIN_P any_name opt_as Typename ColQualList
			{
				n := ast.NewCreateDomainStmt($3, $5)
				// Use SplitColQualList to separate constraints and collate clause
				constraints, collClause := SplitColQualList($6)
				n.Constraints = constraints
				n.CollClause = collClause
				$$ = n
			}
	;

/*****************************************************************************
 *
 *		ALTER DOMAIN statements - Phase 3G
 *
 *****************************************************************************/

AlterDomainStmt:
		ALTER DOMAIN_P any_name alter_column_default
			{
				n := ast.NewAlterDomainStmt('T', $3)
				n.Def = $4
				$$ = n
			}
	|	ALTER DOMAIN_P any_name DROP NOT NULL_P
			{
				n := ast.NewAlterDomainStmt('N', $3)
				$$ = n
			}
	|	ALTER DOMAIN_P any_name SET NOT NULL_P
			{
				n := ast.NewAlterDomainStmt('O', $3)
				$$ = n
			}
	|	ALTER DOMAIN_P any_name ADD_P DomainConstraint
			{
				n := ast.NewAlterDomainStmt('C', $3)
				n.Def = $5
				$$ = n
			}
	|	ALTER DOMAIN_P any_name DROP CONSTRAINT name opt_drop_behavior
			{
				n := ast.NewAlterDomainStmt('X', $3)
				n.Name = $6
				n.Behavior = $7
				n.MissingOk = false
				$$ = n
			}
	|	ALTER DOMAIN_P any_name DROP CONSTRAINT IF_P EXISTS name opt_drop_behavior
			{
				n := ast.NewAlterDomainStmt('X', $3)
				n.Name = $8
				n.Behavior = $9
				n.MissingOk = true
				$$ = n
			}
	|	ALTER DOMAIN_P any_name VALIDATE CONSTRAINT name
			{
				n := ast.NewAlterDomainStmt('V', $3)
				n.Name = $6
				$$ = n
			}
	;

/*****************************************************************************
 *
 *		CREATE TYPE statements - Phase 3G
 *
 *****************************************************************************/

DefineStmt:
		CREATE opt_or_replace AGGREGATE func_name aggr_args definition
			{
				// Store the full aggr_args result [args, position_indicator] for proper deparsing
				n := ast.NewDefineStmt(ast.OBJECT_AGGREGATE, false, $4, $5, $6, false, $2)
				$$ = n
			}
	|	CREATE opt_or_replace AGGREGATE func_name old_aggr_definition
			{
				// old-style (pre-8.2) syntax for CREATE AGGREGATE
				n := ast.NewDefineStmt(ast.OBJECT_AGGREGATE, true, $4, nil, $5, false, $2)
				$$ = n
			}
	|	CREATE OPERATOR any_operator definition
			{
				n := ast.NewDefineStmt(ast.OBJECT_OPERATOR, false, $3, nil, $4, false, false)
				$$ = n
			}
	|	CREATE TYPE_P any_name definition
			{
				n := ast.NewDefineStmt(ast.OBJECT_TYPE, false, $3, nil, $4, false, false)
				$$ = n
			}
	|	CREATE TYPE_P any_name
			{
				// Shell type (identified by lack of definition)
				n := ast.NewDefineStmt(ast.OBJECT_TYPE, false, $3, nil, nil, false, false)
				$$ = n
			}
	|	CREATE TEXT_P SEARCH PARSER any_name definition
			{
				n := ast.NewDefineStmt(ast.OBJECT_TSPARSER, false, $5, nil, $6, false, false)
				$$ = n
			}
	|	CREATE TEXT_P SEARCH DICTIONARY any_name definition
			{
				n := ast.NewDefineStmt(ast.OBJECT_TSDICTIONARY, false, $5, nil, $6, false, false)
				$$ = n
			}
	|	CREATE TEXT_P SEARCH TEMPLATE any_name definition
			{
				n := ast.NewDefineStmt(ast.OBJECT_TSTEMPLATE, false, $5, nil, $6, false, false)
				$$ = n
			}
	|	CREATE TEXT_P SEARCH CONFIGURATION any_name definition
			{
				n := ast.NewDefineStmt(ast.OBJECT_TSCONFIGURATION, false, $5, nil, $6, false, false)
				$$ = n
			}
	|	CREATE COLLATION any_name definition
			{
				n := ast.NewDefineStmt(ast.OBJECT_COLLATION, false, $3, nil, $4, false, false)
				$$ = n
			}
	|	CREATE COLLATION IF_P NOT EXISTS any_name definition
			{
				n := ast.NewDefineStmt(ast.OBJECT_COLLATION, false, $6, nil, $7, true, false)
				$$ = n
			}
	|	CREATE COLLATION any_name FROM any_name
			{
				n := ast.NewDefineStmt(ast.OBJECT_COLLATION, false, $3, nil, ast.NewNodeList($5), false, false)
				$$ = n
			}
	|	CREATE COLLATION IF_P NOT EXISTS any_name FROM any_name
			{
				n := ast.NewDefineStmt(ast.OBJECT_COLLATION, false, $6, nil, ast.NewNodeList($8), true, false)
				$$ = n
			}
	|	CREATE TYPE_P any_name AS '(' OptTableFuncElementList ')'
			{
				typevar, err := makeRangeVarFromAnyName($3, 0)
				if err != nil {
					yylex.Error(fmt.Sprintf("invalid type name: %v", err))
					return 1
				}

				n := ast.NewCompositeTypeStmt(typevar, $6)
				$$ = n
			}
	|	CREATE TYPE_P any_name AS ENUM_P '(' opt_enum_val_list ')'
			{
				n := ast.NewCreateEnumStmt($3, $7)
				$$ = n
			}
	|	CREATE TYPE_P any_name AS RANGE definition
			{
				n := ast.NewCreateRangeStmt($3, $6)
				$$ = n
			}
	;

/*****************************************************************************
 *
 *		CREATE SEQUENCE
 *
 *****************************************************************************/

CreateSeqStmt:
		CREATE OptTemp SEQUENCE qualified_name OptSeqOptList
			{
				n := ast.NewCreateSeqStmt($4, $5, 0, false, false)
				$$ = n
			}
	|	CREATE OptTemp SEQUENCE IF_P NOT EXISTS qualified_name OptSeqOptList
			{
				n := ast.NewCreateSeqStmt($7, $8, 0, false, true)
				$$ = n
			}
		;

OptSeqOptList:
			SeqOptList								{ $$ = $1 }
		|	/* EMPTY */								{ $$ = nil }
		;

/*****************************************************************************
 *
 *		ALTER SEQUENCE
 *
 *****************************************************************************/

AlterSeqStmt:
		ALTER SEQUENCE qualified_name SeqOptList
			{
				$$ = ast.NewAlterSeqStmt($3, $4, false, false)
			}
	|	ALTER SEQUENCE IF_P EXISTS qualified_name SeqOptList
			{
				$$ = ast.NewAlterSeqStmt($5, $6, false, true)
			}
		;

/*****************************************************************************
 *
 *		CREATE EXTENSION
 *
 *****************************************************************************/

CreateExtensionStmt:
		CREATE EXTENSION name opt_with create_extension_opt_list
			{
				n := ast.NewCreateExtensionStmt($3, false, $5)
				$$ = n
			}
	|	CREATE EXTENSION IF_P NOT EXISTS name opt_with create_extension_opt_list
			{
				n := ast.NewCreateExtensionStmt($6, true, $8)
				$$ = n
			}
		;

create_extension_opt_list:
			create_extension_opt_list create_extension_opt_item
			{
				if $1 == nil {
					$$ = ast.NewNodeList($2)
				} else {
					$1.Append($2)
					$$ = $1
				}
			}
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

create_extension_opt_item:
			SCHEMA name
			{
				$$ = ast.NewDefElem("schema", ast.NewString($2))
			}
		|	VERSION_P NonReservedWord_or_Sconst
			{
				$$ = ast.NewDefElem("version", ast.NewString($2))
			}
		|	FROM NonReservedWord_or_Sconst
			{
				yylex.Error("CREATE EXTENSION ... FROM is no longer supported")
				return 1;
			}
		|	CASCADE
			{
				$$ = ast.NewDefElem("cascade", ast.NewBoolean(true))
			}
		;

/*****************************************************************************
 *
 *		ALTER EXTENSION
 *
 *****************************************************************************/

AlterExtensionStmt:
		ALTER EXTENSION name UPDATE alter_extension_opt_list
			{
				n := ast.NewAlterExtensionStmt($3, $5)
				$$ = n
			}
		;

alter_extension_opt_list:
			alter_extension_opt_list alter_extension_opt_item
			{
				if $1 == nil {
					$$ = ast.NewNodeList($2)
				} else {
					$1.Append($2)
					$$ = $1
				}
			}
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

alter_extension_opt_item:
			TO NonReservedWord_or_Sconst
			{
				$$ = ast.NewDefElem("to", ast.NewString($2))
			}
		;

/*****************************************************************************
 *
 *		ALTER EXTENSION contents
 *
 *****************************************************************************/

AlterExtensionContentsStmt:
		ALTER EXTENSION name add_drop object_type_name name
			{
				$$ = ast.NewAlterExtensionContentsStmt($3, $4 != 0, int($5), ast.NewString($6))
			}
	|	ALTER EXTENSION name add_drop object_type_any_name any_name
			{
				$$ = ast.NewAlterExtensionContentsStmt($3, $4 != 0, int($5), $6)
			}
	|	ALTER EXTENSION name add_drop AGGREGATE aggregate_with_argtypes
			{
				$$ = ast.NewAlterExtensionContentsStmt($3, $4 != 0, int(ast.OBJECT_AGGREGATE), $6)
			}
	|	ALTER EXTENSION name add_drop CAST '(' Typename AS Typename ')'
			{
				// CAST takes two TypeNames as a NodeList
				list := ast.NewNodeList($7)
				list.Append($9)
				$$ = ast.NewAlterExtensionContentsStmt($3, $4 != 0, int(ast.OBJECT_CAST), list)
			}
	|	ALTER EXTENSION name add_drop DOMAIN_P Typename
			{
				$$ = ast.NewAlterExtensionContentsStmt($3, $4 != 0, int(ast.OBJECT_DOMAIN), $6)
			}
	|	ALTER EXTENSION name add_drop FUNCTION function_with_argtypes
			{
				$$ = ast.NewAlterExtensionContentsStmt($3, $4 != 0, int(ast.OBJECT_FUNCTION), $6)
			}
	|	ALTER EXTENSION name add_drop OPERATOR operator_with_argtypes
			{
				$$ = ast.NewAlterExtensionContentsStmt($3, $4 != 0, int(ast.OBJECT_OPERATOR), $6)
			}
	|	ALTER EXTENSION name add_drop OPERATOR CLASS any_name USING name
			{
				// OPERATOR CLASS takes method name + class name as NodeList
				list := ast.NewNodeList(ast.NewString($9)) // method first
				for _, item := range $7.Items {
					list.Append(item) // then class name parts
				}
				$$ = ast.NewAlterExtensionContentsStmt($3, $4 != 0, int(ast.OBJECT_OPCLASS), list)
			}
	|	ALTER EXTENSION name add_drop OPERATOR FAMILY any_name USING name
			{
				// OPERATOR FAMILY takes method name + family name as NodeList
				list := ast.NewNodeList(ast.NewString($9)) // method first
				for _, item := range $7.Items {
					list.Append(item) // then family name parts
				}
				$$ = ast.NewAlterExtensionContentsStmt($3, $4 != 0, int(ast.OBJECT_OPFAMILY), list)
			}
	|	ALTER EXTENSION name add_drop PROCEDURE function_with_argtypes
			{
				$$ = ast.NewAlterExtensionContentsStmt($3, $4 != 0, int(ast.OBJECT_PROCEDURE), $6)
			}
	|	ALTER EXTENSION name add_drop ROUTINE function_with_argtypes
			{
				$$ = ast.NewAlterExtensionContentsStmt($3, $4 != 0, int(ast.OBJECT_ROUTINE), $6)
			}
	| ALTER EXTENSION name add_drop TRANSFORM FOR Typename LANGUAGE name
			{
				list := ast.NewNodeList($7, ast.NewString($9))
				$$ = ast.NewAlterExtensionContentsStmt($3, $4 != 0, int(ast.OBJECT_TRANSFORM), list)
			}
	|	ALTER EXTENSION name add_drop TYPE_P Typename
			{
				$$ = ast.NewAlterExtensionContentsStmt($3, $4 != 0, int(ast.OBJECT_TYPE), $6)
			}
		;

/*****************************************************************************
 *
 * FOREIGN DATA WRAPPER support
 *
 *****************************************************************************/

CreateFdwStmt: CREATE FOREIGN DATA_P WRAPPER name opt_fdw_options create_generic_options
			{
				$$ = ast.NewCreateFdwStmt($5, $6, $7)
			}
		;

AlterFdwStmt: ALTER FOREIGN DATA_P WRAPPER name opt_fdw_options alter_generic_options
			{
				$$ = ast.NewAlterFdwStmt($5, $6, $7)
			}
		| ALTER FOREIGN DATA_P WRAPPER name fdw_options
			{
				$$ = ast.NewAlterFdwStmt($5, $6, nil)
			}
		;

fdw_option:
		HANDLER handler_name			{ $$ = ast.NewDefElem("handler", $2) }
	|	NO HANDLER						{ $$ = ast.NewDefElem("handler", nil) }
	|	VALIDATOR handler_name			{ $$ = ast.NewDefElem("validator", $2) }
	|	NO VALIDATOR					{ $$ = ast.NewDefElem("validator", nil) }
	;

fdw_options:
		fdw_option						{ $$ = ast.NewNodeList($1) }
	|	fdw_options fdw_option			{ $1.Append($2); $$ = $1 }
	;

opt_fdw_options:
		fdw_options						{ $$ = $1 }
	|	/* EMPTY */						{ $$ = nil }
	;

handler_name:
		name							{ $$ = ast.NewNodeList(ast.NewString($1)) }
	|	name attrs						{
										result := ast.NewNodeList(ast.NewString($1))
										for _, item := range $2.Items {
											result.Append(item)
										}
										$$ = result
									}
	;

/*****************************************************************************
 *
 * CREATE/ALTER FOREIGN SERVER support
 *
 *****************************************************************************/

CreateForeignServerStmt: CREATE SERVER name opt_type opt_foreign_server_version
						 FOREIGN DATA_P WRAPPER name create_generic_options
			{
				$$ = ast.NewCreateForeignServerStmt($3, $4, $5, $9, $10, false)
			}
		| CREATE SERVER IF_P NOT EXISTS name opt_type opt_foreign_server_version
						 FOREIGN DATA_P WRAPPER name create_generic_options
			{
				$$ = ast.NewCreateForeignServerStmt($6, $7, $8, $12, $13, true)
			}
		;

AlterForeignServerStmt: ALTER SERVER name foreign_server_version alter_generic_options
			{
				$$ = ast.NewAlterForeignServerStmt($3, $4, $5, true)
			}
		| ALTER SERVER name foreign_server_version
			{
				$$ = ast.NewAlterForeignServerStmt($3, $4, nil, true)
			}
		| ALTER SERVER name alter_generic_options
			{
				$$ = ast.NewAlterForeignServerStmt($3, "", $4, false)
			}
		;

opt_type:
		TYPE_P Sconst				{ $$ = $2 }
	|	/* EMPTY */					{ $$ = "" }
	;

foreign_server_version:
		VERSION_P Sconst			{ $$ = $2 }
	|	VERSION_P NULL_P			{ $$ = "" }
	;

opt_foreign_server_version:
		foreign_server_version		{ $$ = $1 }
	|	/* EMPTY */					{ $$ = "" }
	;

/*****************************************************************************
 *
 * CREATE FOREIGN TABLE support
 *
 *****************************************************************************/

CreateForeignTableStmt:
		CREATE FOREIGN TABLE qualified_name
			'(' OptTableElementList ')'
			OptInherit SERVER name create_generic_options
			{
				$$ = ast.NewCreateForeignTableStmt($4, $6, $8, $10, $11, nil, false)
			}
	|	CREATE FOREIGN TABLE IF_P NOT EXISTS qualified_name
			'(' OptTableElementList ')'
			OptInherit SERVER name create_generic_options
			{
				$$ = ast.NewCreateForeignTableStmt($7, $9, $11, $13, $14, nil, true)
			}
	|	CREATE FOREIGN TABLE qualified_name
			PARTITION OF qualified_name OptTypedTableElementList PartitionBoundSpec
			SERVER name create_generic_options
			{
				$$ = ast.NewCreateForeignTableStmt($4, $8, ast.NewNodeList($7), $11, $12, $9, false)
			}
	|	CREATE FOREIGN TABLE IF_P NOT EXISTS qualified_name
			PARTITION OF qualified_name OptTypedTableElementList PartitionBoundSpec
			SERVER name create_generic_options
			{
				$$ = ast.NewCreateForeignTableStmt($7, $11, ast.NewNodeList($10), $14, $15, $12, true)
			}
	;

/*****************************************************************************
 *
 * IMPORT FOREIGN SCHEMA support
 *
 *****************************************************************************/

ImportForeignSchemaStmt:
		IMPORT_P FOREIGN SCHEMA name import_qualification
		FROM SERVER name INTO name create_generic_options
			{
				stmt := ast.NewImportForeignSchemaStmt(
					ast.NewString($8),     // server name
					ast.NewString($4),     // remote schema
					ast.NewString($10),    // local schema
					$5.typ,                // list type
					$5.tableNames,         // table list
					$11,                   // options
				)
				$$ = stmt
			}
		;

import_qualification:
		import_qualification_type '(' relation_expr_list ')'
			{
				qual := &ImportQual{
					typ:        $1,
					tableNames: $3,
				}
				$$ = qual
			}
		| /* EMPTY */
			{
				qual := &ImportQual{
					typ:        ast.FDW_IMPORT_SCHEMA_ALL,
					tableNames: nil,
				}
				$$ = qual
			}
		;

import_qualification_type:
		LIMIT TO						{ $$ = ast.FDW_IMPORT_SCHEMA_LIMIT_TO }
		| EXCEPT						{ $$ = ast.FDW_IMPORT_SCHEMA_EXCEPT }
		;

/*****************************************************************************
 *
 * CREATE/ALTER/DROP USER MAPPING support
 *
 *****************************************************************************/

CreateUserMappingStmt: CREATE USER MAPPING FOR auth_ident SERVER name create_generic_options
			{
				$$ = ast.NewCreateUserMappingStmt($5, $7, $8, false)
			}
		| CREATE USER MAPPING IF_P NOT EXISTS FOR auth_ident SERVER name create_generic_options
			{
				$$ = ast.NewCreateUserMappingStmt($8, $10, $11, true)
			}
		;

AlterUserMappingStmt: ALTER USER MAPPING FOR auth_ident SERVER name alter_generic_options
			{
				$$ = ast.NewAlterUserMappingStmt($5, $7, $8)
			}
		;

DropUserMappingStmt: DROP USER MAPPING FOR auth_ident SERVER name
			{
				$$ = ast.NewDropUserMappingStmt($5, $7, false)
			}
		| DROP USER MAPPING IF_P EXISTS FOR auth_ident SERVER name
			{
				$$ = ast.NewDropUserMappingStmt($7, $9, true)
			}
		;

auth_ident:
		RoleSpec					{ $$ = $1 }
	|	USER						{ $$ = ast.NewRoleSpec(ast.ROLESPEC_CURRENT_USER, "") }
	;

aggr_args:
		'(' '*' ')'
			{
				// For aggregates like COUNT(*)
				// Return a list with [nil, -1] matching PostgreSQL's list_make2(NIL, makeInteger(-1))
				$$ = ast.NewNodeList(nil, ast.NewInteger(-1))
			}
	|	'(' aggr_args_list ')'
			{
				// Regular aggregate arguments
				// Return a list with [args, -1] matching PostgreSQL's list_make2($2, makeInteger(-1))
				$$ = ast.NewNodeList($2, ast.NewInteger(-1))
			}
	|	'(' ORDER BY aggr_args_list ')'
			{
				// Ordered-set aggregate without direct arguments
				// Return a list with [args, 0] matching PostgreSQL's list_make2($4, makeInteger(0))
				$$ = ast.NewNodeList($4, ast.NewInteger(0))
			}
	|	'(' aggr_args_list ORDER BY aggr_args_list ')'
			{
				// Hypothetical-set aggregate
				// This is the only case requiring consistency checking in PostgreSQL
				result, err := makeOrderedSetArgs($2, $5)
				if err != nil {
					yylex.Error(err.Error())
					return 1
				}
				$$ = result
			}
	;

aggr_args_list:
		aggr_arg
			{
				$$ = ast.NewNodeList($1)
			}
	|	aggr_args_list ',' aggr_arg
			{
				$1.Append($3)
				$$ = $1
			}
	;

aggr_arg:
		func_arg
			{
				$$ = $1
			}
	;

old_aggr_definition:
		'(' old_aggr_list ')'
			{
				$$ = $2
			}
	;

old_aggr_list:
		old_aggr_elem
			{
				$$ = ast.NewNodeList($1)
			}
	|	old_aggr_list ',' old_aggr_elem
			{
				$1.Append($3)
				$$ = $1
			}
	;

old_aggr_elem:
		IDENT '=' def_arg
			{
				$$ = ast.NewDefElem($1, $3)
			}
	;

AlterEnumStmt:
		ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists Sconst
			{
				n := ast.NewAlterEnumStmt($3)
				n.NewVal = $7
				n.SkipIfNewValExists = $6 != 0
				n.NewValIsAfter = true
				$$ = n
			}
	|	ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists Sconst BEFORE Sconst
			{
				n := ast.NewAlterEnumStmt($3)
				n.NewVal = $7
				n.NewValNeighbor = $9
				n.NewValIsAfter = false
				n.SkipIfNewValExists = $6 != 0
				$$ = n
			}
	|	ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists Sconst AFTER Sconst
			{
				n := ast.NewAlterEnumStmt($3)
				n.NewVal = $7
				n.NewValNeighbor = $9
				n.NewValIsAfter = true
				n.SkipIfNewValExists = $6 != 0
				$$ = n
			}
	|	ALTER TYPE_P any_name RENAME VALUE_P Sconst TO Sconst
			{
				n := ast.NewAlterEnumStmt($3)
				n.OldVal = $6
				n.NewVal = $8
				n.SkipIfNewValExists = false
				$$ = n
			}
	|	ALTER TYPE_P any_name DROP VALUE_P Sconst
			{
				// Following PostgreSQL's approach - DROP VALUE is parsed but not implemented
				// PostgreSQL throws an error saying "dropping an enum value is not implemented"
				yylex.Error("dropping an enum value is not implemented")
				return 1
			}
	;

/*****************************************************************************
 *
 *		Supporting rules for CREATE DOMAIN and CREATE TYPE
 *
 *****************************************************************************/

opt_enum_val_list:
		enum_val_list
			{ $$ = $1 }
	|	/* EMPTY */
			{ $$ = nil }
	;

enum_val_list:
		Sconst
			{ $$ = ast.NewNodeList(ast.NewString($1)) }
	|	enum_val_list ',' Sconst
			{ $1.Append(ast.NewString($3)); $$ = $1 }
	;

DomainConstraint:
		CONSTRAINT name DomainConstraintElem
			{
				if constraint, ok := $3.(*ast.Constraint); ok {
					constraint.Conname = $2
					$$ = constraint
				} else {
					$$ = $3
				}
			}
	|	DomainConstraintElem
			{ $$ = $1 }
	;

DomainConstraintElem:
		CHECK '(' a_expr ')' ConstraintAttributeSpec
			{
				n := ast.NewConstraint(ast.CONSTR_CHECK)
				n.RawExpr = $3
				n.CookedExpr = ""  // Empty string, not nil
				// Process constraint attributes from $5
				processConstraintAttributeSpec($5, n)
				// PostgreSQL: n->initially_valid = !n->skip_validation
				n.InitiallyValid = !n.SkipValidation
				$$ = n
			}
	|	NOT NULL_P ConstraintAttributeSpec
			{
				n := ast.NewConstraint(ast.CONSTR_NOTNULL)
				// In PostgreSQL, domain NOT NULL constraints have keys = list_make1(makeString("value"))
				n.Keys = ast.NewNodeList(ast.NewString("value"))
				// Process constraint attributes from $3
				processConstraintAttributeSpec($3, n)
				// PostgreSQL sets initially_valid = true for NOT NULL (no NOT VALID support yet)
				n.InitiallyValid = true
				$$ = n
			}
	;

/*****************************************************************************
 *
 *		CREATE MATERIALIZED VIEW statements - Phase 3G
 *
 *****************************************************************************/

CreateMatViewStmt:
		CREATE OptNoLog MATERIALIZED VIEW create_mv_target AS SelectStmt opt_with_data
				{
					ctas := ast.NewCreateTableAsStmt($7, $5, ast.OBJECT_MATVIEW, false, false)
					/* cram additional flags into the IntoClause */
					$5.Rel.RelPersistence = $2
					$5.SkipData = !$8
					$$ = ctas
				}
		| CREATE OptNoLog MATERIALIZED VIEW IF_P NOT EXISTS create_mv_target AS SelectStmt opt_with_data
				{
					ctas := ast.NewCreateTableAsStmt($10, $8, ast.OBJECT_MATVIEW, false, true)
					/* cram additional flags into the IntoClause */
					$8.Rel.RelPersistence = $2
					$8.SkipData = !$11
					$$ = ctas
				}
		;

RefreshMatViewStmt:
			REFRESH MATERIALIZED VIEW opt_concurrently qualified_name opt_with_data
				{
					n := ast.NewRefreshMatViewStmt($4, !$6, $5)
					$$ = n
				}
		;

/*****************************************************************************
 *
 *		CREATE ASSERTION statement
 *		Note: Not yet implemented in PostgreSQL, returns error
 *
 *****************************************************************************/

CreateAssertionStmt:
		CREATE ASSERTION any_name CHECK '(' a_expr ')' ConstraintAttributeSpec
			{
				yylex.Error("CREATE ASSERTION is not yet implemented")
				return 1
				// PostgreSQL doesn't actually implement CREATE ASSERTION yet.
				// $$ = ast.NewCreateAssertionStmt($3, $6, nil)
			}
		;

/*****************************************************************************
 *
 *		CREATE TABLE AS and CREATE TABLE ... AS SELECT
 *
 *****************************************************************************/

CreateAsStmt:
		CREATE OptTemp TABLE create_as_target AS SelectStmt opt_with_data
			{
				ctas := ast.NewCreateTableAsStmt($6, $4, ast.OBJECT_TABLE, false, false)
				/* cram additional flags into the IntoClause */
				if $4.Rel != nil {
					$4.Rel.RelPersistence = $2
				}
				$4.SkipData = !$7
				$$ = ctas
			}
		| CREATE OptTemp TABLE IF_P NOT EXISTS create_as_target AS SelectStmt opt_with_data
			{
				ctas := ast.NewCreateTableAsStmt($9, $7, ast.OBJECT_TABLE, false, true)
				/* cram additional flags into the IntoClause */
				if $7.Rel != nil {
					$7.Rel.RelPersistence = $2
				}
				$7.SkipData = !$10
				$$ = ctas
			}
		;

/*****************************************************************************
 *
 *		RULE statements
 *
 *****************************************************************************/

RuleStmt:	CREATE opt_or_replace RULE name AS
			ON event TO qualified_name where_clause
			DO opt_instead RuleActionList
			{
				n := &ast.RuleStmt{
					BaseNode:    ast.BaseNode{Tag: ast.T_RuleStmt},
					Replace:     $2,
					Relation:    $9,
					Rulename:    $4,
					WhereClause: $10,
					Event:       ast.CmdType($7),
					Instead:     $12,
					Actions:     $13,
				}
				$$ = n
			}
		;

RuleActionList:
			NOTHING									{ $$ = nil }
			| RuleActionStmt						{ $$ = ast.NewNodeList($1) }
			| '(' RuleActionMulti ')'				{ $$ = $2 }
		;

RuleActionMulti:
			RuleActionMulti ';' RuleActionStmtOrEmpty
				{
					if $3 != nil {
						if $1 != nil {
							$1.Items = append($1.Items, $3)
							$$ = $1
						} else {
							$$ = ast.NewNodeList($3)
						}
					}
				}
			| RuleActionStmtOrEmpty
				{
					if $1 != nil {
						$$ = ast.NewNodeList($1)
					} else {
						$$ = nil
					}
				}
		;

RuleActionStmt:
			SelectStmt
			| InsertStmt
			| UpdateStmt
			| DeleteStmt
			| NotifyStmt
		;

RuleActionStmtOrEmpty:
			RuleActionStmt							{ $$ = $1 }
			|	/*EMPTY*/							{ $$ = nil }
		;

opt_instead:
			INSTEAD									{ $$ = true }
			| ALSO									{ $$ = false }
			| /*EMPTY*/								{ $$ = false }
		;

OptNoLog:	UNLOGGED					{ $$ = ast.RELPERSISTENCE_UNLOGGED }
			| /*EMPTY*/					{ $$ = ast.RELPERSISTENCE_PERMANENT }
		;

create_mv_target:
			qualified_name opt_column_list table_access_method_clause opt_reloptions OptTableSpace
				{
					$$ = &ast.IntoClause{
						Rel: $1,
						ColNames: $2,
						AccessMethod: $3,
						Options: $4,
						TableSpaceName: $5,
					}
				}
		;

opt_with_data:
		WITH DATA_P								{ $$ = true }
		|	WITH NO DATA_P						{ $$ = false }
		|	/* EMPTY */							{ $$ = true }
		;

OptSchemaEltList:
			OptSchemaEltList schema_stmt
				{
					if $1 == nil {
						$$ = ast.NewNodeList($2)
					} else {
						$1.Append($2)
						$$ = $1
					}
				}
			| /* EMPTY */
				{ $$ = nil }
		;

schema_stmt:
			CreateStmt
			| IndexStmt
			| CreateTrigStmt
			| ViewStmt
		;

set_rest_more:
			generic_set							{ $$ = $1 }
		|	var_name FROM CURRENT_P
				{
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_CURRENT, $1, nil, false)
				}
		|	TIME ZONE zone_value
				{
					args := ast.NewNodeList($3)
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_VALUE, "timezone", args, false)
				}
		|	CATALOG_P Sconst
				{
					args := ast.NewNodeList(ast.NewString($2))
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_VALUE, "catalog", args, false)
				}
		|	SCHEMA Sconst
				{
					args := ast.NewNodeList(ast.NewString($2))
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_VALUE, "search_path", args, false)
				}
		|	NAMES opt_encoding
				{
					var args *ast.NodeList
					if $2 != "" {
						args = ast.NewNodeList(ast.NewString($2))
					}
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_VALUE, "client_encoding", args, false)
				}
		|	ROLE NonReservedWord_or_Sconst
				{
					args := ast.NewNodeList(ast.NewString($2))
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_VALUE, "role", args, false)
				}
		|	SESSION AUTHORIZATION NonReservedWord_or_Sconst
				{
					args := ast.NewNodeList(ast.NewString($3))
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_VALUE, "session_authorization", args, false)
				}
		|	SESSION AUTHORIZATION DEFAULT
				{
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_DEFAULT, "session_authorization", nil, false)
				}
		|	XML_P OPTION document_or_content
				{
					var value string
					if $3 == int(ast.XMLOPTION_DOCUMENT) {
						value = "document"
					} else {
						value = "content"
					}
					args := ast.NewNodeList(ast.NewString(value))
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_VALUE, "xmloption", args, false)
				}
		|	TRANSACTION SNAPSHOT Sconst
				{
					args := ast.NewNodeList(ast.NewString($3))
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_VALUE, "transaction_snapshot", args, false)
				}
		;

generic_set:
			var_name TO var_list
				{
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_VALUE, $1, $3, false)
				}
		|	var_name '=' var_list
				{
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_VALUE, $1, $3, false)
				}
		|	var_name TO DEFAULT
				{
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_DEFAULT, $1, nil, false)
				}
		|	var_name '=' DEFAULT
				{
					$$ = ast.NewVariableSetStmt(ast.VAR_SET_DEFAULT, $1, nil, false)
				}
		;

var_name:
			ColId
				{
					$$ = $1
				}
		|	var_name '.' ColId
				{
					$$ = $1 + "." + $3
				}
		;

var_list:
			var_value
				{
					list := ast.NewNodeList()
					list.Append($1)
					$$ = list
				}
		|	var_list ',' var_value
				{
					$1.Append($3)
					$$ = $1
				}
		;

var_value:
			opt_boolean_or_string					{ $$ = ast.NewString($1) }
		|	NumericOnly								{ $$ = $1 }
		;

zone_value:
			Sconst									{ $$ = ast.NewString($1) }
		|	IDENT									{ $$ = ast.NewString($1) }
		|	ConstInterval Sconst opt_interval
			{
				t := $1
				t.Typmods = $3
				stringConst := ast.NewA_Const(ast.NewString($2), 0)
				$$ = ast.NewTypeCast(stringConst, t, 0)
			}
		|	ConstInterval '(' Iconst ')' Sconst
			{
				t := $1
				// INTERVAL_FULL_RANGE equivalent and precision
				t.Typmods = ast.NewNodeList(ast.NewInteger(ast.INTERVAL_FULL_RANGE), ast.NewInteger($3))
				stringConst := ast.NewA_Const(ast.NewString($5), 0)
				$$ = ast.NewTypeCast(stringConst, t, 0)
			}
		|	NumericOnly								{ $$ = $1 }
		|	DEFAULT									{ $$ = ast.NewString("default") }
		|	LOCAL									{ $$ = ast.NewString("local") }
		;

opt_encoding:
			Sconst									{ $$ = $1 }
		|	DEFAULT									{ $$ = "default" }
		|	/* EMPTY */								{ $$ = "" }
		;

NonReservedWord_or_Sconst:
			NonReservedWord							{ $$ = $1 }
		|	Sconst									{ $$ = $1 }
		;

NonReservedWord:
			IDENT									{ $$ = $1 }
		|	unreserved_keyword						{ $$ = $1 }
		|	col_name_keyword						{ $$ = $1 }
		|	type_func_name_keyword					{ $$ = $1 }
		;

document_or_content:
			DOCUMENT_P								{ $$ = int(ast.XMLOPTION_DOCUMENT) }
		|	CONTENT_P								{ $$ = int(ast.XMLOPTION_CONTENT) }
		;

transaction_mode_list:
			transaction_mode_item
				{
					list := ast.NewNodeList()
					list.Append($1)
					$$ = list
				}
		|	transaction_mode_list ',' transaction_mode_item
				{
					$1.Append($3)
					$$ = $1
				}
		|	transaction_mode_list transaction_mode_item
				{
					$1.Append($2)
					$$ = $1
				}
		;

transaction_mode_item:
			ISOLATION LEVEL iso_level
				{
					$$ = ast.NewDefElem("transaction_isolation", ast.NewString($3))
				}
		|	READ ONLY
				{
					$$ = ast.NewDefElem("transaction_read_only", ast.NewBoolean(true))
				}
		|	READ WRITE
				{
					$$ = ast.NewDefElem("transaction_read_only", ast.NewBoolean(false))
				}
		|	DEFERRABLE
				{
					$$ = ast.NewDefElem("transaction_deferrable", ast.NewBoolean(true))
				}
		|	NOT DEFERRABLE
				{
					$$ = ast.NewDefElem("transaction_deferrable", ast.NewBoolean(false))
				}
		;

iso_level:
			READ UNCOMMITTED						{ $$ = "read uncommitted" }
		|	READ COMMITTED							{ $$ = "read committed" }
		|	REPEATABLE READ							{ $$ = "repeatable read" }
		|	SERIALIZABLE							{ $$ = "serializable" }
		;

transaction_mode_list_or_empty:
			transaction_mode_list				{ $$ = $1 }
		|	/* EMPTY */							{ $$ = nil }
		;

def_arg:
			func_type					{ $$ = $1 }
		|	reserved_keyword			{ $$ = ast.NewString($1) }
		|	qual_all_Op					{
				$$ = $1
			}
		|	NumericOnly					{ $$ = $1 }
		|	Sconst						{ $$ = ast.NewString($1) }
		|	NONE						{ $$ = ast.NewString("none") }
		;

opt_definition:
			WITH definition				{ $$ = $2 }
		|	/* EMPTY */					{ $$ = nil }
		;

alter_using:
			USING a_expr				{ $$ = $2 }
		|	/* EMPTY */					{ $$ = nil }
		;

alter_generic_options:
			OPTIONS '(' alter_generic_option_list ')'	{ $$ = $3 }
		;

alter_generic_option_list:
			alter_generic_option_elem
				{
					$$ = ast.NewNodeList()
					$$.Append($1)
				}
		|	alter_generic_option_list ',' alter_generic_option_elem
				{
					$1.Append($3)
					$$ = $1
				}
		;

alter_generic_option_elem:
			generic_option_elem
				{
					$$ = $1
				}
		|	SET generic_option_elem
				{
					elem := $2
					elem.Defaction = ast.DEFELEM_SET
					$$ = elem
				}
		|	ADD_P generic_option_elem
				{
					elem := $2
					elem.Defaction = ast.DEFELEM_ADD
					$$ = elem
				}
		|	DROP generic_option_name
				{
					$$ = ast.NewDefElemExtended("", $2, nil, ast.DEFELEM_DROP)
				}
		;

set_access_method_name:
			ColId					{ $$ = $1 }
		|	DEFAULT					{ $$ = "" }
		;

replica_identity:
			NOTHING					{ $$ = ast.NewReplicaIdentityStmt(ast.REPLICA_IDENTITY_NOTHING, "") }
		|	FULL					{ $$ = ast.NewReplicaIdentityStmt(ast.REPLICA_IDENTITY_FULL, "") }
		|	DEFAULT					{ $$ = ast.NewReplicaIdentityStmt(ast.REPLICA_IDENTITY_DEFAULT, "") }
		|	USING INDEX name		{ $$ = ast.NewReplicaIdentityStmt(ast.REPLICA_IDENTITY_INDEX, $3) }
		;

definition:
			'(' def_list ')'			{ $$ = $2 }
		;

def_list:
			def_elem
				{
					$$ = ast.NewNodeList()
					$$.Append($1)
				}
		|	def_list ',' def_elem
				{
					$1.Append($3)
					$$ = $1
				}
		;

def_elem:
			ColLabel					{ $$ = ast.NewDefElem($1, nil) }
		|	ColLabel '=' def_arg		{ $$ = ast.NewDefElem($1, $3) }
		;

/*****************************************************************************
 *
 * CREATE/ALTER EVENT TRIGGER support
 *
 *****************************************************************************/

CreateEventTrigStmt:
		CREATE EVENT TRIGGER name ON ColLabel EXECUTE FUNCTION_or_PROCEDURE func_name '(' ')'
			{
				$$ = ast.NewCreateEventTrigStmt($4, $6, $9, nil)
			}
	|	CREATE EVENT TRIGGER name ON ColLabel WHEN event_trigger_when_list EXECUTE FUNCTION_or_PROCEDURE func_name '(' ')'
			{
				$$ = ast.NewCreateEventTrigStmt($4, $6, $11, $8)
			}

AlterEventTrigStmt:
		ALTER EVENT TRIGGER name enable_trigger
			{
				$$ = ast.NewAlterEventTrigStmt($4, ast.TriggerFires($5))
			}


enable_trigger:
		ENABLE_P				{ $$ = int(ast.TRIGGER_FIRES_ON_ORIGIN) }
	|	ENABLE_P REPLICA		{ $$ = int(ast.TRIGGER_FIRES_ON_REPLICA) }
	|	ENABLE_P ALWAYS			{ $$ = int(ast.TRIGGER_FIRES_ALWAYS) }
	|	DISABLE_P				{ $$ = int(ast.TRIGGER_DISABLED) }
	;

event_trigger_when_list:
		event_trigger_when_item
			{
				$$ = ast.NewNodeList()
				$$.Append($1)
			}
	|	event_trigger_when_list AND event_trigger_when_item
			{
				$1.Append($3)
				$$ = $1
			}
		;

event_trigger_when_item:
		ColId IN_P '(' event_trigger_value_list ')'
			{
				$$ = ast.NewDefElem($1, $4)
			}
		;

event_trigger_value_list:
		SCONST
			{
				$$ = ast.NewNodeList()
				$$.Append(ast.NewString($1))
			}
	|	event_trigger_value_list ',' SCONST
			{
				$1.Append(ast.NewString($3))
				$$ = $1
			}
		;

/*****************************************************************************
 *
 * CREATE/ALTER TABLESPACE support
 *
 *****************************************************************************/

CreateTableSpaceStmt:
		CREATE TABLESPACE name OptTableSpaceOwner LOCATION SCONST opt_reloptions
			{
				$$ = ast.NewCreateTableSpaceStmt($3, $4, $6, $7)
			}
		;

OptTableSpaceOwner:
		OWNER RoleSpec				{ $$ = $2 }
		| /*EMPTY */				{ $$ = nil }
		;

AlterTblSpcStmt:
		ALTER TABLESPACE name SET reloptions
			{
				$$ = ast.NewAlterTableSpaceStmt($3, $5, false)
			}
		| ALTER TABLESPACE name RESET reloptions
			{
				$$ = ast.NewAlterTableSpaceStmt($3, $5, true)
			}
		;

/*****************************************************************************
 *
 * CREATE/ALTER POLICY support
 *
 *****************************************************************************/

CreatePolicyStmt:
		CREATE POLICY name ON qualified_name RowSecurityDefaultPermissive
			RowSecurityDefaultForCmd RowSecurityDefaultToRole
			RowSecurityOptionalExpr RowSecurityOptionalWithCheck
			{
				$$ = ast.NewCreatePolicyStmt($3, $5, $6, $7, $8, $9, $10)
			}
		;

AlterPolicyStmt:
		ALTER POLICY name ON qualified_name RowSecurityOptionalToRole
			RowSecurityOptionalExpr RowSecurityOptionalWithCheck
			{
				$$ = ast.NewAlterPolicyStmt($3, $5, $6, $7, $8)
			}
		;

RowSecurityDefaultPermissive:
		AS IDENT
			{
				// Check for "permissive" or "restrictive" (case-insensitive)
				if strings.EqualFold($2, "permissive") {
					$$ = true
				} else if strings.EqualFold($2, "restrictive") {
					$$ = false
				} else {
					// Parser will error on invalid value
					yylex.Error("unrecognized row security option")
					return 1
				}
			}
		| /* EMPTY */				{ $$ = true }
		;

RowSecurityDefaultForCmd:
		FOR row_security_cmd		{ $$ = $2 }
		| /* EMPTY */				{ $$ = "all" }
		;

row_security_cmd:
		ALL						{ $$ = "all" }
		| SELECT					{ $$ = "select" }
		| INSERT					{ $$ = "insert" }
		| UPDATE					{ $$ = "update" }
		| DELETE_P					{ $$ = "delete" }
		;

RowSecurityDefaultToRole:
		TO role_list				{ $$ = $2 }
		| /* EMPTY */				{
			// Default to PUBLIC when no TO clause is specified
			publicRole := ast.NewRoleSpec(ast.ROLESPEC_PUBLIC, "")
			$$ = ast.NewNodeList(publicRole)
		}
		;

RowSecurityOptionalToRole:
		TO role_list				{ $$ = $2 }
		| /* EMPTY */				{ $$ = nil }
		;

RowSecurityOptionalExpr:
		USING '(' a_expr ')'		{ $$ = $3 }
		| /* EMPTY */				{ $$ = nil }
		;

RowSecurityOptionalWithCheck:
		WITH CHECK '(' a_expr ')'	{ $$ = $4 }
		| /* EMPTY */				{ $$ = nil }
		;

/*****************************************************************************
 *
 * CREATE ACCESS METHOD support
 *
 *****************************************************************************/

CreateAmStmt:
		CREATE ACCESS METHOD name TYPE_P am_type HANDLER handler_name
			{
				$$ = ast.NewCreateAmStmt($4, ast.AmType($6), $8)
			}
		;

am_type:
		INDEX					{ $$ = int(ast.AMTYPE_INDEX) }
		| TABLE					{ $$ = int(ast.AMTYPE_TABLE) }
		;

/*****************************************************************************
 *
 * CREATE/ALTER STATISTICS support
 *
 *****************************************************************************/

CreateStatsStmt:
		CREATE STATISTICS opt_qualified_name opt_name_list ON stats_params FROM from_list
			{
				$$ = ast.NewCreateStatsStmt($3, $4, $6, $8, "", false, false)
			}
		| CREATE STATISTICS IF_P NOT EXISTS any_name opt_name_list ON stats_params FROM from_list
			{
				$$ = ast.NewCreateStatsStmt($6, $7, $9, $11, "", false, true)
			}
		;

AlterStatsStmt:
		ALTER STATISTICS any_name SET STATISTICS set_statistics_value
			{
				$$ = ast.NewAlterStatsStmt($3, $6, false)
			}
		| ALTER STATISTICS IF_P EXISTS any_name SET STATISTICS set_statistics_value
			{
				$$ = ast.NewAlterStatsStmt($5, $8, true)
			}
		;

stats_params:
		stats_param						{ $$ = ast.NewNodeList(); $$.Append($1) }
		| stats_params ',' stats_param		{ $1.Append($3); $$ = $1 }
		;

stats_param:
		ColId								{ $$ = ast.NewStatsElem($1) }
		| func_expr_windowless				{ $$ = ast.NewStatsElemExpr($1) }
		| '(' a_expr ')'					{ $$ = ast.NewStatsElemExpr(ast.NewParenExpr($2,0)) }
		;

/*****************************************************************************
 *
 * CREATE/ALTER PUBLICATION support
 *
 *****************************************************************************/

CreatePublicationStmt:
		CREATE PUBLICATION name opt_definition
			{
				$$ = ast.NewCreatePublicationStmt($3, nil, false, $4)
			}
		| CREATE PUBLICATION name FOR ALL TABLES opt_definition
			{
				$$ = ast.NewCreatePublicationStmt($3, nil, true, $7)
			}
		| CREATE PUBLICATION name FOR pub_obj_list opt_definition
			{
				$$ = ast.NewCreatePublicationStmt($3, $5, false, $6)
			}
		;

AlterPublicationStmt:
		ALTER PUBLICATION name SET definition
			{
				$$ = ast.NewAlterPublicationStmt($3, $5, nil, ast.AP_SetOptions)
			}
		| ALTER PUBLICATION name ADD_P pub_obj_list
			{
				$$ = ast.NewAlterPublicationStmt($3, nil, $5, ast.AP_AddObjects)
			}
		| ALTER PUBLICATION name SET pub_obj_list
			{
				$$ = ast.NewAlterPublicationStmt($3, nil, $5, ast.AP_SetObjects)
			}
		| ALTER PUBLICATION name DROP pub_obj_list
			{
				$$ = ast.NewAlterPublicationStmt($3, nil, $5, ast.AP_DropObjects)
			}
		;

pub_obj_list:
		PublicationObjSpec						{ $$ = ast.NewNodeList(); $$.Append($1) }
		| pub_obj_list ',' PublicationObjSpec	{ $1.Append($3); $$ = $1 }
		;

PublicationObjSpec:
		TABLE relation_expr opt_column_list OptWhereClause
			{
				pubTable := ast.NewPublicationTable($2, $4, $3)
				$$ = ast.NewPublicationObjSpecTable(ast.PUBLICATIONOBJ_TABLE, pubTable)
			}
		| TABLES IN_P SCHEMA ColId
			{
				$$ = ast.NewPublicationObjSpecName(ast.PUBLICATIONOBJ_TABLES_IN_SCHEMA, $4)
			}
		| TABLES IN_P SCHEMA CURRENT_SCHEMA
			{
				$$ = ast.NewPublicationObjSpec(ast.PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA)
			}
		| ColId opt_column_list OptWhereClause
			{
				// If either a row filter or column list is specified, create a PublicationTable object
				if $2 != nil || $3 != nil {
					// Create a simple RangeVar from the ColId
					rangeVar := ast.NewRangeVar($1, "", "")
					pubTable := ast.NewPublicationTable(rangeVar, $3, $2)
					$$ = ast.NewPublicationObjSpecTable(ast.PUBLICATIONOBJ_CONTINUATION, pubTable)
				} else {
					$$ = ast.NewPublicationObjSpecName(ast.PUBLICATIONOBJ_CONTINUATION, $1)
				}
			}
		| ColId indirection opt_column_list OptWhereClause
			{
				rangeVar := makeRangeVarFromQualifiedName($1, $2, -1)
				pubTable := ast.NewPublicationTable(rangeVar, $4, $3)
				$$ = ast.NewPublicationObjSpecTable(ast.PUBLICATIONOBJ_CONTINUATION, pubTable)
			}
		| extended_relation_expr opt_column_list OptWhereClause
			{
				pubTable := ast.NewPublicationTable($1, $3, $2)
				$$ = ast.NewPublicationObjSpecTable(ast.PUBLICATIONOBJ_CONTINUATION, pubTable)
			}
		| CURRENT_SCHEMA
			{
				$$ = ast.NewPublicationObjSpec(ast.PUBLICATIONOBJ_CONTINUATION)
			}
		;

OptWhereClause:
			WHERE '(' a_expr ')'					{ $$ = $3; }
			| /*EMPTY*/								{ $$ = nil; }
		;

/*****************************************************************************
 *
 * CREATE/ALTER SUBSCRIPTION support
 *
 *****************************************************************************/

CreateSubscriptionStmt:
		CREATE SUBSCRIPTION name CONNECTION Sconst PUBLICATION name_list opt_definition
			{
				$$ = ast.NewCreateSubscriptionStmt($3, $5, $7, $8)
			}
		;

AlterSubscriptionStmt:
		ALTER SUBSCRIPTION name SET definition
			{
				$$ = ast.NewAlterSubscriptionStmt($3, ast.ALTER_SUBSCRIPTION_OPTIONS, "", nil, $5)
			}
		| ALTER SUBSCRIPTION name CONNECTION Sconst
			{
				$$ = ast.NewAlterSubscriptionStmt($3, ast.ALTER_SUBSCRIPTION_CONNECTION, $5, nil, nil)
			}
		| ALTER SUBSCRIPTION name REFRESH PUBLICATION opt_definition
			{
				$$ = ast.NewAlterSubscriptionStmt($3, ast.ALTER_SUBSCRIPTION_REFRESH, "", nil, $6)
			}
		| ALTER SUBSCRIPTION name ADD_P PUBLICATION name_list opt_definition
			{
				$$ = ast.NewAlterSubscriptionStmt($3, ast.ALTER_SUBSCRIPTION_ADD_PUBLICATION, "", $6, $7)
			}
		| ALTER SUBSCRIPTION name DROP PUBLICATION name_list opt_definition
			{
				$$ = ast.NewAlterSubscriptionStmt($3, ast.ALTER_SUBSCRIPTION_DROP_PUBLICATION, "", $6, $7)
			}
		| ALTER SUBSCRIPTION name SET PUBLICATION name_list opt_definition
			{
				$$ = ast.NewAlterSubscriptionStmt($3, ast.ALTER_SUBSCRIPTION_SET_PUBLICATION, "", $6, $7)
			}
		| ALTER SUBSCRIPTION name ENABLE_P
			{
				enableOpt := ast.NewNodeList()
				enableOpt.Append(ast.NewDefElem("enabled", ast.NewBoolean(true)))
				$$ = ast.NewAlterSubscriptionStmt($3, ast.ALTER_SUBSCRIPTION_ENABLED, "", nil, enableOpt)
			}
		| ALTER SUBSCRIPTION name DISABLE_P
			{
				disableOpt := ast.NewNodeList()
				disableOpt.Append(ast.NewDefElem("enabled", ast.NewBoolean(false)))
				$$ = ast.NewAlterSubscriptionStmt($3, ast.ALTER_SUBSCRIPTION_ENABLED, "", nil, disableOpt)
			}
		| ALTER SUBSCRIPTION name SKIP definition
			{
				$$ = ast.NewAlterSubscriptionStmt($3, ast.ALTER_SUBSCRIPTION_SKIP, "", nil, $5)
			}
		;

/*****************************************************************************
 *
 * CREATE CAST support
 *
 *****************************************************************************/

CreateCastStmt:
		CREATE CAST '(' Typename AS Typename ')' WITH FUNCTION function_with_argtypes cast_context
			{
				$$ = ast.NewCreateCastStmt($4, $6, $10, ast.CoercionContext($11), false)
			}
		| CREATE CAST '(' Typename AS Typename ')' WITHOUT FUNCTION cast_context
			{
				$$ = ast.NewCreateCastStmt($4, $6, nil, ast.CoercionContext($10), false)
			}
		| CREATE CAST '(' Typename AS Typename ')' WITH INOUT cast_context
			{
				$$ = ast.NewCreateCastStmt($4, $6, nil, ast.CoercionContext($10), true)
			}
		;

cast_context:
		AS IMPLICIT_P				{ $$ = int(ast.COERCION_IMPLICIT) }
		| AS ASSIGNMENT				{ $$ = int(ast.COERCION_ASSIGNMENT) }
		| /*EMPTY*/					{ $$ = int(ast.COERCION_EXPLICIT) }
		;

/*****************************************************************************
 *
 * CREATE OPERATOR CLASS support
 *
 *****************************************************************************/

CreateOpClassStmt:
		CREATE OPERATOR CLASS any_name opt_default FOR TYPE_P Typename
		USING name opt_opfamily AS opclass_item_list
			{
				$$ = ast.NewCreateOpClassStmt($4, $11, $10, $8, $13, $5)
			}
		;

opt_opfamily:
		FAMILY any_name				{ $$ = $2 }
		| /*EMPTY*/					{ $$ = nil }
		;

opclass_item_list:
		opclass_item						{ $$ = ast.NewNodeList($1) }
		| opclass_item_list ',' opclass_item	{ $1.Append($3); $$ = $1 }
		;

opclass_item:
		OPERATOR Iconst any_operator opclass_purpose opt_recheck
			{
				// Create ObjectWithArgs for simple operator
				owa := ast.NewObjectWithArgs($3, nil, false, -1)
				$$ = ast.NewOpClassItemOperator($2, owa, $4)
			}
		| OPERATOR Iconst operator_with_argtypes opclass_purpose opt_recheck
			{
				$$ = ast.NewOpClassItemOperator($2, $3, $4)
			}
		| FUNCTION Iconst function_with_argtypes
			{
				$$ = ast.NewOpClassItemFunction($2, $3, nil)
			}
		| FUNCTION Iconst '(' type_list ')' function_with_argtypes
			{
				$$ = ast.NewOpClassItemFunction($2, $6, $4)
			}
		| STORAGE Typename
			{
				$$ = ast.NewOpClassItemStorage($2)
			}
		;

opt_default:
		DEFAULT						{ $$ = true }
		| /*EMPTY*/					{ $$ = false }
		;

opclass_purpose:
		FOR SEARCH					{ $$ = nil }
		| FOR ORDER BY any_name		{ $$ = $4 }
		| /*EMPTY*/					{ $$ = nil }
		;

type_list:
		Typename						{ $$ = ast.NewNodeList($1) }
		| type_list ',' Typename		{ $1.Append($3); $$ = $1 }
		;

/*****************************************************************************
 *
 * CREATE OPERATOR FAMILY support
 *
 *****************************************************************************/

CreateOpFamilyStmt:
		CREATE OPERATOR FAMILY any_name USING name
			{
				$$ = ast.NewCreateOpFamilyStmt($4, $6)
			}
		;

/*****************************************************************************
 *
 * ALTER OPERATOR FAMILY support
 *
 *****************************************************************************/

AlterOpFamilyStmt:
		ALTER OPERATOR FAMILY any_name USING name ADD_P opclass_item_list
			{
				$$ = ast.NewAlterOpFamilyStmt($4, $6, false, $8)
			}
		| ALTER OPERATOR FAMILY any_name USING name DROP opclass_drop_list
			{
				$$ = ast.NewAlterOpFamilyStmt($4, $6, true, $8)
			}
		;

opclass_drop_list:
		opclass_drop						{ $$ = ast.NewNodeList($1) }
		| opclass_drop_list ',' opclass_drop	{ $1.Append($3); $$ = $1 }
		;

opclass_drop:
		OPERATOR Iconst '(' type_list ')'
			{
				// Create ObjectWithArgs for operator with args
				owa := ast.NewObjectWithArgs(nil, $4, false, -1)
				$$ = ast.NewOpClassItemOperator($2, owa, nil)
			}
		| FUNCTION Iconst '(' type_list ')'
			{
				// Create ObjectWithArgs for function with args
				owa := ast.NewObjectWithArgs(nil, $4, false, -1)
				$$ = ast.NewOpClassItemFunction($2, owa, nil)
			}
		;

/*****************************************************************************
 *
 * CREATE CONVERSION support
 *
 *****************************************************************************/

CreateConversionStmt:
		CREATE opt_default CONVERSION_P any_name FOR Sconst TO Sconst FROM any_name
			{
				$$ = ast.NewCreateConversionStmt($4, $6, $8, $10, $2)
			}
		;

/*****************************************************************************
 *
 * CREATE TRANSFORM support
 *
 *****************************************************************************/

CreateTransformStmt:
		CREATE opt_or_replace TRANSFORM FOR Typename LANGUAGE name '(' transform_element_list ')'
			{
				$$ = ast.NewCreateTransformStmt($2, $5, $7, linitial($9), lsecond($9))
			}
		;

transform_element_list:
		FROM SQL_P WITH FUNCTION function_with_argtypes ',' TO SQL_P WITH FUNCTION function_with_argtypes
			{
				$$ = ast.NewNodeList()
				$$.Append($5)  // fromsql
				$$.Append($11) // tosql
			}
		| TO SQL_P WITH FUNCTION function_with_argtypes ',' FROM SQL_P WITH FUNCTION function_with_argtypes
			{
				$$ = ast.NewNodeList()
				$$.Append($11) // fromsql
				$$.Append($5)  // tosql
			}
		| FROM SQL_P WITH FUNCTION function_with_argtypes
			{
				$$ = ast.NewNodeList()
				$$.Append($5)  // fromsql
				$$.Append(nil) // tosql
			}
		| TO SQL_P WITH FUNCTION function_with_argtypes
			{
				$$ = ast.NewNodeList()
				$$.Append(nil) // fromsql
				$$.Append($5)  // tosql
			}
		;

/*****************************************************************************
 *
 * CREATE LANGUAGE support
 *
 *****************************************************************************/

CreatePLangStmt:
		CREATE opt_or_replace opt_trusted opt_procedural LANGUAGE name
			{
				// Parameterless CREATE LANGUAGE is now treated as CREATE EXTENSION
				$$ = ast.NewCreateExtensionStmt($6, $2, nil)
			}
		| CREATE opt_or_replace opt_trusted opt_procedural LANGUAGE name
		  HANDLER handler_name opt_inline_handler opt_validator
			{
				$$ = ast.NewCreatePLangStmt($2, $6, $8, $9, $10, $3)
			}
		;

opt_trusted:
		TRUSTED						{ $$ = true }
		| /*EMPTY*/					{ $$ = false }
		;


opt_inline_handler:
		INLINE_P handler_name		{ $$ = $2 }
		| /*EMPTY*/					{ $$ = nil }
		;

opt_validator:
		validator_clause			{ $$ = $1 }
		| /*EMPTY*/					{ $$ = nil }
		;

validator_clause:
		VALIDATOR handler_name					{ $$ = $2 }
		| NO VALIDATOR							{ $$ = nil }

set_quantifier:
		ALL										{ $$ = ast.SET_QUANTIFIER_ALL }
		| DISTINCT								{ $$ = ast.SET_QUANTIFIER_DISTINCT }
		| /*EMPTY*/								{ $$ = ast.SET_QUANTIFIER_DEFAULT }
		;

group_clause:
		GROUP_P BY set_quantifier group_by_list
			{
				$$ = &ast.GroupClause{
					Distinct: $3 == ast.SET_QUANTIFIER_DISTINCT,
					List:     $4,
				}
			}
	| /* EMPTY */
			{
				$$ = nil
			}
	;

group_by_list:
		group_by_item							{ $$ = ast.NewNodeList($1) }
	|	group_by_list ',' group_by_item			{ $1.Append($3); $$ = $1 }
	;

group_by_item:
		a_expr									{ $$ = $1 }
	|	empty_grouping_set						{ $$ = $1 }
	|	cube_clause								{ $$ = $1 }
	|	rollup_clause							{ $$ = $1 }
	|	grouping_sets_clause					{ $$ = $1 }
	;

empty_grouping_set:
		'(' ')'
			{
				$$ = ast.NewGroupingSet(ast.GROUPING_SET_EMPTY, ast.NewNodeList(), 0)
			}
	;

rollup_clause:
		ROLLUP '(' expr_list ')'
			{
				$$ = ast.NewGroupingSet(ast.GROUPING_SET_ROLLUP, $3, 0)
			}
	;

cube_clause:
		CUBE '(' expr_list ')'
			{
				$$ = ast.NewGroupingSet(ast.GROUPING_SET_CUBE, $3, 0)
			}
	;

grouping_sets_clause:
		GROUPING SETS '(' group_by_list ')'
			{
				$$ = ast.NewGroupingSet(ast.GROUPING_SET_SETS, $4, 0)
			}
	;

/*
 * HAVING clause implementation
 */
having_clause:
		HAVING a_expr							{ $$ = $2 }
	|	/* EMPTY */								{ $$ = nil }
	;

/*
 * ORDER BY clause implementation
 */
sort_clause:
		ORDER BY sortby_list					{ $$ = $3 }
	;

sortby_list:
		sortby									{ $$ = ast.NewNodeList($1) }
	|	sortby_list ',' sortby					{ $1.Append($3); $$ = $1 }
	;

sortby:
		a_expr USING qual_all_Op opt_nulls_order
			{
				sortBy := ast.NewSortBy($1, ast.SORTBY_USING, ast.SortByNulls($4), 0)
				// Use qual_all_Op (NodeList) directly for UseOp
				sortBy.UseOp = $3
				$$ = sortBy
			}
	| a_expr opt_asc_desc opt_nulls_order
			{
				$$ = ast.NewSortBy($1, ast.SortByDir($2), ast.SortByNulls($3), 0)
			}
	;

/*
 * WINDOW FUNCTIONS
 * From postgres/src/backend/parser/gram.y:16203+
 */
window_clause:
		WINDOW window_definition_list
			{ $$ = $2 }
	|	/* EMPTY */
			{ $$ = nil }
	;

window_definition_list:
		window_definition
			{
				$$ = ast.NewNodeList()
				$$.Items = append($$.Items, $1)
			}
	|	window_definition_list ',' window_definition
			{
				$$ = $1
				$$.Items = append($$.Items, $3)
			}
	;

window_definition:
		ColId AS window_specification
			{
				n := $3
				n.Name = $1
				$$ = n
			}
	;

over_clause:
		OVER window_specification
			{ $$ = $2 }
	|	OVER ColId
			{
				n := ast.NewWindowDef("", -1)
				n.Refname = $2
				n.FrameOptions = ast.FRAMEOPTION_DEFAULTS
				$$ = n
			}
	|	/* EMPTY */
			{ $$ = nil }
	;

window_specification:
		'(' opt_existing_window_name opt_partition_clause opt_sort_clause opt_frame_clause ')'
			{
				n := ast.NewWindowDef("", -1)
				n.Refname = $2
				n.PartitionClause = $3
				n.OrderClause = $4

				n.FrameOptions = $5.FrameOptions
				n.StartOffset = $5.StartOffset
				n.EndOffset = $5.EndOffset
				$$ = n
			}
	;

opt_existing_window_name:
		ColId		{ $$ = $1 }
	|	/* EMPTY */ %prec Op { $$ = "" }
	;

opt_partition_clause:
		PARTITION BY expr_list	{ $$ = $3 }
	|	/* EMPTY */				{ $$ = nil }
	;

opt_frame_clause:
		RANGE frame_extent opt_window_exclusion_clause
			{
				n := $2
				n.FrameOptions |= ast.FRAMEOPTION_NONDEFAULT | ast.FRAMEOPTION_RANGE
				n.FrameOptions |= $3
				$$ = n
			}
	|	ROWS frame_extent opt_window_exclusion_clause
			{
				n := $2
				n.FrameOptions |= ast.FRAMEOPTION_NONDEFAULT | ast.FRAMEOPTION_ROWS
				n.FrameOptions |= $3
				$$ = n
			}
	|	GROUPS frame_extent opt_window_exclusion_clause
			{
				n := $2
				n.FrameOptions |= ast.FRAMEOPTION_NONDEFAULT | ast.FRAMEOPTION_GROUPS
				n.FrameOptions |= $3
				$$ = n
			}
	|	/* EMPTY */
			{
				n := ast.NewWindowDef("", -1)
				n.FrameOptions = ast.FRAMEOPTION_DEFAULTS
				n.StartOffset = nil
				n.EndOffset = nil
				$$ = n
			}
	;

frame_extent:
		frame_bound
			{
				n := $1
				// reject invalid cases - these would be runtime errors in PostgreSQL
				if (n.FrameOptions & ast.FRAMEOPTION_START_UNBOUNDED_FOLLOWING) != 0 {
					yylex.Error("frame start cannot be UNBOUNDED FOLLOWING")
					return 1
				} else if (n.FrameOptions & ast.FRAMEOPTION_START_OFFSET_FOLLOWING) != 0 {
					yylex.Error("frame starting from following row cannot end with current row")
					return 1
				}
				n.FrameOptions |= ast.FRAMEOPTION_END_CURRENT_ROW
				$$ = n
			}
	|	BETWEEN frame_bound AND frame_bound
			{
				n1 := $2
				n2 := $4

				// form merged options
				frameOptions := n1.FrameOptions
				// shift converts START_ options to END_ options
				frameOptions |= (n2.FrameOptions << 1)
				frameOptions |= ast.FRAMEOPTION_BETWEEN

				// reject invalid cases
				if (frameOptions & ast.FRAMEOPTION_START_UNBOUNDED_FOLLOWING) != 0 {
					yylex.Error("frame start cannot be UNBOUNDED FOLLOWING")
					return 1;
				} else if (frameOptions & ast.FRAMEOPTION_END_UNBOUNDED_PRECEDING) != 0 {
					yylex.Error("frame end cannot be UNBOUNDED PRECEDING")
					return 1;
				} else if (frameOptions & ast.FRAMEOPTION_START_CURRENT_ROW) != 0 &&
						  (frameOptions & ast.FRAMEOPTION_END_OFFSET_PRECEDING) != 0 {
					yylex.Error("frame starting from current row cannot have preceding rows")
					return 1;
				} else if (frameOptions & ast.FRAMEOPTION_START_OFFSET_FOLLOWING) != 0 &&
						  ((frameOptions & ast.FRAMEOPTION_END_OFFSET_PRECEDING) != 0 ||
						   (frameOptions & ast.FRAMEOPTION_END_CURRENT_ROW) != 0) {
					yylex.Error("frame starting from following row cannot have preceding rows")
					return 1;
				}
				n1.FrameOptions = frameOptions
				n1.EndOffset = n2.StartOffset
				$$ = n1
			}
	;

frame_bound:
		UNBOUNDED PRECEDING
			{
				n := ast.NewWindowDef("", -1)
				n.FrameOptions = ast.FRAMEOPTION_START_UNBOUNDED_PRECEDING
				n.StartOffset = nil
				n.EndOffset = nil
				$$ = n
			}
	|	UNBOUNDED FOLLOWING
			{
				n := ast.NewWindowDef("", -1)
				n.FrameOptions = ast.FRAMEOPTION_START_UNBOUNDED_FOLLOWING
				n.StartOffset = nil
				n.EndOffset = nil
				$$ = n
			}
	|	CURRENT_P ROW
			{
				n := ast.NewWindowDef("", -1)
				n.FrameOptions = ast.FRAMEOPTION_START_CURRENT_ROW
				n.StartOffset = nil
				n.EndOffset = nil
				$$ = n
			}
	|	a_expr PRECEDING
			{
				n := ast.NewWindowDef("", -1)
				n.FrameOptions = ast.FRAMEOPTION_START_OFFSET_PRECEDING
				n.StartOffset = $1
				n.EndOffset = nil
				$$ = n
			}
	|	a_expr FOLLOWING
			{
				n := ast.NewWindowDef("", -1)
				n.FrameOptions = ast.FRAMEOPTION_START_OFFSET_FOLLOWING
				n.StartOffset = $1
				n.EndOffset = nil
				$$ = n
			}
	;

opt_window_exclusion_clause:
		EXCLUDE CURRENT_P ROW	{ $$ = ast.FRAMEOPTION_EXCLUDE_CURRENT_ROW }
	|	EXCLUDE GROUP_P			{ $$ = ast.FRAMEOPTION_EXCLUDE_GROUP }
	|	EXCLUDE TIES			{ $$ = ast.FRAMEOPTION_EXCLUDE_TIES }
	|	EXCLUDE NO OTHERS		{ $$ = 0 }
	|	/* EMPTY */				{ $$ = 0 }
	;

/*
 * LIMIT/OFFSET clause support
 * From postgres/src/backend/parser/gram.y:13116+
 */
select_limit:
		limit_clause offset_clause
			{
				$$ = $1
				$$.limitOffset = $2
			}
	|	offset_clause limit_clause
			{
				$$ = $2
				$$.limitOffset = $1
			}
	|	limit_clause
			{
				$$ = $1
			}
	|	offset_clause
			{
				n := &selectLimit{}
				n.limitOffset = $1
				n.limitCount = nil
				n.limitOption = ast.LIMIT_OPTION_COUNT
				$$ = n
			}
	;

opt_select_limit:
		select_limit						{ $$ = $1 }
	|	/* EMPTY */							{ $$ = nil }
	;

limit_clause:
		LIMIT select_limit_value
			{
				n := &selectLimit{}
				n.limitOffset = nil
				n.limitCount = $2
				n.limitOption = ast.LIMIT_OPTION_COUNT
				$$ = n
			}
	|	LIMIT select_limit_value ',' select_offset_value
			{
				// Disabled because it was too confusing - PostgreSQL error
				yylex.Error("LIMIT #,# syntax is not supported. Use separate LIMIT and OFFSET clauses.")
				return 1;
			}
	/* SQL:2008 syntax - FETCH FIRST */
	|	FETCH first_or_next select_fetch_first_value row_or_rows ONLY
			{
				n := &selectLimit{}
				n.limitOffset = nil
				n.limitCount = $3
				n.limitOption = ast.LIMIT_OPTION_COUNT
				$$ = n
			}
	|	FETCH first_or_next select_fetch_first_value row_or_rows WITH TIES
			{
				n := &selectLimit{}
				n.limitOffset = nil
				n.limitCount = $3
				n.limitOption = ast.LIMIT_OPTION_WITH_TIES
				$$ = n
			}
	|	FETCH first_or_next row_or_rows ONLY
			{
				n := &selectLimit{}
				n.limitOffset = nil
				n.limitCount = ast.NewA_Const(ast.NewInteger(1), -1)
				n.limitOption = ast.LIMIT_OPTION_COUNT
				$$ = n
			}
	|	FETCH first_or_next row_or_rows WITH TIES
			{
				n := &selectLimit{}
				n.limitOffset = nil
				n.limitCount = ast.NewA_Const(ast.NewInteger(1), -1)
				n.limitOption = ast.LIMIT_OPTION_WITH_TIES
				$$ = n
			}
	;

offset_clause:
		OFFSET select_offset_value
			{ $$ = $2 }
	/* SQL:2008 syntax */
	|	OFFSET select_fetch_first_value row_or_rows
			{ $$ = $2 }
	;

select_limit_value:
		a_expr									{ $$ = $1 }
	|	ALL
			{
				/* LIMIT ALL is represented as a NULL constant */
				$$ = ast.NewA_Const(ast.NewNull(), -1)
			}
	;

select_offset_value:
		a_expr									{ $$ = $1 }
	;

/*
 * Allowing full expressions without parentheses causes various parsing
 * problems with the trailing ROW/ROWS key words. SQL spec only calls for
 * <simple value specification>, which is either a literal or a parameter (but
 * an <SQL parameter reference> could be an identifier, bringing up conflicts
 * with ROW/ROWS). We solve this by leveraging the presence of ONLY (see above)
 * to determine whether the expression is missing rather than trying to make it
 * optional. With the extra productions, ONLY is not optional.
 */
select_fetch_first_value:
		c_expr									{ $$ = $1 }
	|	'+' I_or_F_const
			{
				$$ = ast.NewA_Expr(ast.AEXPR_OP, &ast.NodeList{Items: []ast.Node{ast.NewString("+")}}, nil, $2, -1)
			}
	|	'-' I_or_F_const
			{
				// Create a unary minus expression
				$$ = ast.NewA_Expr(ast.AEXPR_OP, &ast.NodeList{Items: []ast.Node{ast.NewString("-")}}, nil, $2, -1)
			}
	;

I_or_F_const:
		Iconst									{ $$ = ast.NewA_Const(ast.NewInteger($1), -1) }
	|	FCONST									{ $$ = ast.NewA_Const(ast.NewFloat($1), -1) }
	;

/* noise words */
row_or_rows:
		ROW										{ $$ = 0 }
	|	ROWS									{ $$ = 0 }
	;

first_or_next:
		FIRST_P									{ $$ = 0 }
	|	NEXT									{ $$ = 0 }
	;

/*****************************************************************************
 *
 * TRANSACTIONS:
 * BEGIN / COMMIT / ROLLBACK / SAVEPOINT
 * (ported from postgres/src/backend/parser/gram.y:14290-14400)
 *
 *****************************************************************************/

TransactionStmt:
			ABORT_P opt_transaction opt_transaction_chain
				{
					stmt := ast.NewTransactionStmt(ast.TRANS_STMT_ROLLBACK)
					stmt.Chain = $3
					$$ = stmt
				}
		|	START TRANSACTION transaction_mode_list_or_empty
				{
					stmt := ast.NewTransactionStmt(ast.TRANS_STMT_START)
					stmt.Options = $3
					$$ = stmt
				}
		|	COMMIT opt_transaction opt_transaction_chain
				{
					stmt := ast.NewTransactionStmt(ast.TRANS_STMT_COMMIT)
					stmt.Chain = $3
					$$ = stmt
				}
		|	ROLLBACK opt_transaction opt_transaction_chain
				{
					stmt := ast.NewTransactionStmt(ast.TRANS_STMT_ROLLBACK)
					stmt.Chain = $3
					$$ = stmt
				}
		|	SAVEPOINT ColId
				{
					stmt := ast.NewSavepointStmt($2)
					$$ = stmt
				}
		|	RELEASE SAVEPOINT ColId
				{
					stmt := ast.NewReleaseStmt($3)
					$$ = stmt
				}
		|	RELEASE ColId
				{
					stmt := ast.NewReleaseStmt($2)
					$$ = stmt
				}
		|	ROLLBACK opt_transaction TO SAVEPOINT ColId
				{
					stmt := ast.NewRollbackToStmt($5)
					$$ = stmt
				}
		|	ROLLBACK opt_transaction TO ColId
				{
					stmt := ast.NewRollbackToStmt($4)
					$$ = stmt
				}
		|	PREPARE TRANSACTION Sconst
				{
					stmt := ast.NewTransactionStmt(ast.TRANS_STMT_PREPARE)
					stmt.Gid = $3
					$$ = stmt
				}
		|	COMMIT PREPARED Sconst
				{
					stmt := ast.NewTransactionStmt(ast.TRANS_STMT_COMMIT_PREPARED)
					stmt.Gid = $3
					$$ = stmt
				}
		|	ROLLBACK PREPARED Sconst
				{
					stmt := ast.NewTransactionStmt(ast.TRANS_STMT_ROLLBACK_PREPARED)
					stmt.Gid = $3
					$$ = stmt
				}
		;

TransactionStmtLegacy:
	END_P opt_transaction opt_transaction_chain
				{
					stmt := ast.NewTransactionStmt(ast.TRANS_STMT_COMMIT)
					stmt.Chain = $3
					$$ = stmt
				}
|	BEGIN_P opt_transaction transaction_mode_list_or_empty
				{
					stmt := ast.NewTransactionStmt(ast.TRANS_STMT_BEGIN)
					stmt.Options = $3
					$$ = stmt
				}

opt_transaction:
			WORK								{ }
		|	TRANSACTION							{ }
		|	/* EMPTY */							{ }
		;

opt_transaction_chain:
			AND CHAIN							{ $$ = true }
		|	AND NO CHAIN						{ $$ = false }
		|	/* EMPTY */							{ $$ = false }
		;


/*****************************************************************************
 *
 * CREATE/ALTER/DROP ROLE
 * (ported from postgres/src/backend/parser/gram.y:4430-4570)
 *
 *****************************************************************************/

CreateRoleStmt:
			CREATE ROLE RoleId opt_with OptRoleList
				{
					$$ = ast.NewCreateRoleStmt(ast.ROLESTMT_ROLE, $3, $5)
				}
		;

CreateUserStmt:
			CREATE USER RoleId opt_with OptRoleList
				{
					$$ = ast.NewCreateRoleStmt(ast.ROLESTMT_USER, $3, $5)
				}
		;

CreateGroupStmt:
			CREATE GROUP_P RoleId opt_with OptRoleList
				{
					$$ = ast.NewCreateRoleStmt(ast.ROLESTMT_GROUP, $3, $5)
				}
		;

AlterRoleStmt:
			ALTER ROLE RoleSpec opt_with AlterOptRoleList
				{
					as := ast.NewAlterRoleStmt($3, $5)
					as.Action = +1
					$$ = as
				}
		|	ALTER USER RoleSpec opt_with AlterOptRoleList
				{
					as := ast.NewAlterRoleStmt($3, $5)
					as.Action = +1
					$$ = as
				}
		;

AlterRoleSetStmt:
		ALTER ROLE RoleSpec opt_in_database SetResetClause
			{
				$$ = ast.NewAlterRoleSetStmt($3, $4, $5)
			}
	|	ALTER ROLE ALL opt_in_database SetResetClause
			{
				$$ = ast.NewAlterRoleSetStmt(nil, $4, $5)
			}
	|	ALTER USER RoleSpec opt_in_database SetResetClause
			{
				$$ = ast.NewAlterRoleSetStmt($3, $4, $5)
			}
	|	ALTER USER ALL opt_in_database SetResetClause
			{
				$$ = ast.NewAlterRoleSetStmt(nil, $4, $5)
			}
	;

opt_in_database:
		/* EMPTY */			{ $$ = "" }
	|	IN_P DATABASE name	{ $$ = $3 }
	;

AlterGroupStmt:
			ALTER GROUP_P RoleSpec add_drop USER role_list
				{
					options := ast.NewNodeList(ast.NewDefElem("rolemembers", $6))
					stmt := ast.NewAlterRoleStmt($3, options)
					stmt.Action = $4
					$$ = stmt
				}
		;

DropRoleStmt:
			DROP ROLE role_list
				{
					$$ = ast.NewDropRoleStmt($3, false)
				}
		|	DROP ROLE IF_P EXISTS role_list
				{
					$$ = ast.NewDropRoleStmt($5, true)
				}
		|	DROP USER role_list
				{
					$$ = ast.NewDropRoleStmt($3, false)
				}
		|	DROP USER IF_P EXISTS role_list
				{
					$$ = ast.NewDropRoleStmt($5, true)
				}
		|	DROP GROUP_P role_list
				{
					$$ = ast.NewDropRoleStmt($3, false)
				}
		|	DROP GROUP_P IF_P EXISTS role_list
				{
					$$ = ast.NewDropRoleStmt($5, true)
				}
		;

/* Role options for CREATE ROLE and ALTER ROLE */
OptRoleList:
			OptRoleList CreateOptRoleElem
				{
					if $1 == nil {
						list := ast.NewNodeList()
						list.Append($2)
						$$ = list
					} else {
						list := $1
						list.Append($2)
						$$ = list
					}
				}
		|	/* EMPTY */							{ $$ = nil }
		;

AlterOptRoleList:
			AlterOptRoleList AlterOptRoleElem
				{
					if $1 == nil {
						list := ast.NewNodeList()
						list.Append($2)
						$$ = list
					} else {
						list := $1
						list.Append($2)
						$$ = list
					}
				}
		|	/* EMPTY */							{ $$ = nil }
		;

CreateOptRoleElem:
			AlterOptRoleElem					{ $$ = $1 }
		|	SYSID Iconst
				{
					$$ = ast.NewDefElem("sysid", ast.NewInteger($2))
				}
		|	ADMIN role_list
				{
					$$ = ast.NewDefElem("adminmembers", $2)
				}
		|	ROLE role_list
				{
					$$ = ast.NewDefElem("rolemembers", $2)
				}
		|	IN_P ROLE role_list
				{
					$$ = ast.NewDefElem("addroleto", $3)
				}
		|	IN_P GROUP_P role_list
				{
					$$ = ast.NewDefElem("addroleto", $3)
				}
		;

AlterOptRoleElem:
			PASSWORD Sconst
				{
					$$ = ast.NewDefElem("password", ast.NewString($2))
				}
		|	PASSWORD NULL_P
				{
					$$ = ast.NewDefElem("password", nil)
				}
		|	ENCRYPTED PASSWORD Sconst
				{
					$$ = ast.NewDefElem("password", ast.NewString($3))
				}
		|	UNENCRYPTED PASSWORD Sconst
				{
					yylex.Error("UNENCRYPTED PASSWORD is no longer supported")
					return 1
				}
		|	INHERIT
				{
					$$ = ast.NewDefElem("inherit", ast.NewBoolean(true))
				}
		|	IDENT
				{
					// Handle identifiers like PostgreSQL does with string comparisons
					if $1 == "superuser" {
						$$ = ast.NewDefElem("superuser", ast.NewBoolean(true))
					} else if $1 == "nosuperuser" {
						$$ = ast.NewDefElem("superuser", ast.NewBoolean(false))
					} else if $1 == "createrole" {
						$$ = ast.NewDefElem("createrole", ast.NewBoolean(true))
					} else if $1 == "nocreaterole" {
						$$ = ast.NewDefElem("createrole", ast.NewBoolean(false))
					} else if $1 == "createdb" {
						$$ = ast.NewDefElem("createdb", ast.NewBoolean(true))
					} else if $1 == "nocreatedb" {
						$$ = ast.NewDefElem("createdb", ast.NewBoolean(false))
					} else if $1 == "login" {
						$$ = ast.NewDefElem("canlogin", ast.NewBoolean(true))
					} else if $1 == "nologin" {
						$$ = ast.NewDefElem("canlogin", ast.NewBoolean(false))
					} else if $1 == "replication" {
						$$ = ast.NewDefElem("isreplication", ast.NewBoolean(true))
					} else if $1 == "noreplication" {
						$$ = ast.NewDefElem("isreplication", ast.NewBoolean(false))
					} else if $1 == "bypassrls" {
						$$ = ast.NewDefElem("bypassrls", ast.NewBoolean(true))
					} else if $1 == "nobypassrls" {
						$$ = ast.NewDefElem("bypassrls", ast.NewBoolean(false))
					} else if $1 == "noinherit" {
						$$ = ast.NewDefElem("inherit", ast.NewBoolean(false))
					} else {
						// Return error for unrecognized role option
						yylex.Error("unrecognized role option \"" + $1 + "\"")
						$$ = nil
					}
				}
		|	CONNECTION LIMIT SignedIconst
				{
					$$ = ast.NewDefElem("connectionlimit", ast.NewInteger($3))
				}
		|	VALID UNTIL Sconst
				{
					$$ = ast.NewDefElem("validUntil", ast.NewString($3))
				}
		| 	USER role_list
				{
					$$ = ast.NewDefElem("rolemembers", $2);
				}
		;

add_drop:
			ADD_P									{ $$ = 1 }
		|	DROP									{ $$ = -1 }
		;

/*****************************************************************************
 *
 * GRANT and REVOKE statements
 * (ported from postgres/src/backend/parser/gram.y:7535-7920)
 *
 *****************************************************************************/

GrantStmt:	GRANT privileges ON privilege_target TO grantee_list
			opt_grant_grant_option opt_granted_by
				{
					n := ast.NewGrantStmt($4.objtype, $4.objs, $2, $6)
					n.Targtype = $4.targtype
					n.GrantOption = $7
					n.Grantor = $8
					$$ = n
				}
		;

RevokeStmt:
			REVOKE privileges ON privilege_target
			FROM grantee_list opt_granted_by opt_drop_behavior
				{
					n := ast.NewRevokeStmt($4.objtype, $4.objs, $2, $6)
					n.Targtype = $4.targtype
					n.Grantor = $7
					n.Behavior = $8
					$$ = n
				}
			| REVOKE GRANT OPTION FOR privileges ON privilege_target
			FROM grantee_list opt_granted_by opt_drop_behavior
				{
					n := ast.NewRevokeStmt($7.objtype, $7.objs, $5, $9)
					n.Targtype = $7.targtype
					n.GrantOption = true
					n.Grantor = $10
					n.Behavior = $11
					$$ = n
				}
		;

GrantRoleStmt:
			GRANT privilege_list TO role_list opt_granted_by
				{
					stmt := ast.NewGrantRoleStmt($2, $4)
					stmt.Grantor = $5
					$$ = stmt
				}
		  | GRANT privilege_list TO role_list WITH grant_role_opt_list opt_granted_by
				{
					stmt := ast.NewGrantRoleStmtWithOptions($2, $4, $6)
					stmt.Grantor = $7
					$$ = stmt
				}
		;

RevokeRoleStmt:
			REVOKE privilege_list FROM role_list opt_granted_by opt_drop_behavior
				{
					stmt := ast.NewRevokeRoleStmt($2, $4)
					stmt.Grantor = $5
					stmt.Behavior = $6
					$$ = stmt
				}
			| REVOKE ColId OPTION FOR privilege_list FROM role_list opt_granted_by opt_drop_behavior
				{
					opt := ast.NewDefElem($2, ast.NewBoolean(false))
					stmt := ast.NewRevokeRoleStmt($5, $7)
					stmt.Opt = &ast.NodeList{Items: []ast.Node{opt}}
					stmt.Grantor = $8
					stmt.Behavior = $9
					$$ = stmt
				}
		;

/*****************************************************************************
 *
 * ALTER DEFAULT PRIVILEGES statement
 * (ported from postgres/src/backend/parser/gram.y)
 *
 *****************************************************************************/

AlterDefaultPrivilegesStmt:
			ALTER DEFAULT PRIVILEGES DefACLOptionList DefACLAction
				{
					$$ = ast.NewAlterDefaultPrivilegesStmt($4, $5.(*ast.GrantStmt))
				}
		;

DefACLOptionList:
			DefACLOptionList DefACLOption			{ $1.Append($2); $$ = $1 }
			| /* EMPTY */							{ $$ = ast.NewNodeList() }
		;

DefACLOption:
			IN_P SCHEMA name_list
				{
					$$ = ast.NewDefElem("schemas", $3)
				}
			| FOR ROLE role_list
				{
					$$ = ast.NewDefElem("roles", $3)
				}
			| FOR USER role_list
				{
					$$ = ast.NewDefElem("roles", $3)
				}
		;

/*
 * This should match GRANT/REVOKE, except that individual target objects
 * are not mentioned and we only allow a subset of object types.
 */
DefACLAction:
			GRANT privileges ON defacl_privilege_target TO grantee_list
			opt_grant_grant_option
				{
					n := ast.NewGrantStmt(ast.ObjectType($4), nil, $2, $6)
					n.Targtype = ast.ACL_TARGET_DEFAULTS
					n.GrantOption = $7
					$$ = n
				}
			| REVOKE privileges ON defacl_privilege_target
			FROM grantee_list opt_drop_behavior
				{
					n := ast.NewRevokeStmt(ast.ObjectType($4), nil, $2, $6)
					n.Targtype = ast.ACL_TARGET_DEFAULTS
					n.Behavior = $7
					$$ = n
				}
			| REVOKE GRANT OPTION FOR privileges ON defacl_privilege_target
			FROM grantee_list opt_drop_behavior
				{
					n := ast.NewRevokeStmt(ast.ObjectType($7), nil, $5, $9)
					n.Targtype = ast.ACL_TARGET_DEFAULTS
					n.GrantOption = true
					n.Behavior = $10
					$$ = n
				}
		;

defacl_privilege_target:
			TABLES			{ $$ = int(ast.OBJECT_TABLE) }
			| FUNCTIONS		{ $$ = int(ast.OBJECT_FUNCTION) }
			| ROUTINES		{ $$ = int(ast.OBJECT_FUNCTION) }
			| SEQUENCES		{ $$ = int(ast.OBJECT_SEQUENCE) }
			| TYPES_P		{ $$ = int(ast.OBJECT_TYPE) }
			| SCHEMAS		{ $$ = int(ast.OBJECT_SCHEMA) }
		;

/* either ALL [PRIVILEGES] or a list of individual privileges */
privileges: privilege_list
				{ $$ = $1 }
			| ALL
				{ $$ = nil }
			| ALL PRIVILEGES
				{ $$ = nil }
			| ALL '(' columnList ')'
				{
					ap := ast.NewAccessPriv("", $3)
					$$ = ast.NewNodeList(ap)
				}
			| ALL PRIVILEGES '(' columnList ')'
				{
					ap := ast.NewAccessPriv("", $4)
					$$ = ast.NewNodeList(ap)
				}
		;

privilege_list:	privilege							{ $$ = ast.NewNodeList($1) }
			| privilege_list ',' privilege			{ $1.Items = append($1.Items, $3); $$ = $1 }
		;

privilege:	SELECT opt_column_list
			{
				$$ = ast.NewAccessPriv("SELECT", $2)
			}
		| REFERENCES opt_column_list
			{
				$$ = ast.NewAccessPriv("REFERENCES", $2)
			}
		| CREATE opt_column_list
			{
				$$ = ast.NewAccessPriv("CREATE", $2)
			}
		| ALTER SYSTEM_P
			{
				$$ = ast.NewAccessPriv("ALTER SYSTEM", nil)
			}
		| ColId opt_column_list
			{
				$$ = ast.NewAccessPriv($1, $2)
			}
		;

/* Don't bother trying to fold the first two rules into one using
 * opt_table.  You're going to get conflicts.
 */
privilege_target:
			qualified_name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_TABLE,
						objs:     $1,
					}
				}
			| TABLE qualified_name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_TABLE,
						objs:     $2,
					}
				}
			| SEQUENCE qualified_name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_SEQUENCE,
						objs:     $2,
					}
				}
			| FOREIGN DATA_P WRAPPER name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_FDW,
						objs:     $4,
					}
				}
			| FOREIGN SERVER name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_FOREIGN_SERVER,
						objs:     $3,
					}
				}
			| FUNCTION function_with_argtypes_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_FUNCTION,
						objs:     $2,
					}
				}
			| PROCEDURE function_with_argtypes_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_PROCEDURE,
						objs:     $2,
					}
				}
			| ROUTINE function_with_argtypes_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_ROUTINE,
						objs:     $2,
					}
				}
			| DATABASE name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_DATABASE,
						objs:     $2,
					}
				}
			| DOMAIN_P any_name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_DOMAIN,
						objs:     $2,
					}
				}
			| LANGUAGE name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_LANGUAGE,
						objs:     $2,
					}
				}
			| LARGE_P OBJECT_P NumericOnly_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_LARGEOBJECT,
						objs:     $3,
					}
				}
			| PARAMETER parameter_name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_PARAMETER_ACL,
						objs:     $2,
					}
				}
			| SCHEMA name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_SCHEMA,
						objs:     $2,
					}
				}
			| TABLESPACE name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_TABLESPACE,
						objs:     $2,
					}
				}
			| TYPE_P any_name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_OBJECT,
						objtype:  ast.OBJECT_TYPE,
						objs:     $2,
					}
				}
			| ALL TABLES IN_P SCHEMA name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_ALL_IN_SCHEMA,
						objtype:  ast.OBJECT_TABLE,
						objs:     $5,
					}
				}
			| ALL SEQUENCES IN_P SCHEMA name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_ALL_IN_SCHEMA,
						objtype:  ast.OBJECT_SEQUENCE,
						objs:     $5,
					}
				}
			| ALL FUNCTIONS IN_P SCHEMA name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_ALL_IN_SCHEMA,
						objtype:  ast.OBJECT_FUNCTION,
						objs:     $5,
					}
				}
			| ALL PROCEDURES IN_P SCHEMA name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_ALL_IN_SCHEMA,
						objtype:  ast.OBJECT_PROCEDURE,
						objs:     $5,
					}
				}
			| ALL ROUTINES IN_P SCHEMA name_list
				{
					$$ = &PrivTarget{
						targtype: ast.ACL_TARGET_ALL_IN_SCHEMA,
						objtype:  ast.OBJECT_ROUTINE,
						objs:     $5,
					}
				}
		;


grantee_list:
			grantee									{ $$ = ast.NewNodeList($1) }
			| grantee_list ',' grantee				{ $1.Items = append($1.Items, $3); $$ = $1 }
		;

grantee:
			RoleSpec								{ $$ = $1 }
			| GROUP_P RoleSpec						{ $$ = $2 }
		;


opt_grant_grant_option:
			WITH GRANT OPTION { $$ = true }
			| /*EMPTY*/ { $$ = false }
		;

grant_role_opt_list:
			grant_role_opt_list ',' grant_role_opt	{ $1.Items = append($1.Items, $3); $$ = $1 }
			| grant_role_opt						{ $$ = ast.NewNodeList($1) }
		;

grant_role_opt:
		ColLabel grant_role_opt_value
			{
				$$ = ast.NewDefElem($1, $2)
			}
		;

grant_role_opt_value:
		OPTION			{ $$ = ast.NewBoolean(true) }
		| TRUE_P		{ $$ = ast.NewBoolean(true) }
		| FALSE_P		{ $$ = ast.NewBoolean(false) }
		;

opt_granted_by: GRANTED BY RoleSpec						{ $$ = $3 }
			| /*EMPTY*/									{ $$ = nil }
		;

parameter_name_list:
		parameter_name
			{
				$$ = ast.NewNodeList(ast.NewString($1))
			}
		| parameter_name_list ',' parameter_name
			{
				$1.Append(ast.NewString($3))
				$$ = $1
			}
		;

parameter_name:
		ColId
			{
				$$ = $1;
			}
		| parameter_name '.' ColId
			{
				$$ = $1 + "." + $3
			}
		;

NumericOnly_list:	NumericOnly						{ $$ = ast.NewNodeList($1) }
		| NumericOnly_list ',' NumericOnly			{ $1.Append($3); $$ = $1 }

tablesample_clause:
			TABLESAMPLE func_name '(' expr_list ')' opt_repeatable_clause
			{
				n := ast.NewRangeTableSample(nil, $2, $4, $6, 0)
				$$ = n
			}
		;

opt_repeatable_clause:
			REPEATABLE '(' a_expr ')'
			{
				$$ = $3
			}
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

/*****************************************************************************
 *
 * ALTER FUNCTION statement
 *
 * Ported from postgres/src/backend/parser/gram.y AlterFunctionStmt
 *****************************************************************************/

AlterFunctionStmt:
			ALTER FUNCTION function_with_argtypes alterfunc_opt_list opt_restrict
				{
					n := ast.NewAlterFunctionStmt(ast.OBJECT_FUNCTION, $3, $4)
					$$ = n
				}
			| ALTER PROCEDURE function_with_argtypes alterfunc_opt_list opt_restrict
				{
					n := ast.NewAlterFunctionStmt(ast.OBJECT_PROCEDURE, $3, $4)
					$$ = n
				}
			| ALTER ROUTINE function_with_argtypes alterfunc_opt_list opt_restrict
				{
					n := ast.NewAlterFunctionStmt(ast.OBJECT_ROUTINE, $3, $4)
					$$ = n
				}
		;

alterfunc_opt_list:
			/* At least one option must be specified */
			common_func_opt_item					{ $$ = ast.NewNodeList($1) }
			| alterfunc_opt_list common_func_opt_item { $1.Append($2); $$ = $1 }
		;

/* Common function options that can be used in both CREATE and ALTER */
common_func_opt_item:
			CALLED ON NULL_P INPUT_P
				{
					$$ = ast.NewDefElem("strict", ast.NewBoolean(false))
				}
			| RETURNS NULL_P ON NULL_P INPUT_P
				{
					$$ = ast.NewDefElem("strict", ast.NewBoolean(true))
				}
			| STRICT_P
				{
					$$ = ast.NewDefElem("strict", ast.NewBoolean(true))
				}
			| IMMUTABLE
				{
					$$ = ast.NewDefElem("volatility", ast.NewString("immutable"))
				}
			| STABLE
				{
					$$ = ast.NewDefElem("volatility", ast.NewString("stable"))
				}
			| VOLATILE
				{
					$$ = ast.NewDefElem("volatility", ast.NewString("volatile"))
				}
			| EXTERNAL SECURITY DEFINER
				{
					$$ = ast.NewDefElem("security", ast.NewBoolean(true))
				}
			| EXTERNAL SECURITY INVOKER
				{
					$$ = ast.NewDefElem("security", ast.NewBoolean(false))
				}
			| SECURITY DEFINER
				{
					$$ = ast.NewDefElem("security", ast.NewBoolean(true))
				}
			| SECURITY INVOKER
				{
					$$ = ast.NewDefElem("security", ast.NewBoolean(false))
				}
			| LEAKPROOF
				{
					$$ = ast.NewDefElem("leakproof", ast.NewBoolean(true))
				}
			| NOT LEAKPROOF
				{
					$$ = ast.NewDefElem("leakproof", ast.NewBoolean(false))
				}
			| COST NumericOnly
				{
					$$ = ast.NewDefElem("cost", $2)
				}
			| ROWS NumericOnly
				{
					$$ = ast.NewDefElem("rows", $2)
				}
			| SUPPORT any_name
				{
					$$ = ast.NewDefElem("support", $2)
				}
			| FunctionSetResetClause
				{
					/* we abuse the normal content of a DefElem here */
					$$ = ast.NewDefElem("set", $1)
				}
			| PARALLEL ColId
				{
					$$ = ast.NewDefElem("parallel", ast.NewString($2))
				}
		;

operator_def_list:
			operator_def_elem						{ $$ = ast.NewNodeList($1) }
			| operator_def_list ',' operator_def_elem	{ $1.Append($3); $$ = $1 }
		;

operator_def_elem:
			ColLabel '=' NONE
				{
					$$ = ast.NewDefElem($1, nil)
				}
			| ColLabel '=' operator_def_arg
				{
					$$ = ast.NewDefElem($1, $3)
				}
			| ColLabel
				{
					$$ = ast.NewDefElem($1, nil)
				}
		;

operator_def_arg:
			func_type						{ $$ = $1 }
			| reserved_keyword				{ $$ = ast.NewString($1) }
			| qual_all_Op					{ $$ = $1 }
			| NumericOnly					{ $$ = $1 }
			| Sconst						{ $$ = ast.NewString($1) }
		;

/* Ignored, merely for SQL compliance */
opt_restrict:
			RESTRICT
			| /* EMPTY */
		;

function_with_argtypes:
			func_name func_args
				{
					n := ast.NewEmptyObjectWithArgs()
					n.Objname = $1
					n.Objargs = ast.ExtractArgTypes($2)
					n.ObjfuncArgs = $2
					$$ = n
				}
			/*
			 * Because of reduce/reduce conflicts, we can't use func_name
			 * below, but we can write it out the long way, which actually
			 * allows more cases.
			 */
			| type_func_name_keyword
				{
					n := ast.NewEmptyObjectWithArgs()
					n.Objname = ast.NewNodeList(ast.NewString($1))
					n.ArgsUnspecified = true
					$$ = n
				}
			| ColId
				{
					n := ast.NewEmptyObjectWithArgs()
					n.Objname = ast.NewNodeList(ast.NewString($1))
					n.ArgsUnspecified = true
					$$ = n
				}
			| ColId indirection
				{
					n := ast.NewEmptyObjectWithArgs()
					nameList := ast.NewNodeList(ast.NewString($1))
					// Append indirection elements
					for i := 0; i < $2.Len(); i++ {
						nameList.Append($2.Items[i])
					}
					n.Objname = nameList
					n.ArgsUnspecified = true
					$$ = n
				}
		;

function_with_argtypes_list:
			function_with_argtypes					{ $$ = ast.NewNodeList($1) }
			| function_with_argtypes_list ',' function_with_argtypes
													{ $1.Append($3); $$ = $1 }
		;

operator_with_argtypes_list:
			operator_with_argtypes					{ $$ = ast.NewNodeList($1) }
			| operator_with_argtypes_list ',' operator_with_argtypes
													{ $1.Append($3); $$ = $1 }
		;

/*****************************************************************************
 *
 * ALTER TYPE statements
 *
 * Ported from postgres/src/backend/parser/gram.y
 *****************************************************************************/

AlterTypeStmt:
			ALTER TYPE_P any_name SET '(' operator_def_list ')'
				{
					n := ast.NewAlterTypeStmt($3, $6)
					$$ = n
				}
		;

AlterCompositeTypeStmt:
			ALTER TYPE_P any_name alter_type_cmds
				{
					relation, err := makeRangeVarFromAnyName($3, 0)
					if err != nil {
						yylex.Error("invalid type name")
						return 1
					}
					n := ast.NewAlterTableStmt(relation, $4)
					n.Objtype = ast.OBJECT_TYPE  // Mark this as a composite type alteration
					$$ = n
				}
		;

alter_type_cmds:
			alter_type_cmd							{ $$ = ast.NewNodeList($1) }
			| alter_type_cmds ',' alter_type_cmd	{ $1.Append($3); $$ = $1 }
		;

alter_type_cmd:
			/* ALTER TYPE <name> ADD ATTRIBUTE <coldef> [RESTRICT|CASCADE] */
			ADD_P ATTRIBUTE TableFuncElement opt_drop_behavior
				{
					n := ast.NewAlterTableCmd(ast.AT_AddColumn, "", $3)
					n.Behavior = $4
					$$ = n
				}
			/* ALTER TYPE <name> DROP ATTRIBUTE IF EXISTS <attname> [RESTRICT|CASCADE] */
			| DROP ATTRIBUTE IF_P EXISTS ColId opt_drop_behavior
				{
					n := ast.NewAlterTableCmd(ast.AT_DropColumn, $5, nil)
					n.Behavior = $6
					n.MissingOk = true
					$$ = n
				}
			/* ALTER TYPE <name> DROP ATTRIBUTE <attname> [RESTRICT|CASCADE] */
			| DROP ATTRIBUTE ColId opt_drop_behavior
				{
					n := ast.NewAlterTableCmd(ast.AT_DropColumn, $3, nil)
					n.Behavior = $4
					n.MissingOk = false
					$$ = n
				}
			/* ALTER TYPE <name> ALTER ATTRIBUTE <attname> [SET DATA] TYPE <typename> [RESTRICT|CASCADE] */
			| ALTER ATTRIBUTE ColId opt_set_data TYPE_P Typename opt_collate_clause opt_drop_behavior
				{
					def := ast.NewColumnDef($3, $6, -1)
					n := ast.NewAlterTableCmd(ast.AT_AlterColumnType, $3, def)
					n.Behavior = $8
					/* We only use these fields of the ColumnDef node */
					def.TypeName = $6
					if collClause, ok := $7.(*ast.CollateClause); ok {
						def.Collclause = collClause
					}
					def.RawDefault = nil
					$$ = n
				}
		;

/*****************************************************************************
 *
 * EVENT specification for rules
 *
 * Ported from postgres/src/backend/parser/gram.y
 *****************************************************************************/

event:		SELECT									{ $$ = int(ast.CMD_SELECT) }
			| UPDATE								{ $$ = int(ast.CMD_UPDATE) }
			| DELETE_P								{ $$ = int(ast.CMD_DELETE) }
			| INSERT								{ $$ = int(ast.CMD_INSERT) }
		;

/*****************************************************************************
 *
 * CURSOR STATEMENTS
 *
 *****************************************************************************/

DeclareCursorStmt: DECLARE cursor_name cursor_options CURSOR opt_hold FOR SelectStmt
				{
					$$ = ast.NewDeclareCursorStmt($2, $3|$5|ast.CURSOR_OPT_FAST_PLAN, $7)
				}
		;

cursor_options:
			cursor_options BINARY
				{
					$$ = $1 | ast.CURSOR_OPT_BINARY
				}
		|	cursor_options INSENSITIVE
				{
					$$ = $1 | ast.CURSOR_OPT_INSENSITIVE
				}
		|	cursor_options ASENSITIVE
				{
					$$ = $1 | ast.CURSOR_OPT_ASENSITIVE
				}
		|	cursor_options SCROLL
				{
					$$ = $1 | ast.CURSOR_OPT_SCROLL
				}
		|	cursor_options NO SCROLL
				{
					$$ = $1 | ast.CURSOR_OPT_NO_SCROLL
				}
		|	/* EMPTY */								{ $$ = 0 }
		;

opt_hold:	WITH HOLD								{ $$ = ast.CURSOR_OPT_HOLD }
		|	WITHOUT HOLD							{ $$ = 0 }
		|	/* EMPTY */								{ $$ = 0 }
		;

FetchStmt:	FETCH fetch_args
				{
					stmt := $2.(*ast.FetchStmt)
					stmt.IsMove = false
					$$ = stmt
				}
		|	MOVE fetch_args
				{
					stmt := $2.(*ast.FetchStmt)
					stmt.IsMove = true
					$$ = stmt
				}
		;

fetch_args:	cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_FORWARD, 1, $1, false)
				}
		|	from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_FORWARD, 1, $2, false)
				}
		|	NEXT opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_FORWARD, 1, $3, false)
				}
		|	PRIOR opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_BACKWARD, 1, $3, false)
				}
		|	FIRST_P opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_ABSOLUTE, 1, $3, false)
				}
		|	LAST_P opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_ABSOLUTE, -1, $3, false)
				}
		|	ABSOLUTE_P SignedIconst opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_ABSOLUTE, int64($2), $4, false)
				}
		|	RELATIVE_P SignedIconst opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_RELATIVE, int64($2), $4, false)
				}
		|	SignedIconst opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_FORWARD, int64($1), $3, false)
				}
		|	ALL opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_FORWARD, ast.FETCH_ALL, $3, false)
				}
		|	FORWARD opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_FORWARD, 1, $3, false)
				}
		|	FORWARD SignedIconst opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_FORWARD, int64($2), $4, false)
				}
		|	FORWARD ALL opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_FORWARD, ast.FETCH_ALL, $4, false)
				}
		|	BACKWARD opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_BACKWARD, 1, $3, false)
				}
		|	BACKWARD SignedIconst opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_BACKWARD, int64($2), $4, false)
				}
		|	BACKWARD ALL opt_from_in cursor_name
				{
					$$ = ast.NewFetchStmt(ast.FETCH_BACKWARD, ast.FETCH_ALL, $4, false)
				}
		;

from_in:	FROM									{ $$ = 0 }
		|	IN_P									{ $$ = 0 }
		;

opt_from_in:
			from_in									{ $$ = 0 }
		|	/* EMPTY */								{ $$ = 0 }
		;

ClosePortalStmt:
			CLOSE cursor_name
				{
					name := $2
					$$ = ast.NewClosePortalStmt(name)
				}
		|	CLOSE ALL
				{
					$$ = ast.NewClosePortalStmt("")
				}
		;

/*****************************************************************************
 *
 * PREPARED STATEMENTS
 *
 *****************************************************************************/

PrepareStmt: PREPARE name prep_type_clause AS PreparableStmt
				{
					$$ = ast.NewPrepareStmt($2, $3, $5)
				}
		;

prep_type_clause:
			'(' type_list ')'
				{
					$$ = $2
				}
		|	/* EMPTY */								{ $$ = nil }
		;

PreparableStmt:
			SelectStmt								{ $$ = $1 }
		|	InsertStmt								{ $$ = $1 }
		|	UpdateStmt								{ $$ = $1 }
		|	DeleteStmt								{ $$ = $1 }
		|	MergeStmt								{ $$ = $1 }
		;

ExecuteStmt: EXECUTE name execute_param_clause
				{
					$$ = ast.NewExecuteStmt($2, $3)
				}
		|	CREATE OptTemp TABLE create_as_target AS EXECUTE name execute_param_clause opt_with_data
				{
					executeStmt := ast.NewExecuteStmt($7, $8)
					ctas := ast.NewCreateTableAsStmt(executeStmt, $4, ast.OBJECT_TABLE, false, false)
					// Set relpersistence from OptTemp (following PostgreSQL pattern)
					$4.Rel.RelPersistence = $2
					// Set skipData from opt_with_data (following PostgreSQL pattern)
					$4.SkipData = !$9
					$$ = ctas
				}
		|	CREATE OptTemp TABLE IF_P NOT EXISTS create_as_target AS EXECUTE name execute_param_clause opt_with_data
				{
					executeStmt := ast.NewExecuteStmt($10, $11)
					ctas := ast.NewCreateTableAsStmt(executeStmt, $7, ast.OBJECT_TABLE, false, true)
					// Set relpersistence from OptTemp (following PostgreSQL pattern)
					$7.Rel.RelPersistence = $2
					// Set skipData from opt_with_data (following PostgreSQL pattern)
					$7.SkipData = !$12
					$$ = ctas
				}
		;

execute_param_clause:
			'(' expr_list ')'
				{
					$$ = $2
				}
		|	/* EMPTY */								{ $$ = nil }
		;

/* CREATE TABLE AS target - simplified version of PostgreSQL's create_as_target */
create_as_target:
		qualified_name opt_column_list table_access_method_clause OptWith OnCommitOption OptTableSpace
			{
				into := ast.NewIntoClause($1, $2, $3, $4, $5, $6, nil, false, 0)
				$$ = into
			}
		;

DeallocateStmt:
			DEALLOCATE name
				{
					$$ = ast.NewDeallocateStmt($2)
				}
		|	DEALLOCATE PREPARE name
				{
					$$ = ast.NewDeallocateStmt($3)
				}
		|	DEALLOCATE ALL
				{
					$$ = ast.NewDeallocateAllStmt()
				}
		|	DEALLOCATE PREPARE ALL
				{
					$$ = ast.NewDeallocateAllStmt()
				}
		;

/*
 * LISTEN statement
 * From postgres/src/backend/parser/gram.y:10952-10958
 */
ListenStmt:
		LISTEN ColId
			{
				$$ = ast.NewListenStmt($2)
			}
		;

/*
 * UNLISTEN statement
 * From postgres/src/backend/parser/gram.y:10961-10975
 */
UnlistenStmt:
		UNLISTEN ColId
			{
				$$ = ast.NewUnlistenStmt($2)
			}
	|	UNLISTEN '*'
			{
				$$ = ast.NewUnlistenAllStmt()
			}
		;

/*
 * NOTIFY statement
 * From postgres/src/backend/parser/gram.y:10937-10943
 */
NotifyStmt:
		NOTIFY ColId notify_payload
			{
				$$ = ast.NewNotifyStmt($2, $3)
			}
		;

/*
 * Notify payload
 * From postgres/src/backend/parser/gram.y:10947-10950
 */
notify_payload:
		',' Sconst		{ $$ = $2 }
	|	/* EMPTY */		{ $$ = "" }
		;

/*
 * LOAD statement
 * From postgres/src/backend/parser/gram.y:11260-11265
 */
LoadStmt:
		LOAD file_name
			{
				$$ = ast.NewLoadStmt($2)
			}
		;

/*
 * LOCK statement
 * From postgres/src/backend/parser/gram.y:12309-12317
 */
LockStmt:
		LOCK_P opt_table relation_expr_list opt_lock opt_nowait
			{
				stmt := ast.NewLockStmt($3, ast.LockMode($4))
				stmt.Nowait = $5
				$$ = stmt
			}
		;

/*
 * Lock mode options
 * From postgres/src/backend/parser/gram.y:12320-12336
 */
opt_lock:
		IN_P lock_type MODE		{ $$ = $2 }
	|	/* EMPTY */				{ $$ = int(ast.AccessExclusiveLock) }
		;

lock_type:
		ACCESS SHARE				{ $$ = int(ast.AccessShareLock) }
	|	ROW SHARE					{ $$ = int(ast.RowShareLock) }
	|	ROW EXCLUSIVE				{ $$ = int(ast.RowExclusiveLock) }
	|	SHARE UPDATE EXCLUSIVE		{ $$ = int(ast.ShareUpdateExclusiveLock) }
	|	SHARE						{ $$ = int(ast.ShareLock) }
	|	SHARE ROW EXCLUSIVE			{ $$ = int(ast.ShareRowExclusiveLock) }
	|	EXCLUSIVE					{ $$ = int(ast.ExclusiveLock) }
	|	ACCESS EXCLUSIVE			{ $$ = int(ast.AccessExclusiveLock) }
		;

/*
 * TRUNCATE statement
 * From postgres/src/backend/parser/gram.y:7025-7036
 */
TruncateStmt:
		TRUNCATE opt_table relation_expr_list opt_restart_seqs opt_drop_behavior
			{
				stmt := ast.NewTruncateStmt($3)
				stmt.RestartSeqs = $4
				stmt.Behavior = $5
				$$ = stmt
			}
		;

/*
 * COMMENT ON statement
 * From postgres/src/backend/parser/gram.y:4933-4971
 */
CommentStmt:
		COMMENT ON object_type_any_name any_name IS comment_text
			{
				$$ = ast.NewCommentStmt($3, $4, $6)
			}
		|	COMMENT ON COLUMN any_name IS comment_text
			{
				$$ = ast.NewCommentStmt(ast.OBJECT_COLUMN, $4, $6)
			}
		|	COMMENT ON object_type_name name IS comment_text
			{
				$$ = ast.NewCommentStmt($3, ast.NewString($4), $6)
			}
		|	COMMENT ON TYPE_P Typename IS comment_text
			{
				$$ = ast.NewCommentStmt(ast.OBJECT_TYPE, $4, $6)
			}
		|	COMMENT ON DOMAIN_P Typename IS comment_text
			{
				$$ = ast.NewCommentStmt(ast.OBJECT_DOMAIN, $4, $6)
			}
		|	COMMENT ON AGGREGATE aggregate_with_argtypes IS comment_text
			{
				$$ = ast.NewCommentStmt(ast.OBJECT_AGGREGATE, $4, $6)
			}
		|	COMMENT ON FUNCTION function_with_argtypes IS comment_text
			{
				$$ = ast.NewCommentStmt(ast.OBJECT_FUNCTION, $4, $6)
			}
		|	COMMENT ON OPERATOR operator_with_argtypes IS comment_text
			{
				$$ = ast.NewCommentStmt(ast.OBJECT_OPERATOR, $4, $6)
			}
		|	COMMENT ON CONSTRAINT name ON any_name IS comment_text
			{
				// For table constraints, append constraint name to table name list
				newObj := $6
				newObj.Append(ast.NewString($4))
				$$ = ast.NewCommentStmt(ast.OBJECT_TABCONSTRAINT, newObj, $8)
			}
		|	COMMENT ON CONSTRAINT name ON DOMAIN_P any_name IS comment_text
			{
				// For domain constraints, we need a list of [TypeName, constraint_name]
				// This matches PostgreSQL's approach where they comment:
				// "should use Typename not any_name in the production, but
				// there's a shift/reduce conflict if we do that, so fix it up here."
				objList := ast.NewNodeList()
				objList.Append(makeTypeNameFromNodeList($7))  // Convert any_name to TypeName
				objList.Append(ast.NewString($4))              // Add constraint name
				$$ = ast.NewCommentStmt(ast.OBJECT_DOMCONSTRAINT, objList, $9)
			}
		|	COMMENT ON object_type_name_on_any_name name ON any_name IS comment_text
			{
				// For object types that need name ON any_name: append name to any_name list
				newObj := $6
				newObj.Append(ast.NewString($4))
				$$ = ast.NewCommentStmt($3, newObj, $8)
			}
		|	COMMENT ON PROCEDURE function_with_argtypes IS comment_text
			{
				$$ = ast.NewCommentStmt(ast.OBJECT_PROCEDURE, $4, $6)
			}
		|	COMMENT ON ROUTINE function_with_argtypes IS comment_text
			{
				$$ = ast.NewCommentStmt(ast.OBJECT_ROUTINE, $4, $6)
			}
		|	COMMENT ON TRANSFORM FOR Typename LANGUAGE name IS comment_text
			{
				// Transform: typename + language name
				transformObj := ast.NewNodeList()
				transformObj.Append($5)  // Typename
				transformObj.Append(ast.NewString($7))  // Language name
				$$ = ast.NewCommentStmt(ast.OBJECT_TRANSFORM, transformObj, $9)
			}
		|	COMMENT ON OPERATOR CLASS any_name USING name IS comment_text
			{
				// Operator class: access method + class name
				opclassObj := ast.NewNodeList()
				opclassObj.Append(ast.NewString($7))  // Access method name first
				for _, item := range $5.Items {
					opclassObj.Append(item)  // Class name parts
				}
				$$ = ast.NewCommentStmt(ast.OBJECT_OPCLASS, opclassObj, $9)
			}
		|	COMMENT ON OPERATOR FAMILY any_name USING name IS comment_text
			{
				// Operator family: access method + family name
				opfamilyObj := ast.NewNodeList()
				opfamilyObj.Append(ast.NewString($7))  // Access method name first
				for _, item := range $5.Items {
					opfamilyObj.Append(item)  // Family name parts
				}
				$$ = ast.NewCommentStmt(ast.OBJECT_OPFAMILY, opfamilyObj, $9)
			}
		|	COMMENT ON LARGE_P OBJECT_P NumericOnly IS comment_text
			{
				$$ = ast.NewCommentStmt(ast.OBJECT_LARGEOBJECT, $5, $7)
			}
		|	COMMENT ON CAST '(' Typename AS Typename ')' IS comment_text
			{
				// Cast: source type + target type
				castObj := ast.NewNodeList()
				castObj.Append($5)  // Source typename
				castObj.Append($7)  // Target typename
				$$ = ast.NewCommentStmt(ast.OBJECT_CAST, castObj, $10)
			}
		;

/*
 * comment_text
 * From postgres/src/backend/parser/gram.y:5006-5009
 */
comment_text:
		Sconst		{ $$ = $1 }
	|	NULL_P		{ $$ = "" }
		;

/*
 * SECURITY LABEL statement
 * From postgres/src/backend/parser/gram.y:5011-5032
 */
SecLabelStmt:
		SECURITY LABEL opt_provider ON object_type_any_name any_name IS security_label
			{
				$$ = ast.NewSecLabelStmt($5, $6, $3, $8)
			}
		|	SECURITY LABEL opt_provider ON COLUMN any_name IS security_label
			{
				$$ = ast.NewSecLabelStmt(ast.OBJECT_COLUMN, $6, $3, $8)
			}
		|	SECURITY LABEL opt_provider ON object_type_name name IS security_label
			{
				$$ = ast.NewSecLabelStmt($5, ast.NewString($6), $3, $8)
			}
		|	SECURITY LABEL opt_provider ON TYPE_P Typename IS security_label
			{
				$$ = ast.NewSecLabelStmt(ast.OBJECT_TYPE, $6, $3, $8)
			}
		|	SECURITY LABEL opt_provider ON DOMAIN_P Typename IS security_label
			{
				$$ = ast.NewSecLabelStmt(ast.OBJECT_DOMAIN, $6, $3, $8)
			}
		|	SECURITY LABEL opt_provider ON AGGREGATE aggregate_with_argtypes IS security_label
			{
				$$ = ast.NewSecLabelStmt(ast.OBJECT_AGGREGATE, $6, $3, $8)
			}
		|	SECURITY LABEL opt_provider ON FUNCTION function_with_argtypes IS security_label
			{
				$$ = ast.NewSecLabelStmt(ast.OBJECT_FUNCTION, $6, $3, $8)
			}
		|	SECURITY LABEL opt_provider ON LARGE_P OBJECT_P NumericOnly IS security_label
			{
				$$ = ast.NewSecLabelStmt(ast.OBJECT_LARGEOBJECT, $7, $3, $9)
			}
		|	SECURITY LABEL opt_provider ON PROCEDURE function_with_argtypes IS security_label
			{
				$$ = ast.NewSecLabelStmt(ast.OBJECT_PROCEDURE, $6, $3, $8)
			}
		|	SECURITY LABEL opt_provider ON ROUTINE function_with_argtypes IS security_label
			{
				$$ = ast.NewSecLabelStmt(ast.OBJECT_ROUTINE, $6, $3, $8)
			}
		;

/*
 * opt_provider and security_label
 * From postgres/src/backend/parser/gram.y:5034-5042
 */
opt_provider:
		FOR NonReservedWord_or_Sconst	{ $$ = $2 }
	|	/* EMPTY */						{ $$ = "" }
		;

security_label:
		Sconst		{ $$ = $1 }
	|	NULL_P		{ $$ = "" }
		;

/*
 * DO statement
 * From postgres/src/backend/parser/gram.y:5044-5050
 */
DoStmt:
		DO dostmt_opt_list
			{
				$$ = ast.NewDoStmt($2)
			}
		;

/*
 * dostmt_opt_list and dostmt_opt_item
 * From postgres/src/backend/parser/gram.y:5052-5071
 */
dostmt_opt_list:
		dostmt_opt_item						{ $$ = ast.NewNodeList($1) }
	|	dostmt_opt_list dostmt_opt_item		{ $$ = $1; $$.Append($2) }
		;

dostmt_opt_item:
		Sconst
			{
				$$ = ast.NewDefElem("as", ast.NewString($1))
			}
	|	LANGUAGE NonReservedWord_or_Sconst
			{
				$$ = ast.NewDefElem("language", ast.NewString($2))
			}
		;

/*
 * CALL statement
 * From postgres/src/backend/parser/gram.y:5073-5078
 */
CallStmt:
		CALL func_application
			{
				$$ = ast.NewCallStmt($2.(*ast.FuncCall))
			}
		;

/*****************************************************************************
 *
 * ALTER ... SET SCHEMA statements
 *
 *****************************************************************************/

AlterObjectSchemaStmt:
			ALTER AGGREGATE aggregate_with_argtypes SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_AGGREGATE, $6)
					stmt.Object = $3
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER COLLATION any_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_COLLATION, $6)
					stmt.Object = $3
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER CONVERSION_P any_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_CONVERSION, $6)
					stmt.Object = $3
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER DOMAIN_P any_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_DOMAIN, $6)
					stmt.Object = $3
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER EXTENSION name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_EXTENSION, $6)
					stmt.Object = ast.NewString($3)
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER FUNCTION function_with_argtypes SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_FUNCTION, $6)
					stmt.Object = $3
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER OPERATOR operator_with_argtypes SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_OPERATOR, $6)
					stmt.Object = $3
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER OPERATOR CLASS any_name USING name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_OPCLASS, $9)
					// Create list with access method name first, then class name
					objList := ast.NewNodeList(ast.NewString($6))
					for _, item := range $4.Items {
						objList.Append(item)
					}
					stmt.Object = objList
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER OPERATOR FAMILY any_name USING name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_OPFAMILY, $9)
					// Create list with access method name first, then family name
					objList := ast.NewNodeList(ast.NewString($6))
					for _, item := range $4.Items {
						objList.Append(item)
					}
					stmt.Object = objList
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER PROCEDURE function_with_argtypes SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_PROCEDURE, $6)
					stmt.Object = $3
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER ROUTINE function_with_argtypes SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_ROUTINE, $6)
					stmt.Object = $3
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER TABLE relation_expr SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_TABLE, $6)
					stmt.Relation = $3
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER TABLE IF_P EXISTS relation_expr SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_TABLE, $8)
					stmt.Relation = $5
					stmt.MissingOk = true
					$$ = stmt
				}
		|	ALTER STATISTICS any_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_STATISTIC_EXT, $6)
					stmt.Object = $3
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER TEXT_P SEARCH PARSER any_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_TSPARSER, $8)
					stmt.Object = $5
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER TEXT_P SEARCH DICTIONARY any_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_TSDICTIONARY, $8)
					stmt.Object = $5
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER TEXT_P SEARCH TEMPLATE any_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_TSTEMPLATE, $8)
					stmt.Object = $5
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER TEXT_P SEARCH CONFIGURATION any_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_TSCONFIGURATION, $8)
					stmt.Object = $5
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER SEQUENCE qualified_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_SEQUENCE, $6)
					stmt.Relation = $3
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER SEQUENCE IF_P EXISTS qualified_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_SEQUENCE, $8)
					stmt.Relation = $5
					stmt.MissingOk = true
					$$ = stmt
				}
		|	ALTER VIEW qualified_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_VIEW, $6)
					stmt.Relation = $3
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER VIEW IF_P EXISTS qualified_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_VIEW, $8)
					stmt.Relation = $5
					stmt.MissingOk = true
					$$ = stmt
				}
		|	ALTER MATERIALIZED VIEW qualified_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_MATVIEW, $7)
					stmt.Relation = $4
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_MATVIEW, $9)
					stmt.Relation = $6
					stmt.MissingOk = true
					$$ = stmt
				}
		|	ALTER FOREIGN TABLE relation_expr SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_FOREIGN_TABLE, $7)
					stmt.Relation = $4
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER FOREIGN TABLE IF_P EXISTS relation_expr SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_FOREIGN_TABLE, $9)
					stmt.Relation = $6
					stmt.MissingOk = true
					$$ = stmt
				}
		|	ALTER TYPE_P any_name SET SCHEMA name
				{
					stmt := ast.NewAlterObjectSchemaStmt(ast.OBJECT_TYPE, $6)
					stmt.Object = $3
					stmt.MissingOk = false
					$$ = stmt
				}
		;

/*****************************************************************************
 *
 * ALTER ... OWNER TO statements
 *
 *****************************************************************************/

AlterOwnerStmt:
			ALTER AGGREGATE aggregate_with_argtypes OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_AGGREGATE,
						Object:     $3,
						Newowner:   $6,
					}
				}
		|	ALTER COLLATION any_name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_COLLATION,
						Object:     $3,
						Newowner:   $6,
					}
				}
		|	ALTER CONVERSION_P any_name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_CONVERSION,
						Object:     $3,
						Newowner:   $6,
					}
				}
		|	ALTER DATABASE name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_DATABASE,
						Object:     ast.NewString($3),
						Newowner:   $6,
					}
				}
		|	ALTER DOMAIN_P any_name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_DOMAIN,
						Object:     $3,
						Newowner:   $6,
					}
				}
		|	ALTER FUNCTION function_with_argtypes OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_FUNCTION,
						Object:     $3,
						Newowner:   $6,
					}
				}
		|	ALTER opt_procedural LANGUAGE name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_LANGUAGE,
						Object:     ast.NewString($4),
						Newowner:   $7,
					}
				}
		|	ALTER LARGE_P OBJECT_P NumericOnly OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_LARGEOBJECT,
						Object:     $4,
						Newowner:   $7,
					}
				}
		|	ALTER OPERATOR operator_with_argtypes OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_OPERATOR,
						Object:     $3,
						Newowner:   $6,
					}
				}
		|	ALTER OPERATOR CLASS any_name USING name OWNER TO RoleSpec
				{
					list := ast.NewNodeList(ast.NewString($6))
					for _, item := range $4.Items {
						list.Append(item)
					}
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_OPCLASS,
						Object:     list,
						Newowner:   $9,
					}
				}
		|	ALTER OPERATOR FAMILY any_name USING name OWNER TO RoleSpec
				{
					list := ast.NewNodeList(ast.NewString($6))
					for _, item := range $4.Items {
						list.Append(item)
					}
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_OPFAMILY,
						Object:     list,
						Newowner:   $9,
					}
				}
		|	ALTER PROCEDURE function_with_argtypes OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_PROCEDURE,
						Object:     $3,
						Newowner:   $6,
					}
				}
		|	ALTER ROUTINE function_with_argtypes OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_ROUTINE,
						Object:     $3,
						Newowner:   $6,
					}
				}
		|	ALTER SCHEMA name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_SCHEMA,
						Object:     ast.NewString($3),
						Newowner:   $6,
					}
				}
		|	ALTER TYPE_P any_name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_TYPE,
						Object:     $3,
						Newowner:   $6,
					}
				}
		|	ALTER TABLESPACE name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_TABLESPACE,
						Object:     ast.NewString($3),
						Newowner:   $6,
					}
				}
		|	ALTER STATISTICS any_name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_STATISTIC_EXT,
						Object:     $3,
						Newowner:   $6,
					}
				}
		|	ALTER TEXT_P SEARCH DICTIONARY any_name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_TSDICTIONARY,
						Object:     $5,
						Newowner:   $8,
					}
				}
		|	ALTER TEXT_P SEARCH CONFIGURATION any_name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_TSCONFIGURATION,
						Object:     $5,
						Newowner:   $8,
					}
				}
		|	ALTER FOREIGN DATA_P WRAPPER name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_FDW,
						Object:     ast.NewString($5),
						Newowner:   $8,
					}
				}
		|	ALTER SERVER name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_FOREIGN_SERVER,
						Object:     ast.NewString($3),
						Newowner:   $6,
					}
				}
		|	ALTER EVENT TRIGGER name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_EVENT_TRIGGER,
						Object:     ast.NewString($4),
						Newowner:   $7,
					}
				}
		|	ALTER PUBLICATION name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_PUBLICATION,
						Object:     ast.NewString($3),
						Newowner:   $6,
					}
				}
		|	ALTER SUBSCRIPTION name OWNER TO RoleSpec
				{
					$$ = &ast.AlterOwnerStmt{
						BaseNode:   ast.BaseNode{Tag: ast.T_AlterOwnerStmt},
						ObjectType: ast.OBJECT_SUBSCRIPTION,
						Object:     ast.NewString($3),
						Newowner:   $6,
					}
				}
		;

/*****************************************************************************
 *
 * ALTER OPERATOR statements
 *
 *****************************************************************************/

AlterOperatorStmt:
			ALTER OPERATOR operator_with_argtypes SET '(' operator_def_list ')'
				{
					$$ = ast.NewAlterOperatorStmt($3, $6)
				}
		;

/*****************************************************************************
 *
 * ALTER ... DEPENDS ON EXTENSION statements
 *
 *****************************************************************************/

AlterObjectDependsStmt:
			ALTER FUNCTION function_with_argtypes opt_no DEPENDS ON EXTENSION name
				{
					stmt := ast.NewAlterObjectDependsStmt(ast.OBJECT_FUNCTION, ast.NewString($8), $4)
					stmt.Object = $3
					$$ = stmt
				}
		|	ALTER PROCEDURE function_with_argtypes opt_no DEPENDS ON EXTENSION name
				{
					stmt := ast.NewAlterObjectDependsStmt(ast.OBJECT_PROCEDURE, ast.NewString($8), $4)
					stmt.Object = $3
					$$ = stmt
				}
		|	ALTER ROUTINE function_with_argtypes opt_no DEPENDS ON EXTENSION name
				{
					stmt := ast.NewAlterObjectDependsStmt(ast.OBJECT_ROUTINE, ast.NewString($8), $4)
					stmt.Object = $3
					$$ = stmt
				}
		|	ALTER TRIGGER name ON qualified_name opt_no DEPENDS ON EXTENSION name
				{
					stmt := ast.NewAlterObjectDependsStmt(ast.OBJECT_TRIGGER, ast.NewString($10), $6)
					stmt.Relation = $5
					stmt.Object = ast.NewString($3)
					$$ = stmt
				}
		|	ALTER MATERIALIZED VIEW qualified_name opt_no DEPENDS ON EXTENSION name
				{
					stmt := ast.NewAlterObjectDependsStmt(ast.OBJECT_MATVIEW, ast.NewString($9), $5)
					stmt.Relation = $4
					$$ = stmt
				}
		|	ALTER INDEX qualified_name opt_no DEPENDS ON EXTENSION name
				{
					stmt := ast.NewAlterObjectDependsStmt(ast.OBJECT_INDEX, ast.NewString($8), $4)
					stmt.Relation = $3
					$$ = stmt
				}
		;

opt_no:		NO				{ $$ = true }
		|	/* EMPTY */		{ $$ = false }
		;

/*****************************************************************************
 *
 * ALTER COLLATION statements
 *
 *****************************************************************************/

AlterCollationStmt:
			ALTER COLLATION any_name REFRESH VERSION_P
				{
					$$ = ast.NewAlterCollationStmt($3)
				}
		;

/*****************************************************************************
 *
 * ALTER DATABASE statements
 *
 *****************************************************************************/

AlterDatabaseStmt:
			ALTER DATABASE name WITH createdb_opt_list
				{
					$$ = ast.NewAlterDatabaseStmt($3, $5)
				}
		|	ALTER DATABASE name createdb_opt_list
				{
					$$ = ast.NewAlterDatabaseStmt($3, $4)
				}
		|	ALTER DATABASE name SET TABLESPACE name
				{
					optList := ast.NewNodeList()
					optList.Append(ast.NewDefElem("tablespace", ast.NewString($6)))
					$$ = ast.NewAlterDatabaseStmt($3, optList)
				}
		|	ALTER DATABASE name REFRESH COLLATION VERSION_P
				{
					$$ = ast.NewAlterDatabaseRefreshCollStmt($3)
				}
		;

createdb_opt_list:
			createdb_opt_items						{ $$ = $1 }
		|	/* EMPTY */								{ $$ = nil }
		;

createdb_opt_items:
			createdb_opt_item						{ $$ = ast.NewNodeList($1) }
		|	createdb_opt_items createdb_opt_item	{ $1.Append($2); $$ = $1 }
		;

createdb_opt_item:
			createdb_opt_name opt_equal SignedIconst
				{
					$$ = ast.NewDefElem($1, ast.NewInteger($3))
				}
		|	createdb_opt_name opt_equal opt_boolean_or_string
				{
					$$ = ast.NewDefElem($1, ast.NewString($3))
				}
		|	createdb_opt_name opt_equal DEFAULT
				{
					$$ = ast.NewDefElem($1, nil)
				}
		;

createdb_opt_name:
			IDENT									{ $$ = $1 }
		|	CONNECTION LIMIT						{ $$ = "connection_limit" }
		|	ENCODING								{ $$ = "encoding" }
		|	LOCATION								{ $$ = "location" }
		|	OWNER									{ $$ = "owner" }
		|	TABLESPACE								{ $$ = "tablespace" }
		|	TEMPLATE								{ $$ = "template" }
		;

opt_equal:	'='										{ }
		|	/* EMPTY */								{ }
		;

AlterDatabaseSetStmt:
			ALTER DATABASE name SetResetClause
				{
					$$ = ast.NewAlterDatabaseSetStmt($3, $4)
				}
		;

/*****************************************************************************
 *
 * ALTER TEXT SEARCH CONFIGURATION statements
 *
 *****************************************************************************/

AlterTSConfigurationStmt:
			ALTER TEXT_P SEARCH CONFIGURATION any_name ADD_P MAPPING FOR name_list any_with any_name_list
				{
					stmt := ast.NewAlterTSConfigurationStmt(ast.ALTER_TSCONFIG_ADD_MAPPING, $5)
					stmt.Tokentype = $9
					stmt.Dicts = $11
					stmt.Override = false
					stmt.Replace = false
					$$ = stmt
				}
		|	ALTER TEXT_P SEARCH CONFIGURATION any_name ALTER MAPPING FOR name_list any_with any_name_list
				{
					stmt := ast.NewAlterTSConfigurationStmt(ast.ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN, $5)
					stmt.Tokentype = $9
					stmt.Dicts = $11
					stmt.Override = true
					stmt.Replace = false
					$$ = stmt
				}
		|	ALTER TEXT_P SEARCH CONFIGURATION any_name ALTER MAPPING REPLACE any_name any_with any_name
				{
					stmt := ast.NewAlterTSConfigurationStmt(ast.ALTER_TSCONFIG_REPLACE_DICT, $5)
					stmt.Tokentype = nil
					// Create a list with two elements: old dict and new dict
					stmt.Dicts = ast.NewNodeList()
					stmt.Dicts.Append($9)
					stmt.Dicts.Append($11)
					stmt.Override = false
					stmt.Replace = true
					$$ = stmt
				}
		|	ALTER TEXT_P SEARCH CONFIGURATION any_name ALTER MAPPING FOR name_list REPLACE any_name any_with any_name
				{
					stmt := ast.NewAlterTSConfigurationStmt(ast.ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN, $5)
					stmt.Tokentype = $9
					// Create a list with two elements: old dict and new dict
					stmt.Dicts = ast.NewNodeList()
					stmt.Dicts.Append($11)
					stmt.Dicts.Append($13)
					stmt.Override = false
					stmt.Replace = true
					$$ = stmt
				}
		|	ALTER TEXT_P SEARCH CONFIGURATION any_name DROP MAPPING FOR name_list
				{
					stmt := ast.NewAlterTSConfigurationStmt(ast.ALTER_TSCONFIG_DROP_MAPPING, $5)
					stmt.Tokentype = $9
					stmt.MissingOk = false
					$$ = stmt
				}
		|	ALTER TEXT_P SEARCH CONFIGURATION any_name DROP MAPPING IF_P EXISTS FOR name_list
				{
					stmt := ast.NewAlterTSConfigurationStmt(ast.ALTER_TSCONFIG_DROP_MAPPING, $5)
					stmt.Tokentype = $11
					stmt.MissingOk = true
					$$ = stmt
				}
		;

/* Use this if TIME or ORDINALITY after WITH should be taken as an identifier */
any_with:	WITH
			| WITH_LA
		;

/*****************************************************************************
 *
 * ALTER TEXT SEARCH DICTIONARY statements
 *
 *****************************************************************************/

AlterTSDictionaryStmt:
			ALTER TEXT_P SEARCH DICTIONARY any_name definition
				{
					$$ = ast.NewAlterTSDictionaryStmt($5, $6)
				}
		;

/*
 * RESTART IDENTITY option for TRUNCATE
 * From postgres/src/backend/parser/gram.y:7037-7040
 */
opt_restart_seqs:
		CONTINUE_P IDENTITY_P	{ $$ = false }
	|	RESTART IDENTITY_P		{ $$ = true }
	|	/* EMPTY */				{ $$ = false }
		;

%%

// Lex implements the lexer interface for goyacc
func (l *Lexer) Lex(lval *yySymType) int {
	token := l.NextToken()
	if token == nil {
		return EOF // EOF = 0, exactly what yacc expects
	}

	// Set location and always populate both semantic value fields
	lval.location = token.Position
	lval.str = token.Value.Str
	lval.ival = token.Value.Ival

	// Simply return the token type - no complex switch needed!
	// All parser constants, keywords, operators, etc. work directly
	return token.Type
}

// Error implements the error interface for goyacc
func (l *Lexer) Error(s string) {
	l.RecordError(fmt.Errorf("parse error at position %d: %s", l.GetPosition(), s))
}

// ParseSQL parses SQL input and returns the AST
func ParseSQL(input string) ([]ast.Stmt, error) {
	lexer := NewLexer(input)
	yyParse(lexer)

	if lexer.HasErrors() {
		return nil, lexer.GetErrors()[0]
	}

	return lexer.GetParseTree(), nil
}
