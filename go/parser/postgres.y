/*
 * PostgreSQL Grammar File - Foundation and Expression Implementation
 * Ported from postgres/src/backend/parser/gram.y
 *
 * This implements the core grammar structure and foundational rules
 * following PostgreSQL patterns and Go/goyacc conventions
 */

%{
package parser

import (
	"fmt"
	"github.com/multigres/parser/go/parser/ast"
)

// Parser state for handling complex grammar constructs
var parserState struct {
	// Future parser state variables can be added here
}

// LexerInterface - implements the lexer interface expected by goyacc
// Note: The actual Lexer struct is defined in lexer.go
type LexerInterface interface {
	Lex(lval *yySymType) int
	Error(s string)
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

	// Specific AST node types
	into       *ast.IntoClause
	onconflict *ast.OnConflictClause
	createStmt *ast.CreateStmt
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
%token <keyword> SELECT FROM WHERE ONLY TABLE LIMIT OFFSET ORDER_P BY GROUP_P HAVING INTO ON
%token <keyword> JOIN INNER_P LEFT RIGHT FULL OUTER_P CROSS NATURAL USING
%token <keyword> RECURSIVE MATERIALIZED LATERAL VALUES SEARCH BREADTH DEPTH CYCLE FIRST_P LAST_P SET ASC DESC
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
%token <keyword> ABORT_P ABSENT ABSOLUTE_P ACCESS ACTION ADD_P ADMIN AFTER AGGREGATE ALSO ALWAYS
%token <keyword> ANALYSE ASENSITIVE ASSERTION ASSIGNMENT ATOMIC ATTACH ATTRIBUTE AUTHORIZATION
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
%type <str>          ColId ColLabel name BareColLabel NonReservedWord generic_option_name RoleId
%type <list>         name_list
%type <list>         columnList any_name
%type <list>         qualified_name_list any_name_list
%type <str>          opt_single_name
%type <str>          unreserved_keyword col_name_keyword type_func_name_keyword reserved_keyword bare_label_keyword
%type <list>         opt_name_list
%type <ival>         opt_if_exists opt_if_not_exists
%type <ival>         opt_or_replace
%type <bval>         opt_concurrently
%type <node>         opt_with
%type <node>         alter_using
%type <list>         alter_generic_options alter_generic_option_list
%type <node>         generic_option_arg
%type <defelt>       alter_generic_option_elem
%type <str>          set_access_method_name
%type <node>         replica_identity
%type <list>         func_args OptWith
%type <node>         function_with_argtypes aggregate_with_argtypes

/* Expression types */
%type <node>         a_expr b_expr c_expr AexprConst columnref
%type <node>         func_expr case_expr case_arg case_default when_clause
%type <list>         when_clause_list
%type <node>         array_expr explicit_row implicit_row
%type <list>         array_expr_list
%type <list>         func_arg_list
%type <node>         func_arg_expr
%type <list>         indirection opt_indirection
%type <node>         indirection_el opt_slice_bound
%type <ival>         Iconst SignedIconst
%type <str>          Sconst
%type <str>          type_function_name character attr_name param_name
%type <typnam>	     Typename SimpleTypename
				     GenericType Numeric opt_float
				     Character
				     CharacterWithLength CharacterWithoutLength
				     ConstDatetime
				     Bit BitWithLength BitWithoutLength
%type <typnam>	     func_type
%type <list>         attrs opt_type_modifiers
%type <ival>         opt_timezone opt_varying
%type <list>         expr_list
%type <list>         opt_sort_clause
%type <node>         func_application within_group_clause filter_clause over_clause
%type <list>         qual_Op any_operator qual_all_Op
%type <str>          all_Op MathOp
%type <node>         in_expr
%type <ival>         opt_asymmetric

/* SELECT statement types - Phase 3C */
%type <stmt>         SelectStmt PreparableStmt select_no_parens select_with_parens simple_select
/* Phase 3E DML statement types */
%type <stmt>         InsertStmt UpdateStmt DeleteStmt MergeStmt CopyStmt
%type <stmt>         CreateStmt IndexStmt AlterTableStmt DropStmt RenameStmt
%type <node>         insert_rest
%type <list>         insert_column_list set_clause_list set_target_list merge_when_list
%type <target>       insert_column_item set_target
%type <list>         set_clause
%type <onconflict>   opt_on_conflict
%type <node>         where_or_current_clause
%type <list>         returning_clause merge_values_clause opt_collate
%type <node>         merge_when_clause opt_merge_when_condition opt_conf_expr merge_update merge_delete merge_insert index_elem index_elem_options
%type <ival>         override_kind merge_when_tgt_matched merge_when_tgt_not_matched opt_asc_desc opt_nulls_order
%type <ival>         copy_from opt_program opt_freeze opt_verbose opt_analyze opt_full
%type <str>          cursor_name copy_file_name
%type <node>         opt_binary copy_delimiter copy_opt_item
%type <list>         copy_options copy_opt_list copy_generic_opt_list copy_generic_opt_arg_list opt_column_list index_elem_list index_params
%type <node>         copy_generic_opt_elem copy_generic_opt_arg copy_generic_opt_arg_list_item NumericOnly var_value
%type <node>         set_statistics_value
%type <ival>         opt_set_data
%type <list>         type_name_list
%type <str>          opt_boolean_or_string NonReservedWord_or_Sconst var_name
%type <list>         target_list opt_target_list var_list
%type <target>       target_el
%type <list>         from_clause from_list
%type <node>         table_ref
%type <node>         where_clause opt_where_clause
%type <alias>        alias_clause opt_alias_clause opt_alias_clause_for_join_using
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
%type <node>         alter_table_cmd partition_cmd index_partition_cmd RoleSpec
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

%type <stmt>         CreateFunctionStmt CreateTrigStmt ViewStmt ReturnStmt VariableSetStmt VariableResetStmt
%type <vsetstmt>     generic_set set_rest set_rest_more generic_reset reset_rest SetResetClause FunctionSetResetClause
%type <list>         func_name func_args_with_defaults func_args_with_defaults_list
%type <funparam>     func_arg_with_default func_arg table_func_column
%type <funparammode> arg_class
%type <typnam>       func_return
%type <list>         opt_createfunc_opt_list createfunc_opt_list transform_type_list
%type <defelt>       createfunc_opt_item common_func_opt_item
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
%type <list>         attrs

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
		;

/*
 * Generic SQL statement
 * This is highly simplified for now - will expand significantly
 * From postgres/src/backend/parser/gram.y:1075 onwards
 */
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
		|	CreateTrigStmt							{ $$ = $1 }
		|	ViewStmt								{ $$ = $1 }
		|	VariableSetStmt							{ $$ = $1 }
		|	VariableResetStmt						{ $$ = $1 }
		|	/* Empty for now - will add other statement types in later phases */
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
			OR REPLACE								{ $$ = 1 }
		|	/* EMPTY */								{ $$ = 0 }
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
			ColId
			{
				$$ = ast.NewNodeList(ast.NewString($1))
			}
		|	columnList ',' ColId
			{
				$1.Append(ast.NewString($3))
				$$ = $1
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
		|	a_expr TYPECAST Typename
			{
				$$ = ast.NewTypeCast($1, $3, 0)
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
		|	a_expr AND a_expr
			{
				$$ = ast.NewBoolExpr(ast.AND_EXPR, ast.NewNodeList($1, $3))
			}
		|	a_expr OR a_expr
			{
				$$ = ast.NewBoolExpr(ast.OR_EXPR, ast.NewNodeList($1, $3))
			}
		|	NOT a_expr %prec NOT
			{
				$$ = ast.NewBoolExpr(ast.NOT_EXPR, ast.NewNodeList($2))
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
		|	a_expr LIKE a_expr
			{
				name := ast.NewNodeList(ast.NewString("~~"))
				$$ = ast.NewA_Expr(ast.AEXPR_LIKE, name, $1, $3, 0)
			}
		|	a_expr LIKE a_expr ESCAPE a_expr %prec LIKE
			{
				// Create like_escape function call
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
				// Create like_escape function call
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
				// Create like_escape function call
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
				// Create like_escape function call
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("like_escape"))
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($4, $6), 0)
				name := ast.NewNodeList(ast.NewString("!~~*"))
				$$ = ast.NewA_Expr(ast.AEXPR_ILIKE, name, $1, escapeFunc, 0)
			}
		|	a_expr COLLATE any_name
			{
				// Pass the NodeList directly to NewCollateClause
				nodeList := $3
				collateClause := ast.NewCollateClause(nodeList)
				collateClause.Arg = $1
				$$ = collateClause
			}
		|	a_expr AT TIME ZONE a_expr %prec AT
			{
				// Create timezone function call
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("timezone"))
				$$ = ast.NewFuncCall(funcName, ast.NewNodeList($5, $1), 0)
			}
		|	a_expr AT LOCAL %prec AT
			{
				// Create timezone function call with no argument
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("timezone"))
				$$ = ast.NewFuncCall(funcName, ast.NewNodeList($1), 0)
			}
		|	a_expr qual_Op a_expr %prec Op
			{
				$$ = ast.NewA_Expr(ast.AEXPR_OP, $2, $1, $3, 0)
			}
		|	qual_Op a_expr %prec Op
			{
				$$ = ast.NewA_Expr(ast.AEXPR_OP, $1, nil, $2, 0)
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
				name := ast.NewNodeList(ast.NewString("="))
				$$ = ast.NewA_Expr(ast.AEXPR_IN, name, $1, $3, 0)
			}
		|	a_expr NOT_LA IN_P in_expr %prec NOT_LA
			{
				name := ast.NewNodeList(ast.NewString("<>"))
				$$ = ast.NewA_Expr(ast.AEXPR_IN, name, $1, $4, 0)
			}
		|	a_expr SIMILAR TO a_expr %prec SIMILAR
			{
				name := ast.NewNodeList(ast.NewString("~"))
				$$ = ast.NewA_Expr(ast.AEXPR_SIMILAR, name, $1, $4, 0)
			}
		|	a_expr SIMILAR TO a_expr ESCAPE a_expr %prec SIMILAR
			{
				// Create similar_escape function call
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("similar_escape"))
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($4, $6), 0)
				name := ast.NewNodeList(ast.NewString("~"))
				$$ = ast.NewA_Expr(ast.AEXPR_SIMILAR, name, $1, escapeFunc, 0)
			}
		|	a_expr NOT_LA SIMILAR TO a_expr %prec NOT_LA
			{
				name := ast.NewNodeList(ast.NewString("!~"))
				$$ = ast.NewA_Expr(ast.AEXPR_SIMILAR, name, $1, $5, 0)
			}
		|	a_expr NOT_LA SIMILAR TO a_expr ESCAPE a_expr %prec NOT_LA
			{
				// Create similar_escape function call
				funcName := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("similar_escape"))
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($5, $7), 0)
				name := ast.NewNodeList(ast.NewString("!~"))
				$$ = ast.NewA_Expr(ast.AEXPR_SIMILAR, name, $1, escapeFunc, 0)
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
				name := ast.NewNodeList(ast.NewString("-"))
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, nil, $2, 0)
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
				parenExpr := ast.NewParenExpr($2, 0)
				if $4 != nil {
					$$ = ast.NewA_Indirection(parenExpr, $4, 0)
				} else {
					$$ = parenExpr
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
				// TODO: Fix GROUPING function implementation
				// Current implementation is simplified and needs proper expr_list handling
				// The expr_list should be properly converted to NodeList and assigned to Args
				grouping := &ast.GroupingFunc{
					BaseExpr: ast.BaseExpr{BaseNode: ast.BaseNode{Tag: ast.T_GroupingFunc}},
					Args:     nil, // We'll need to convert expr_list properly
				}
				$$ = grouping
			}
		;

/* Constants */
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
		;

Iconst:		ICONST								{ $$ = $1 }
		;

Sconst:		SCONST								{ $$ = $1 }
		;

SignedIconst:	Iconst							{ $$ = $1 }
		|	'+' Iconst							{ $$ = $2 }
		|	'-' Iconst							{ $$ = -$2 }
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
				// For now, just return the func_application
				// Note: In full implementation, would apply within_group_clause, filter_clause, over_clause
				$$ = $1
			}
		;

/* Function expression without window clause - used in table functions */
func_expr_windowless:
			func_application                    { $$ = $1 }
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
		|	func_name '(' func_arg_list opt_sort_clause ')'
			{
				funcCall := ast.NewFuncCall($1, $3, 0)
				// Note: In full implementation, would set agg_order from $4
				$$ = funcCall
			}
		|	func_name '(' VARIADIC func_arg_expr opt_sort_clause ')'
			{
				funcCall := ast.NewFuncCall($1, ast.NewNodeList($4), 0)
				// Note: In full implementation, would set func_variadic = true and agg_order from $5
				$$ = funcCall
			}
		|	func_name '(' func_arg_list ',' VARIADIC func_arg_expr opt_sort_clause ')'
			{
				$3.Append($6)
				args := $3
				funcCall := ast.NewFuncCall($1, args, 0)
				// Note: In full implementation, would set func_variadic = true and agg_order from $7
				$$ = funcCall
			}
		|	func_name '(' ALL func_arg_list opt_sort_clause ')'
			{
				funcCall := ast.NewFuncCall($1, $4, 0)
				// Note: In full implementation, would mark as aggregate and set agg_order from $5
				$$ = funcCall
			}
		|	func_name '(' DISTINCT func_arg_list opt_sort_clause ')'
			{
				funcCall := ast.NewFuncCall($1, $4, 0)
				// Note: In full implementation, would set agg_distinct = true and agg_order from $5
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
explicit_row:	ROW '(' expr_list ')'			{ $$ = $3 }
		|	ROW '(' ')'							{ $$ = ast.NewNodeList() }
		;

implicit_row:	'(' expr_list ',' a_expr ')'
			{
				$2.Append($4)
				$$ = $2
			}
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
Typename:	SimpleTypename
			{
				$$ = $1
			}
		;

SimpleTypename: GenericType						{ $$ = $1 }
		|	Numeric								{ $$ = $1 }
		|	Bit									{ $$ = $1 }
		|	Character							{ $$ = $1 }
		|	ConstDatetime						{ $$ = $1 }
		;

type_function_name: IDENT					{ $$ = $1 }
		|	unreserved_keyword					{ $$ = $1 }
		|	type_func_name_keyword				{ $$ = $1 }
		;

attr_name:	ColLabel						{ $$ = $1 }
		;

param_name:	type_function_name				{ $$ = $1 }
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
				// Note: In full implementation, would set type modifiers
				$$ = typeName
			}
		|	type_function_name attrs opt_type_modifiers
			{
				// Create qualified type name from name + attrs
				name := ast.NewString($1)
				names := &ast.NodeList{Items: append([]ast.Node{name}, $2.Items...)}
				typeName := makeTypeNameFromNodeList(names)
				// Note: In full implementation, would set type modifiers
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
			}
		|	DEC opt_type_modifiers
			{
				$$ = makeTypeNameFromString("numeric")
			}
		|	NUMERIC opt_type_modifiers
			{
				$$ = makeTypeNameFromString("numeric")
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
				$$ = makeTypeNameFromString(typeName)
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
				$$ = makeTypeNameFromString(typeName)
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
				$$ = makeTypeNameFromString(typeName)
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
opt_sort_clause: /* EMPTY */					{ $$ = nil }
		;

within_group_clause: /* EMPTY */				{ $$ = nil }
		;

filter_clause: /* EMPTY */					{ $$ = nil }
		;

over_clause: /* EMPTY */					{ $$ = nil }
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
			select_no_parens						{ $$ = $1 }
		|	select_with_parens						{ $$ = $1 }
		;

PreparableStmt:
			SelectStmt								{ $$ = $1 }
			/* TODO: Add InsertStmt, UpdateStmt, DeleteStmt, MergeStmt when implemented */
		;

select_with_parens:
			'(' select_no_parens ')'				{ $$ = $2 }
		|	'(' select_with_parens ')'				{ $$ = $2 }
		;

/*
 * This rule parses the equivalent of the standard's <query expression>.
 * From postgres/src/backend/parser/gram.y:12698+
 */
select_no_parens:
			simple_select							{ $$ = $1 }
		|	with_clause simple_select
			{
				selectStmt := $2.(*ast.SelectStmt)
				selectStmt.WithClause = $1
				$$ = selectStmt
			}
		;

/*
 * simple_select - Core SELECT statement structure
 * From postgres/src/backend/parser/gram.y:12790+
 */
simple_select:
			SELECT opt_all_clause opt_target_list
			into_clause from_clause where_clause
			{
				selectStmt := ast.NewSelectStmt()
				if $3 != nil {
					selectStmt.TargetList = $3
				}
				selectStmt.IntoClause = $4
				selectStmt.FromClause = $5
				selectStmt.WhereClause = $6
				$$ = selectStmt
			}
		|	SELECT distinct_clause target_list
			into_clause from_clause where_clause
			{
				selectStmt := ast.NewSelectStmt()
				selectStmt.DistinctClause = $2
				if $3 != nil {
					selectStmt.TargetList = $3
				}
				selectStmt.IntoClause = $4
				selectStmt.FromClause = $5
				selectStmt.WhereClause = $6
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
		|	LATERAL select_with_parens opt_alias_clause
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
		|	func_table opt_alias_clause
			{
				rangeFunc := $1.(*ast.RangeFunction)
				if $2 != nil {
					rangeFunc.Alias = $2
				}
				$$ = rangeFunc
			}
		|	LATERAL func_table opt_alias_clause
			{
				rangeFunc := $2.(*ast.RangeFunction)
				rangeFunc.Lateral = true
				if $3 != nil {
					rangeFunc.Alias = $3
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
		|	LATERAL xmltable opt_alias_clause
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
		|	LATERAL json_table opt_alias_clause
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
				selectStmt.ValuesLists = []*ast.NodeList{exprList}
				$$ = selectStmt
			}
		|	values_clause ',' '(' expr_list ')'
			{
				/* Add additional VALUES row to existing SelectStmt */
				selectStmt := $1.(*ast.SelectStmt)
				exprList := $4
				selectStmt.ValuesLists = append(selectStmt.ValuesLists, exprList)
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

opt_where_clause:
			where_clause							{ $$ = $1 }
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
		|	/* EMPTY */								{ $$ = nil }
		;


/*
 * INTO clause rules (for SELECT INTO)
 * From postgres/src/backend/parser/gram.y:15700+
 */
into_clause:
			INTO qualified_name
			{
				$$ = ast.NewIntoClause($2, nil, "", nil, ast.ONCOMMIT_NOOP, "", nil, false, 0)
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
			AS '(' TableFuncElementList ')'		{ $$ = $3 }
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
				// Note: opt_collate_clause support can be added when needed
				// PostgreSQL sets: n->collClause = (CollateClause *) $3;
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
				// TODO: Process column options from $3
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
				if $10 != nil {
					jsonTable.Columns = $10
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
				$$ = ast.NewResTarget($1, $2)
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
				$$ = ast.NewResTarget($1, $2)
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
			f.FVal = "-" + f.FVal
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
		PROCEDURAL
	|	/* EMPTY */
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
		|	/* EMPTY */
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

/*
 * Simple index element list for ON CONFLICT
 * For now, just supports column names - full index expressions can be added later
 */
index_elem_list:
			ColId
			{
				$$ = ast.NewNodeList(ast.NewString($1))
			}
		|	index_elem_list ',' ColId
			{
				$1.Append(ast.NewString($3)); $$ = $1
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
					colDef.Constraints = $6
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
					partElem := ast.NewPartitionElem("", $2, 0)
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

/* Optional NOWAIT - stub for now */
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
					roleSpec := $1.(*ast.RoleSpec)

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
					def.Collclause = $7.(*ast.CollateClause)
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
					cmd.Newowner = $3.(*ast.RoleSpec)
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
		SET set_rest_more						{ $$ = $2 }
	|	VariableResetStmt						{ $$ = $1.(*ast.VariableSetStmt) }
	;

VariableResetStmt:
		RESET reset_rest						{ $$ = ast.Stmt($2) }
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

/*
 * object types taking name/name_list
 *
 * DROP handles some of them separately
 */

object_type_name:
			drop_type_name				{ $$ = $1 }
		|	DATABASE					{ $$ = ast.OBJECT_DATABASE }
		|	ROLE						{ $$ = ast.OBJECT_ROLE }
		|	SUBSCRIPTION				{ $$ = ast.OBJECT_SUBSCRIPTION }
		|	TABLESPACE					{ $$ = ast.OBJECT_TABLESPACE }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				RENAME
 *
 *****************************************************************************/

/* For now, simplified function/aggregate specs until we implement full argument support */

func_args:
		'(' ')' { $$ = ast.NewNodeList() }
	;

function_with_argtypes:
		func_name func_args
			{
				objWithArgs := &ast.ObjectWithArgs{
					BaseNode: ast.BaseNode{Tag: ast.T_ObjectWithArgs},
					Objname: $1,
					Objargs: $2,
					ObjfuncArgs: $2,
					ArgsUnspecified: false,
				}
				$$ = objWithArgs
			}
	;

aggregate_with_argtypes:
		func_name func_args
			{
				objWithArgs := &ast.ObjectWithArgs{
					BaseNode: ast.BaseNode{Tag: ast.T_ObjectWithArgs},
					Objname: $1,
					Objargs: $2,
					ObjfuncArgs: $2,
					ArgsUnspecified: false,
				}
				$$ = objWithArgs
			}
	;

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
						Replace: $2 != 0,
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
						Replace: $2 != 0,
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
						Replace: $2 != 0,
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
						Replace: $2 != 0,
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
					$$ = &ast.TypeName{
						Names: list,
						// TODO: Add %TYPE indicator
					}
				}
		|	SETOF type_function_name attrs '%' TYPE_P
				{
					// Handle SETOF %TYPE reference
					list := ast.NewNodeList()
					list.Append(ast.NewString($2))
					$$ = &ast.TypeName{
						Names: list,
						Setof: true,
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
				$$ = &ast.ReturnStmt{
					ReturnVal: $2,
				}
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

common_func_opt_item:
			CALLED ON NULL_P INPUT_P
				{
					$$ = ast.NewDefElem("strict", ast.NewBoolean(false))
				}
		|	RETURNS NULL_P ON NULL_P INPUT_P
				{
					$$ = ast.NewDefElem("strict", ast.NewBoolean(true))
				}
		|	STRICT_P
				{
					$$ = ast.NewDefElem("strict", ast.NewBoolean(true))
				}
		|	IMMUTABLE
				{
					$$ = ast.NewDefElem("volatility", ast.NewString("immutable"))
				}
		|	STABLE
				{
					$$ = ast.NewDefElem("volatility", ast.NewString("stable"))
				}
		|	VOLATILE
				{
					$$ = ast.NewDefElem("volatility", ast.NewString("volatile"))
				}
		|	EXTERNAL SECURITY DEFINER
				{
					$$ = ast.NewDefElem("security", ast.NewBoolean(true))
				}
		|	EXTERNAL SECURITY INVOKER
				{
					$$ = ast.NewDefElem("security", ast.NewBoolean(false))
				}
		|	SECURITY DEFINER
				{
					$$ = ast.NewDefElem("security", ast.NewBoolean(true))
				}
		|	SECURITY INVOKER
				{
					$$ = ast.NewDefElem("security", ast.NewBoolean(false))
				}
		|	LEAKPROOF
				{
					$$ = ast.NewDefElem("leakproof", ast.NewBoolean(true))
				}
		|	NOT LEAKPROOF
				{
					$$ = ast.NewDefElem("leakproof", ast.NewBoolean(false))
				}
		|	COST NumericOnly
				{
					$$ = ast.NewDefElem("cost", $2)
				}
		|	ROWS NumericOnly
				{
					$$ = ast.NewDefElem("rows", $2)
				}
		|	SUPPORT any_name
				{
					$$ = ast.NewDefElem("support", $2)
				}
		|	FunctionSetResetClause
				{
					$$ = ast.NewDefElem("set", $1)
				}
		|	PARALLEL ColId
				{
					$$ = ast.NewDefElem("parallel", ast.NewString($2))
				}
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
						Replace: $2 != 0,
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
						Replace: $2 != 0,
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
		|	NumericOnly								{ $$ = $1 }
		|	DEFAULT									{ $$ = ast.NewString("default") }
		|	LOCAL									{ $$ = ast.NewString("local") }
	|	TRUE_P									{ $$ = ast.NewString("true") }
	|	FALSE_P									{ $$ = ast.NewString("false") }
	|	ON										{ $$ = ast.NewString("on") }
	|	OFF										{ $$ = ast.NewString("off") }
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

// processConstraintAttributeSpec processes constraint attribute specification bitmask
// This is a simplified version of processCASbits from PostgreSQL
func processConstraintAttributeSpec(casbits int, constraint *ast.Constraint) {
	if casbits & ast.CAS_DEFERRABLE != 0 {
		constraint.Deferrable = true
	} else if casbits & ast.CAS_NOT_DEFERRABLE != 0 {
		constraint.Deferrable = false
	}

	if casbits & ast.CAS_INITIALLY_DEFERRED != 0 {
		constraint.Initdeferred = true
	} else if casbits & ast.CAS_INITIALLY_IMMEDIATE != 0 {
		constraint.Initdeferred = false
	}

	if casbits & ast.CAS_NOT_VALID != 0 {
		constraint.SkipValidation = true
		constraint.InitiallyValid = false
	} else {
		constraint.SkipValidation = false
		constraint.InitiallyValid = true
	}

	if casbits & ast.CAS_NO_INHERIT != 0 {
		constraint.IsNoInherit = true
	}
}



