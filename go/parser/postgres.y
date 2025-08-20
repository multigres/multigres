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

	// AST nodes
	node       ast.Node
	stmt       ast.Stmt
	stmtList   []ast.Stmt
	list       *ast.NodeList
	strList    []string
	astStrList []*ast.String

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
%token <keyword> FALSE_P ILIKE IN_P LIKE NULL_P SIMILAR TRUE_P WHEN
%token <keyword> IS ISNULL NOTNULL AT TIME ZONE LOCAL SYMMETRIC ASYMMETRIC TO
%token <keyword> OPERATOR
%token <keyword> SELECT FROM WHERE ONLY TABLE LIMIT OFFSET ORDER_P BY GROUP_P HAVING INTO ON
%token <keyword> JOIN INNER_P LEFT RIGHT FULL OUTER_P CROSS NATURAL USING
%token <keyword> RECURSIVE MATERIALIZED LATERAL VALUES SEARCH BREADTH DEPTH CYCLE FIRST_P SET
/* Type keywords */
%token <keyword> BIT NUMERIC INTEGER SMALLINT BIGINT REAL FLOAT_P DOUBLE_P PRECISION
%token <keyword> CHARACTER CHAR_P VARCHAR NATIONAL NCHAR VARYING
%token <keyword> TIMESTAMP INTERVAL INT_P DECIMAL_P DEC BOOLEAN_P
%token <keyword> VARIADIC
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
/* Expression precedence */
%right           UMINUS
%left            '+' '-'
%left            '*' '/' '%'
%left            '^'
%left            '|'
%left            '#'
%left            '&'

/*
 * Non-terminal type declarations for foundation and expressions
 */
%type <stmtList>     parse_toplevel stmtmulti
%type <stmt>         toplevel_stmt stmt
%type <str>          ColId ColLabel name
%type <node>         name_list columnList
%type <node>         qualified_name any_name
%type <node>         qualified_name_list any_name_list
%type <str>          opt_single_name
%type <str>          unreserved_keyword col_name_keyword type_func_name_keyword reserved_keyword
%type <node>         opt_qualified_name
%type <node>         opt_name_list
%type <ival>         opt_drop_behavior
%type <ival>         opt_if_exists opt_if_not_exists
%type <ival>         opt_or_replace
%type <ival>         opt_concurrently
%type <node>         opt_with OptWith

/* Expression types */
%type <node>         a_expr b_expr c_expr AexprConst columnref
%type <node>         func_expr
%type <astStrList>   func_name
%type <list>         func_arg_list
%type <node>         func_arg_expr
%type <list>         indirection opt_indirection
%type <node>         indirection_el opt_slice_bound
%type <ival>         Iconst SignedIconst
%type <str>          Sconst
%type <node>         Typename SimpleTypename GenericType
%type <node>         Numeric Bit Character ConstDatetime
%type <str>          type_function_name character attr_name param_name
%type <node>         CharacterWithLength CharacterWithoutLength BitWithLength BitWithoutLength
%type <list>         attrs opt_type_modifiers
%type <ival>         opt_timezone opt_varying
%type <node>         opt_float
%type <node>         expr_list
%type <list>         opt_sort_clause
%type <node>         func_application within_group_clause filter_clause over_clause
%type <astStrList>   qual_Op any_operator
%type <str>          all_Op MathOp
%type <node>         in_expr
%type <ival>         opt_asymmetric

/* SELECT statement types - Phase 3C */
%type <stmt>         SelectStmt PreparableStmt select_no_parens select_with_parens simple_select
%type <list>         target_list opt_target_list
%type <node>         target_el
%type <list>         from_clause from_list
%type <node>         table_ref relation_expr extended_relation_expr
%type <node>         where_clause opt_where_clause
%type <node>         alias_clause opt_alias_clause opt_alias_clause_for_join_using
%type <ival>         opt_all_clause
%type <list>         distinct_clause opt_distinct_clause
%type <node>         into_clause
/* Phase 3D JOIN and CTE types */
%type <node>         joined_table
%type <ival>         join_type opt_outer
%type <node>         join_qual using_clause
%type <node>         with_clause opt_with_clause
%type <list>         cte_list
%type <node>         common_table_expr
%type <ival>         opt_materialized
%type <node>         opt_search_clause opt_cycle_clause
%type <stmt>         values_clause

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
			qualified_name							{ $$ = $1 }
		|	/* EMPTY */								{ $$ = nil }
		;

opt_drop_behavior:
			CASCADE									{ $$ = int(ast.DropCascade) }
		|	RESTRICT								{ $$ = int(ast.DropRestrict) }
		|	/* EMPTY */								{ $$ = int(ast.DropRestrict) }
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
			CONCURRENTLY							{ $$ = 1 }
		|	/* EMPTY */								{ $$ = 0 }
		;

opt_with:
			WITH IDENT								{ $$ = ast.NewWithClause(nil, false, 0) }
		|	/* EMPTY */								{ $$ = nil }
		;

OptWith:
			WITH										{ $$ = ast.NewWithClause(nil, false, 0) }
		|	WITH_LA									{ $$ = ast.NewWithClause(nil, false, 0) }
		|	WITHOUT_LA								{ $$ = nil }
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
				list := $1.(*ast.NodeList)
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
				list := $1.(*ast.NodeList)
				list.Append(ast.NewString($3))
				$$ = list
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
		|	ColId '.' ColId
			{
				$$ = &ast.RangeVar{
					SchemaName: $1,
					RelName:    $3,
					Inh:        true, // inheritance enabled by default
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
				list := $1.(*ast.NodeList)
				list.Append($3)
				$$ = list
			}
		;

any_name:
			ColId
			{
				$$ = ast.NewNodeList(ast.NewString($1))
			}
		|	ColId '.' ColId
			{
				$$ = ast.NewNodeList(ast.NewString($1), ast.NewString($3))
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
				list := $1.(*ast.NodeList)
				list.Append($3)
				$$ = list
			}
		;

/*
 * Keyword categories - simplified for now
 * Will expand significantly in later phases
 */

unreserved_keyword:
			/* Will add unreserved keywords as needed */
			ALL										{ $$ = "all" }
		|	MATERIALIZED							{ $$ = "materialized" }
		|	RECURSIVE								{ $$ = "recursive" }
		|	SEARCH									{ $$ = "search" }
		|	BREADTH									{ $$ = "breadth" }
		|	DEPTH									{ $$ = "depth" }
		|	CYCLE									{ $$ = "cycle" }
		|	FIRST_P									{ $$ = "first" }
		|	SET										{ $$ = "set" }
		|	BY										{ $$ = "by" }
		|	TRUE_P									{ $$ = "true" }
		|	FALSE_P									{ $$ = "false" }
		;

col_name_keyword:
			/* Column name keywords - placeholder */
			IDENT									{ $$ = $1 }
		;

type_func_name_keyword:
			/* Type/function name keywords - placeholder */
			IDENT									{ $$ = $1 }
		;

reserved_keyword:
			/* Reserved keywords - placeholder */
			CREATE									{ $$ = "create" }
		|	DROP									{ $$ = "drop" }
		|	ALTER									{ $$ = "alter" }
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
				name := []*ast.String{ast.NewString("+")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, nil, $2, 0)
			}
		|	'-' a_expr %prec UMINUS
			{
				name := []*ast.String{ast.NewString("-")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, nil, $2, 0)
			}
		|	a_expr TYPECAST Typename
			{
				$$ = ast.NewTypeCast($1, $3.(*ast.TypeName), 0)
			}
		|	a_expr '+' a_expr
			{
				name := []*ast.String{ast.NewString("+")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '-' a_expr
			{
				name := []*ast.String{ast.NewString("-")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '*' a_expr
			{
				name := []*ast.String{ast.NewString("*")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '/' a_expr
			{
				name := []*ast.String{ast.NewString("/")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '%' a_expr
			{
				name := []*ast.String{ast.NewString("%")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '^' a_expr
			{
				name := []*ast.String{ast.NewString("^")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '<' a_expr
			{
				name := []*ast.String{ast.NewString("<")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '>' a_expr
			{
				name := []*ast.String{ast.NewString(">")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr '=' a_expr
			{
				name := []*ast.String{ast.NewString("=")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr LESS_EQUALS a_expr
			{
				name := []*ast.String{ast.NewString("<=")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr GREATER_EQUALS a_expr
			{
				name := []*ast.String{ast.NewString(">=")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	a_expr NOT_EQUALS a_expr
			{
				name := []*ast.String{ast.NewString("<>")}
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
				name := []*ast.String{ast.NewString("~~")}
				$$ = ast.NewA_Expr(ast.AEXPR_LIKE, name, $1, $3, 0)
			}
		|	a_expr LIKE a_expr ESCAPE a_expr %prec LIKE
			{
				// Create like_escape function call
				funcName := []*ast.String{ast.NewString("pg_catalog"), ast.NewString("like_escape")}
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($3, $5), 0)
				name := []*ast.String{ast.NewString("~~")}
				$$ = ast.NewA_Expr(ast.AEXPR_LIKE, name, $1, escapeFunc, 0)
			}
		|	a_expr NOT_LA LIKE a_expr %prec NOT_LA
			{
				name := []*ast.String{ast.NewString("!~~")}
				$$ = ast.NewA_Expr(ast.AEXPR_LIKE, name, $1, $4, 0)
			}
		|	a_expr NOT_LA LIKE a_expr ESCAPE a_expr %prec NOT_LA
			{
				// Create like_escape function call
				funcName := []*ast.String{ast.NewString("pg_catalog"), ast.NewString("like_escape")}
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($4, $6), 0)
				name := []*ast.String{ast.NewString("!~~")}
				$$ = ast.NewA_Expr(ast.AEXPR_LIKE, name, $1, escapeFunc, 0)
			}
		|	a_expr ILIKE a_expr
			{
				name := []*ast.String{ast.NewString("~~*")}
				$$ = ast.NewA_Expr(ast.AEXPR_ILIKE, name, $1, $3, 0)
			}
		|	a_expr ILIKE a_expr ESCAPE a_expr %prec ILIKE
			{
				// Create like_escape function call
				funcName := []*ast.String{ast.NewString("pg_catalog"), ast.NewString("like_escape")}
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($3, $5), 0)
				name := []*ast.String{ast.NewString("~~*")}
				$$ = ast.NewA_Expr(ast.AEXPR_ILIKE, name, $1, escapeFunc, 0)
			}
		|	a_expr NOT_LA ILIKE a_expr %prec NOT_LA
			{
				name := []*ast.String{ast.NewString("!~~*")}
				$$ = ast.NewA_Expr(ast.AEXPR_ILIKE, name, $1, $4, 0)
			}
		|	a_expr NOT_LA ILIKE a_expr ESCAPE a_expr %prec NOT_LA
			{
				// Create like_escape function call
				funcName := []*ast.String{ast.NewString("pg_catalog"), ast.NewString("like_escape")}
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($4, $6), 0)
				name := []*ast.String{ast.NewString("!~~*")}
				$$ = ast.NewA_Expr(ast.AEXPR_ILIKE, name, $1, escapeFunc, 0)
			}
		|	a_expr COLLATE any_name
			{
				// Convert any_name (which returns a NodeList of strings) to []string
				nodeList := $3.(*ast.NodeList)
				names := make([]string, len(nodeList.Items))
				for i, node := range nodeList.Items {
					names[i] = node.(*ast.String).SVal
				}
				collateClause := ast.NewCollateClause(names)
				collateClause.Arg = $1
				$$ = collateClause
			}
		|	a_expr AT TIME ZONE a_expr %prec AT
			{
				// Create timezone function call
				funcName := []*ast.String{ast.NewString("pg_catalog"), ast.NewString("timezone")}
				$$ = ast.NewFuncCall(funcName, ast.NewNodeList($5, $1), 0)
			}
		|	a_expr AT LOCAL %prec AT
			{
				// Create timezone function call with no argument
				funcName := []*ast.String{ast.NewString("pg_catalog"), ast.NewString("timezone")}
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
				name := []*ast.String{ast.NewString("BETWEEN")}
				$$ = ast.NewA_Expr(ast.AEXPR_BETWEEN, name, $1, ast.NewNodeList($4, $6), 0)
			}
		|	a_expr NOT_LA BETWEEN opt_asymmetric b_expr AND a_expr %prec NOT_LA
			{
				name := []*ast.String{ast.NewString("NOT BETWEEN")}
				$$ = ast.NewA_Expr(ast.AEXPR_NOT_BETWEEN, name, $1, ast.NewNodeList($5, $7), 0)
			}
		|	a_expr BETWEEN SYMMETRIC b_expr AND a_expr %prec BETWEEN
			{
				name := []*ast.String{ast.NewString("BETWEEN SYMMETRIC")}
				$$ = ast.NewA_Expr(ast.AEXPR_BETWEEN_SYM, name, $1, ast.NewNodeList($4, $6), 0)
			}
		|	a_expr NOT_LA BETWEEN SYMMETRIC b_expr AND a_expr %prec NOT_LA
			{
				name := []*ast.String{ast.NewString("NOT BETWEEN SYMMETRIC")}
				$$ = ast.NewA_Expr(ast.AEXPR_NOT_BETWEEN_SYM, name, $1, ast.NewNodeList($5, $7), 0)
			}
		|	a_expr IN_P in_expr
			{
				name := []*ast.String{ast.NewString("=")}
				$$ = ast.NewA_Expr(ast.AEXPR_IN, name, $1, $3, 0)
			}
		|	a_expr NOT_LA IN_P in_expr %prec NOT_LA
			{
				name := []*ast.String{ast.NewString("<>")}
				$$ = ast.NewA_Expr(ast.AEXPR_IN, name, $1, $4, 0)
			}
		|	a_expr SIMILAR TO a_expr %prec SIMILAR
			{
				name := []*ast.String{ast.NewString("~")}
				$$ = ast.NewA_Expr(ast.AEXPR_SIMILAR, name, $1, $4, 0)
			}
		|	a_expr SIMILAR TO a_expr ESCAPE a_expr %prec SIMILAR
			{
				// Create similar_escape function call
				funcName := []*ast.String{ast.NewString("pg_catalog"), ast.NewString("similar_escape")}
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($4, $6), 0)
				name := []*ast.String{ast.NewString("~")}
				$$ = ast.NewA_Expr(ast.AEXPR_SIMILAR, name, $1, escapeFunc, 0)
			}
		|	a_expr NOT_LA SIMILAR TO a_expr %prec NOT_LA
			{
				name := []*ast.String{ast.NewString("!~")}
				$$ = ast.NewA_Expr(ast.AEXPR_SIMILAR, name, $1, $5, 0)
			}
		|	a_expr NOT_LA SIMILAR TO a_expr ESCAPE a_expr %prec NOT_LA
			{
				// Create similar_escape function call
				funcName := []*ast.String{ast.NewString("pg_catalog"), ast.NewString("similar_escape")}
				escapeFunc := ast.NewFuncCall(funcName, ast.NewNodeList($5, $7), 0)
				name := []*ast.String{ast.NewString("!~")}
				$$ = ast.NewA_Expr(ast.AEXPR_SIMILAR, name, $1, escapeFunc, 0)
			}
		;

b_expr:		c_expr								{ $$ = $1 }
		|	b_expr TYPECAST Typename
			{
				$$ = ast.NewTypeCast($1, $3.(*ast.TypeName), 0)
			}
		|	'+' b_expr %prec UMINUS
			{
				name := []*ast.String{ast.NewString("+")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, nil, $2, 0)
			}
		|	'-' b_expr %prec UMINUS
			{
				name := []*ast.String{ast.NewString("-")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, nil, $2, 0)
			}
		|	b_expr '+' b_expr
			{
				name := []*ast.String{ast.NewString("+")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '-' b_expr
			{
				name := []*ast.String{ast.NewString("-")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '*' b_expr
			{
				name := []*ast.String{ast.NewString("*")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '/' b_expr
			{
				name := []*ast.String{ast.NewString("/")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '%' b_expr
			{
				name := []*ast.String{ast.NewString("%")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '^' b_expr
			{
				name := []*ast.String{ast.NewString("^")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '<' b_expr
			{
				name := []*ast.String{ast.NewString("<")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '>' b_expr
			{
				name := []*ast.String{ast.NewString(">")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr '=' b_expr
			{
				name := []*ast.String{ast.NewString("=")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr LESS_EQUALS b_expr
			{
				name := []*ast.String{ast.NewString("<=")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr GREATER_EQUALS b_expr
			{
				name := []*ast.String{ast.NewString(">=")}
				$$ = ast.NewA_Expr(ast.AEXPR_OP, name, $1, $3, 0)
			}
		|	b_expr NOT_EQUALS b_expr
			{
				name := []*ast.String{ast.NewString("<>")}
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
		|	func_expr							{ $$ = $1 }
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

indirection_el: '.' ColId
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

func_name:	type_function_name
			{
				$$ = []*ast.String{ast.NewString($1)}
			}
		|	ColId indirection
			{
				// Create function name from ColId + indirection
				result := []*ast.String{ast.NewString($1)}
				for _, node := range $2.Items {
					if str, ok := node.(*ast.String); ok {
						result = append(result, str)
					}
				}
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

/* Expression lists */
expr_list:	a_expr
			{
				$$ = ast.NewNodeList($1)
			}
		|	expr_list ',' a_expr
			{
				nodeList := $1.(*ast.NodeList)
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

type_function_name: ColId					{ $$ = $1 }
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
				$$ = $2.(*ast.NodeList)
			}
		|	/* EMPTY */
			{
				$$ = nil
			}
		;

GenericType: type_function_name opt_type_modifiers
			{
				typeName := ast.NewTypeName([]string{$1})
				// Note: In full implementation, would set type modifiers
				$$ = typeName
			}
		|	type_function_name attrs opt_type_modifiers
			{
				// Create qualified type name from name + attrs
				names := []string{$1}
				for _, attr := range $2.Items {
					names = append(names, attr.(*ast.String).SVal)
				}
				typeName := ast.NewTypeName(names)
				// Note: In full implementation, would set type modifiers
				$$ = typeName
			}
		;

Numeric:	INT_P
			{
				$$ = ast.NewTypeName([]string{"int4"})
			}
		|	INTEGER
			{
				$$ = ast.NewTypeName([]string{"int4"})
			}
		|	SMALLINT
			{
				$$ = ast.NewTypeName([]string{"int2"})
			}
		|	BIGINT
			{
				$$ = ast.NewTypeName([]string{"int8"})
			}
		|	REAL
			{
				$$ = ast.NewTypeName([]string{"float4"})
			}
		|	FLOAT_P opt_float
			{
				$$ = $2
			}
		|	DOUBLE_P PRECISION
			{
				$$ = ast.NewTypeName([]string{"float8"})
			}
		|	DECIMAL_P opt_type_modifiers
			{
				$$ = ast.NewTypeName([]string{"numeric"})
			}
		|	DEC opt_type_modifiers
			{
				$$ = ast.NewTypeName([]string{"numeric"})
			}
		|	NUMERIC opt_type_modifiers
			{
				$$ = ast.NewTypeName([]string{"numeric"})
			}
		|	BOOLEAN_P
			{
				$$ = ast.NewTypeName([]string{"bool"})
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
				$$ = ast.NewTypeName([]string{$1})
			}
		;

CharacterWithoutLength: character
			{
				typeName := ast.NewTypeName([]string{$1})
				// char defaults to char(1), varchar to no limit
				if $1 == "bpchar" {
					// Note: In full implementation, would set typmods to list with 1
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
				$$ = ast.NewTypeName([]string{typeName})
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
				$$ = ast.NewTypeName([]string{typeName})
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
				$$ = ast.NewTypeName([]string{typeName})
			}
		|	TIMESTAMP opt_timezone
			{
				var typeName string
				if $2 != 0 {
					typeName = "timestamptz"
				} else {
					typeName = "timestamp"
				}
				$$ = ast.NewTypeName([]string{typeName})
			}
		|	TIME '(' Iconst ')' opt_timezone
			{
				var typeName string
				if $5 != 0 {
					typeName = "timetz"
				} else {
					typeName = "time"
				}
				$$ = ast.NewTypeName([]string{typeName})
			}
		|	TIME opt_timezone
			{
				var typeName string
				if $2 != 0 {
					typeName = "timetz"
				} else {
					typeName = "time"
				}
				$$ = ast.NewTypeName([]string{typeName})
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
					$$ = ast.NewTypeName([]string{"float4"})
				} else if $2 <= 7 {
					$$ = ast.NewTypeName([]string{"float4"})
				} else {
					$$ = ast.NewTypeName([]string{"float8"})
				}
			}
		|	/* EMPTY */
			{
				$$ = ast.NewTypeName([]string{"float8"})
			}
		;

/* Operators - matching PostgreSQL structure */
qual_Op:	Op
			{
				$$ = []*ast.String{ast.NewString($1)}
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
				$$ = []*ast.String{ast.NewString($1)}
			}
		|	ColId '.' any_operator
			{
				$$ = append([]*ast.String{ast.NewString($1)}, $3...)
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

in_expr:		'(' expr_list ')'
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
				selectStmt.WithClause = $1.(*ast.WithClause)
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
				selectStmt.TargetList = convertToResTargetList($3.Items)
				selectStmt.IntoClause = convertToIntoClause($4)
				selectStmt.FromClause = $5
				selectStmt.WhereClause = $6
				$$ = selectStmt
			}
		|	SELECT opt_distinct_clause opt_target_list
			into_clause from_clause where_clause
			{
				selectStmt := ast.NewSelectStmt()
				selectStmt.DistinctClause = $2
				selectStmt.TargetList = convertToResTargetList($3.Items)
				selectStmt.IntoClause = convertToIntoClause($4)
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
				selectStmt.TargetList = []*ast.ResTarget{starTarget}
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
		|	a_expr ColLabel
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
				rangeVar := $1.(*ast.RangeVar)
				if $2 != nil {
					rangeVar.Alias = $2.(*ast.Alias)
				}
				$$ = rangeVar
			}
		|	select_with_parens opt_alias_clause
			{
				/* Subquery in FROM clause */
				subquery := $1.(*ast.SelectStmt)
				var alias *ast.Alias
				if $2 != nil {
					alias = $2.(*ast.Alias)
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
					alias = $3.(*ast.Alias)
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
				joinExpr.Alias = $4.(*ast.Alias)
				$$ = joinExpr
			}
		;

relation_expr:
			qualified_name
			{
				rangeVar := $1.(*ast.RangeVar)
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
				rangeVar := $1.(*ast.RangeVar)
				rangeVar.Inh = true // inheritance query, explicitly
				$$ = rangeVar
			}
		|	ONLY qualified_name
			{
				rangeVar := $2.(*ast.RangeVar)
				rangeVar.Inh = false // no inheritance
				$$ = rangeVar
			}
		|	ONLY '(' qualified_name ')'
			{
				rangeVar := $3.(*ast.RangeVar)
				rangeVar.Inh = false // no inheritance
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

using_clause:
			USING '(' name_list ')'
			{
				$$ = $3
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
					cte.Aliascolnames = $2.(*ast.NodeList)
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
				searchColList := $5.(*ast.NodeList)
				seqColumn := $7
				$$ = ast.NewCTESearchClause(searchColList, false, seqColumn)
			}
		|	SEARCH BREADTH FIRST_P BY columnList SET ColId
			{
				searchColList := $5.(*ast.NodeList)
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
				cycleColList := $2.(*ast.NodeList)
				markColumn := $4
				markValue := $6.(ast.Expression)
				markDefault := $8.(ast.Expression)
				pathColumn := $10
				$$ = ast.NewCTECycleClause(cycleColList, markColumn, markValue, markDefault, pathColumn)
			}
		|	CYCLE columnList SET ColId USING ColId
			{
				cycleColList := $2.(*ast.NodeList)
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
				exprList := $3.(*ast.NodeList)
				selectStmt.ValuesLists = []*ast.NodeList{exprList}
				$$ = selectStmt
			}
		|	values_clause ',' '(' expr_list ')'
			{
				/* Add additional VALUES row to existing SelectStmt */
				selectStmt := $1.(*ast.SelectStmt)
				exprList := $4.(*ast.NodeList)
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
				nameList := $4.(*ast.NodeList)
				$$ = ast.NewAlias($2, nameList)
			}
		|	AS ColId
			{
				$$ = ast.NewAlias($2, nil)
			}
		|	ColId '(' name_list ')'
			{
				nameList := $3.(*ast.NodeList)
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
		|	DISTINCT ON '(' expr_list ')'			{ $$ = $4.(*ast.NodeList) }
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
				$$ = ast.NewIntoClause($2.(*ast.RangeVar), nil, "", nil, ast.ONCOMMIT_NOOP, "", nil, false, 0)
			}
		|	/* EMPTY */								{ $$ = nil }
		;

%%

// Helper functions for converting between grammar types and AST types

// convertToResTargetList converts a slice of ast.Node to []*ast.ResTarget
func convertToResTargetList(nodes []ast.Node) []*ast.ResTarget {
	if nodes == nil {
		return nil
	}
	targets := make([]*ast.ResTarget, len(nodes))
	for i, node := range nodes {
		if target, ok := node.(*ast.ResTarget); ok {
			targets[i] = target
		}
	}
	return targets
}

// convertToIntoClause converts an ast.Node to *ast.IntoClause
func convertToIntoClause(node ast.Node) *ast.IntoClause {
	if node == nil {
		return nil
	}
	if intoClause, ok := node.(*ast.IntoClause); ok {
		return intoClause
	}
	return nil
}

// convertToStringList converts an ast.Node to []*ast.String
func convertToStringList(node ast.Node) []*ast.String {
	if node == nil {
		return nil
	}
	if nodeList, ok := node.(*ast.NodeList); ok {
		strings := make([]*ast.String, len(nodeList.Items))
		for i, item := range nodeList.Items {
			if str, ok := item.(*ast.String); ok {
				strings[i] = str
			}
		}
		return strings
	}
	return nil
}

// convertToNodeList converts an ast.Node to []ast.Node
func convertToNodeList(node ast.Node) []ast.Node {
	if node == nil {
		return nil
	}
	if nodeList, ok := node.(*ast.NodeList); ok {
		return nodeList.Items
	}
	// If it's a single node, wrap it in a slice
	return []ast.Node{node}
}


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
