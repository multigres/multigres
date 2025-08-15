/*
 * PostgreSQL Grammar File - Phase 3A Implementation
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
	ival    int
	str     string
	keyword string
	
	// AST nodes
	node      ast.Node
	stmt      ast.Stmt
	stmtList  []ast.Stmt
	list      []ast.Node
	strList   []string
	
	// Location tracking
	location  int
}

/*
 * Token declarations from PostgreSQL
 * Ported from postgres/src/backend/parser/gram.y:692-695
 */
%token <str>	IDENT UIDENT FCONST SCONST USCONST BCONST XCONST Op
%token <ival>	ICONST PARAM
%token			TYPECAST DOT_DOT COLON_EQUALS EQUALS_GREATER
%token			LESS_EQUALS GREATER_EQUALS NOT_EQUALS

/*
 * Reserved keywords (Phase 3A subset - will expand in later phases)
 * These are the minimum needed for foundation rules
 */
%token <keyword> ALL ALTER AS CASCADE CONCURRENTLY CREATE DROP IF_P EXISTS
%token <keyword> NOT NULLS_P OR REPLACE RESTRICT WITH

/* 
 * Special lookahead tokens
 * From postgres/src/backend/parser/gram.y:860-870
 */
%token		FORMAT_LA NOT_LA NULLS_LA WITH_LA WITHOUT_LA
%token		MODE_TYPE_NAME
%token		MODE_PLPGSQL_EXPR
%token		MODE_PLPGSQL_ASSIGN1
%token		MODE_PLPGSQL_ASSIGN2
%token		MODE_PLPGSQL_ASSIGN3

/*
 * Precedence declarations from PostgreSQL
 * From postgres/src/backend/parser/gram.y:874-924
 */
%left		UNION EXCEPT
%left		INTERSECT
%left		OR
%left		AND
%right		NOT
%nonassoc	IS ISNULL NOTNULL
%nonassoc	'<' '>' '=' LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%nonassoc	BETWEEN IN_P LIKE ILIKE SIMILAR NOT_LA
%nonassoc	ESCAPE

/*
 * Non-terminal type declarations for Phase 3A
 */
%type <stmtList>	parse_toplevel stmtmulti
%type <stmt>		toplevel_stmt stmt
%type <str>			ColId ColLabel name
%type <node>		name_list
%type <node>		qualified_name any_name
%type <node>		qualified_name_list any_name_list
%type <str>			opt_single_name
%type <str>			unreserved_keyword col_name_keyword type_func_name_keyword reserved_keyword
%type <node>		opt_qualified_name
%type <ival>		opt_drop_behavior
%type <ival>		opt_if_exists opt_if_not_exists
%type <ival>		opt_or_replace
%type <ival>		opt_concurrently
%type <node>		opt_with OptWith

/* Start symbol */
%start parse_toplevel

%%

/*
 * Phase 3A: Grammar Foundation & Infrastructure Rules
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
 * This is highly simplified for Phase 3A - will expand significantly
 * From postgres/src/backend/parser/gram.y:1075 onwards
 */
stmt:
			/* Empty for now - will add statement types in Phase 3B+ */
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
 * From postgres/src/backend/parser/gram.y:18184-18305
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

qualified_name:
			ColId
			{
				$$ = &ast.RangeVar{
					RelName: $1,
				}
			}
		|	ColId '.' ColId
			{
				$$ = &ast.RangeVar{
					SchemaName: $1,
					RelName:    $3,
				}
			}
		/* TODO Phase 3B: Add indirection support for 3+ part names:
		|	ColId indirection
			{
				// $$ = makeRangeVarFromQualifiedName($1, $2, @1, yyscanner)
			}
		*/
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
		/* TODO Phase 3B: Add attrs support for 3+ part names:
		|	ColId attrs
			{
				// $$ = lcons(makeString($1), $2)
			}
		*/
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
 * Keyword categories - simplified for Phase 3A
 * Will expand significantly in later phases
 */

unreserved_keyword:
			/* Will add unreserved keywords as needed */
			ALL										{ $$ = "all" }
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