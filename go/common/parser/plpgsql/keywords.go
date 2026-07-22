// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plpgsql

// PL/pgSQL keyword tables, ported verbatim from
// postgres/src/pl/plpgsql/src/pl_reserved_kwlist.h and pl_unreserved_kwlist.h.
//
// Reserved keywords are recognized before any identifier could be a variable
// name; unreserved keywords are only recognized after variable lookup fails
// (and a single identifier was seen, not a compound one). The lexer consults
// these maps by the lowercased identifier text. Keys must stay lowercase and
// no word may appear in both maps — keywordsAreDisjoint (see the test) guards
// that invariant.
//
// Note: PG hands the reserved list to the core scanner so those words never
// reach plpgsql_yylex as identifiers. Our SQL lexer does not know PL/pgSQL
// keywords, so the lexer reclassifies against BOTH maps itself.

// reservedKeywords maps a lowercase reserved keyword to its token code.
var reservedKeywords = map[string]int{
	"all":     K_ALL,
	"begin":   K_BEGIN,
	"by":      K_BY,
	"case":    K_CASE,
	"declare": K_DECLARE,
	"else":    K_ELSE,
	"end":     K_END,
	"execute": K_EXECUTE,
	"for":     K_FOR,
	"foreach": K_FOREACH,
	"from":    K_FROM,
	"if":      K_IF,
	"in":      K_IN,
	"into":    K_INTO,
	"loop":    K_LOOP,
	"not":     K_NOT,
	"null":    K_NULL,
	"or":      K_OR,
	"strict":  K_STRICT,
	"then":    K_THEN,
	"to":      K_TO,
	"using":   K_USING,
	"when":    K_WHEN,
	"while":   K_WHILE,
}

// unreservedKeywords maps a lowercase unreserved keyword to its token code.
// "elseif" and "elsif" are two spellings of the same K_ELSIF token, matching PG.
var unreservedKeywords = map[string]int{
	"absolute":             K_ABSOLUTE,
	"alias":                K_ALIAS,
	"and":                  K_AND,
	"array":                K_ARRAY,
	"assert":               K_ASSERT,
	"backward":             K_BACKWARD,
	"call":                 K_CALL,
	"chain":                K_CHAIN,
	"close":                K_CLOSE,
	"collate":              K_COLLATE,
	"column":               K_COLUMN,
	"column_name":          K_COLUMN_NAME,
	"commit":               K_COMMIT,
	"constant":             K_CONSTANT,
	"constraint":           K_CONSTRAINT,
	"constraint_name":      K_CONSTRAINT_NAME,
	"continue":             K_CONTINUE,
	"current":              K_CURRENT,
	"cursor":               K_CURSOR,
	"datatype":             K_DATATYPE,
	"debug":                K_DEBUG,
	"default":              K_DEFAULT,
	"detail":               K_DETAIL,
	"diagnostics":          K_DIAGNOSTICS,
	"do":                   K_DO,
	"dump":                 K_DUMP,
	"elseif":               K_ELSIF,
	"elsif":                K_ELSIF,
	"errcode":              K_ERRCODE,
	"error":                K_ERROR,
	"exception":            K_EXCEPTION,
	"exit":                 K_EXIT,
	"fetch":                K_FETCH,
	"first":                K_FIRST,
	"forward":              K_FORWARD,
	"get":                  K_GET,
	"hint":                 K_HINT,
	"import":               K_IMPORT,
	"info":                 K_INFO,
	"insert":               K_INSERT,
	"is":                   K_IS,
	"last":                 K_LAST,
	"log":                  K_LOG,
	"merge":                K_MERGE,
	"message":              K_MESSAGE,
	"message_text":         K_MESSAGE_TEXT,
	"move":                 K_MOVE,
	"next":                 K_NEXT,
	"no":                   K_NO,
	"notice":               K_NOTICE,
	"open":                 K_OPEN,
	"option":               K_OPTION,
	"perform":              K_PERFORM,
	"pg_context":           K_PG_CONTEXT,
	"pg_datatype_name":     K_PG_DATATYPE_NAME,
	"pg_exception_context": K_PG_EXCEPTION_CONTEXT,
	"pg_exception_detail":  K_PG_EXCEPTION_DETAIL,
	"pg_exception_hint":    K_PG_EXCEPTION_HINT,
	"pg_routine_oid":       K_PG_ROUTINE_OID,
	"print_strict_params":  K_PRINT_STRICT_PARAMS,
	"prior":                K_PRIOR,
	"query":                K_QUERY,
	"raise":                K_RAISE,
	"relative":             K_RELATIVE,
	"return":               K_RETURN,
	"returned_sqlstate":    K_RETURNED_SQLSTATE,
	"reverse":              K_REVERSE,
	"rollback":             K_ROLLBACK,
	"row_count":            K_ROW_COUNT,
	"rowtype":              K_ROWTYPE,
	"schema":               K_SCHEMA,
	"schema_name":          K_SCHEMA_NAME,
	"scroll":               K_SCROLL,
	"slice":                K_SLICE,
	"sqlstate":             K_SQLSTATE,
	"stacked":              K_STACKED,
	"table":                K_TABLE,
	"table_name":           K_TABLE_NAME,
	"type":                 K_TYPE,
	"use_column":           K_USE_COLUMN,
	"use_variable":         K_USE_VARIABLE,
	"variable_conflict":    K_VARIABLE_CONFLICT,
	"warning":              K_WARNING,
}
