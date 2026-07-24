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

// PostgreSQL keyword classification. This is the single source of truth for
// which category each keyword belongs to; it lives in the ast package (rather
// than the parser package, next to the goyacc token table) so that deparsers
// here can consult it without importing the parser. The parser package keeps the
// keyword-name -> token mapping the lexer needs and copies these categories in
// at init. Data is reconciled against postgres/src/include/parser/kwlist.h.

package ast

// KeywordCategory is PostgreSQL's grammar keyword-category classification. Every
// keyword belongs to exactly one category (postgres/src/backend/parser/gram.y).
type KeywordCategory int

const (
	// UnreservedKeyword — available for use as any kind of name: column, table,
	// type, or function. (gram.y: "available for use as any kind of name".)
	UnreservedKeyword KeywordCategory = iota

	// ColNameKeyword — usable as a column or table name. Many are ALSO recognized
	// as type or function names, but only through dedicated grammar productions, so
	// they are NOT usable as a *generic* type or function name (gram.y: "they have
	// special productions for the purpose, and so can't be treated as 'generic'
	// type or function names"). This is precisely why one must be double-quoted to
	// be emitted as an ordinary function name — bare, the grammar grabs it for its
	// special production instead (e.g. NORMALIZE(...)).
	ColNameKeyword

	// TypeFuncNameKeyword — usable as a type or function name, but NOT as a column
	// name: these are typically operator-like keywords that would be ambiguous with
	// variables in column position (gram.y: "can't be column names because they
	// would be ambiguous with variables, but ... unambiguous as function
	// identifiers").
	TypeFuncNameKeyword

	// ReservedKeyword — usable only as a column label (and only with AS, unless
	// also bare-label-able). Not usable as a column, table, type, or function name
	// (gram.y: "usable only as a ColLabel").
	ReservedKeyword
)

// String returns the PostgreSQL name of the category.
func (kc KeywordCategory) String() string {
	switch kc {
	case UnreservedKeyword:
		return "UNRESERVED_KEYWORD"
	case ColNameKeyword:
		return "COL_NAME_KEYWORD"
	case TypeFuncNameKeyword:
		return "TYPE_FUNC_NAME_KEYWORD"
	case ReservedKeyword:
		return "RESERVED_KEYWORD"
	default:
		return "UNKNOWN_KEYWORD_CATEGORY"
	}
}

// KeywordClass is the category classification of one keyword. It deliberately
// omits the parser token constant, which lives in the parser package.
type KeywordClass struct {
	Name         string          // keyword name (lower case)
	Category     KeywordCategory // grammar category
	CanBareLabel bool            // usable as a bare column label (without AS)
}

// keywordClasses is the full PostgreSQL keyword list with categories and
// bare-label flags, ported from postgres/src/include/parser/kwlist.h.
var keywordClasses = []KeywordClass{
	{"abort", UnreservedKeyword, true},
	{"absent", UnreservedKeyword, true},
	{"absolute", UnreservedKeyword, true},
	{"access", UnreservedKeyword, true},
	{"action", UnreservedKeyword, true},
	{"add", UnreservedKeyword, true},
	{"admin", UnreservedKeyword, true},
	{"after", UnreservedKeyword, true},
	{"aggregate", UnreservedKeyword, true},
	{"all", ReservedKeyword, true},
	{"also", UnreservedKeyword, true},
	{"alter", UnreservedKeyword, true},
	{"always", UnreservedKeyword, true},
	{"analyse", ReservedKeyword, true},
	{"analyze", ReservedKeyword, true},
	{"and", ReservedKeyword, true},
	{"any", ReservedKeyword, true},
	{"array", ReservedKeyword, false},
	{"as", ReservedKeyword, false},
	{"asc", ReservedKeyword, true},
	{"asensitive", UnreservedKeyword, true},
	{"assertion", UnreservedKeyword, true},
	{"assignment", UnreservedKeyword, true},
	{"asymmetric", ReservedKeyword, true},
	{"at", UnreservedKeyword, true},
	{"atomic", UnreservedKeyword, true},
	{"attach", UnreservedKeyword, true},
	{"attribute", UnreservedKeyword, true},
	{"authorization", TypeFuncNameKeyword, true},
	{"backward", UnreservedKeyword, true},
	{"before", UnreservedKeyword, true},
	{"begin", UnreservedKeyword, true},
	{"between", ColNameKeyword, true},
	{"bigint", ColNameKeyword, true},
	{"binary", TypeFuncNameKeyword, true},
	{"bit", ColNameKeyword, true},
	{"boolean", ColNameKeyword, true},
	{"both", ReservedKeyword, true},
	{"breadth", UnreservedKeyword, true},
	{"by", UnreservedKeyword, true},
	{"cache", UnreservedKeyword, true},
	{"call", UnreservedKeyword, true},
	{"called", UnreservedKeyword, true},
	{"cascade", UnreservedKeyword, true},
	{"cascaded", UnreservedKeyword, true},
	{"case", ReservedKeyword, true},
	{"cast", ReservedKeyword, true},
	{"catalog", UnreservedKeyword, true},
	{"chain", UnreservedKeyword, true},
	{"char", ColNameKeyword, false},
	{"character", ColNameKeyword, false},
	{"characteristics", UnreservedKeyword, true},
	{"check", ReservedKeyword, true},
	{"checkpoint", UnreservedKeyword, true},
	{"class", UnreservedKeyword, true},
	{"close", UnreservedKeyword, true},
	{"cluster", UnreservedKeyword, true},
	{"coalesce", ColNameKeyword, true},
	{"collate", ReservedKeyword, true},
	{"collation", TypeFuncNameKeyword, true},
	{"column", ReservedKeyword, true},
	{"columns", UnreservedKeyword, true},
	{"comment", UnreservedKeyword, true},
	{"comments", UnreservedKeyword, true},
	{"commit", UnreservedKeyword, true},
	{"committed", UnreservedKeyword, true},
	{"compression", UnreservedKeyword, true},
	{"concurrently", TypeFuncNameKeyword, true},
	{"conditional", UnreservedKeyword, true},
	{"configuration", UnreservedKeyword, true},
	{"conflict", UnreservedKeyword, true},
	{"connection", UnreservedKeyword, true},
	{"constraint", ReservedKeyword, true},
	{"constraints", UnreservedKeyword, true},
	{"content", UnreservedKeyword, true},
	{"continue", UnreservedKeyword, true},
	{"conversion", UnreservedKeyword, true},
	{"copy", UnreservedKeyword, true},
	{"cost", UnreservedKeyword, true},
	{"create", ReservedKeyword, false},
	{"cross", TypeFuncNameKeyword, true},
	{"csv", UnreservedKeyword, true},
	{"cube", UnreservedKeyword, true},
	{"current", UnreservedKeyword, true},
	{"current_catalog", ReservedKeyword, true},
	{"current_date", ReservedKeyword, true},
	{"current_role", ReservedKeyword, true},
	{"current_schema", TypeFuncNameKeyword, true},
	{"current_time", ReservedKeyword, true},
	{"current_timestamp", ReservedKeyword, true},
	{"current_user", ReservedKeyword, true},
	{"cursor", UnreservedKeyword, true},
	{"cycle", UnreservedKeyword, true},
	{"data", UnreservedKeyword, true},
	{"database", UnreservedKeyword, true},
	{"day", UnreservedKeyword, false},
	{"deallocate", UnreservedKeyword, true},
	{"dec", ColNameKeyword, true},
	{"decimal", ColNameKeyword, true},
	{"declare", UnreservedKeyword, true},
	{"default", ReservedKeyword, true},
	{"defaults", UnreservedKeyword, true},
	{"deferrable", ReservedKeyword, true},
	{"deferred", UnreservedKeyword, true},
	{"definer", UnreservedKeyword, true},
	{"delete", UnreservedKeyword, true},
	{"delimiter", UnreservedKeyword, true},
	{"delimiters", UnreservedKeyword, true},
	{"depends", UnreservedKeyword, true},
	{"depth", UnreservedKeyword, true},
	{"desc", ReservedKeyword, true},
	{"detach", UnreservedKeyword, true},
	{"dictionary", UnreservedKeyword, true},
	{"disable", UnreservedKeyword, true},
	{"discard", UnreservedKeyword, true},
	{"distinct", ReservedKeyword, true},
	{"do", ReservedKeyword, true},
	{"document", UnreservedKeyword, true},
	{"domain", UnreservedKeyword, true},
	{"double", UnreservedKeyword, true},
	{"drop", UnreservedKeyword, true},
	{"each", UnreservedKeyword, true},
	{"else", ReservedKeyword, true},
	{"empty", UnreservedKeyword, true},
	{"enable", UnreservedKeyword, true},
	{"encoding", UnreservedKeyword, true},
	{"encrypted", UnreservedKeyword, true},
	{"end", ReservedKeyword, true},
	{"enum", UnreservedKeyword, true},
	{"error", UnreservedKeyword, true},
	{"escape", UnreservedKeyword, true},
	{"event", UnreservedKeyword, true},
	{"except", ReservedKeyword, false},
	{"exclude", UnreservedKeyword, true},
	{"excluding", UnreservedKeyword, true},
	{"exclusive", UnreservedKeyword, true},
	{"execute", UnreservedKeyword, true},
	{"exists", ColNameKeyword, true},
	{"explain", UnreservedKeyword, true},
	{"expression", UnreservedKeyword, true},
	{"extension", UnreservedKeyword, true},
	{"external", UnreservedKeyword, true},
	{"extract", ColNameKeyword, true},
	{"false", ReservedKeyword, true},
	{"family", UnreservedKeyword, true},
	{"fetch", ReservedKeyword, false},
	{"filter", UnreservedKeyword, false},
	{"finalize", UnreservedKeyword, true},
	{"first", UnreservedKeyword, true},
	{"float", ColNameKeyword, true},
	{"following", UnreservedKeyword, true},
	{"for", ReservedKeyword, false},
	{"force", UnreservedKeyword, true},
	{"foreign", ReservedKeyword, true},
	{"format", UnreservedKeyword, true},
	{"forward", UnreservedKeyword, true},
	{"freeze", TypeFuncNameKeyword, true},
	{"from", ReservedKeyword, false},
	{"full", TypeFuncNameKeyword, true},
	{"function", UnreservedKeyword, true},
	{"functions", UnreservedKeyword, true},
	{"generated", UnreservedKeyword, true},
	{"global", UnreservedKeyword, true},
	{"grant", ReservedKeyword, false},
	{"granted", UnreservedKeyword, true},
	{"greatest", ColNameKeyword, true},
	{"group", ReservedKeyword, false},
	{"grouping", ColNameKeyword, true},
	{"groups", UnreservedKeyword, true},
	{"handler", UnreservedKeyword, true},
	{"having", ReservedKeyword, false},
	{"header", UnreservedKeyword, true},
	{"hold", UnreservedKeyword, true},
	{"hour", UnreservedKeyword, false},
	{"identity", UnreservedKeyword, true},
	{"if", UnreservedKeyword, true},
	{"ilike", TypeFuncNameKeyword, true},
	{"immediate", UnreservedKeyword, true},
	{"immutable", UnreservedKeyword, true},
	{"implicit", UnreservedKeyword, true},
	{"import", UnreservedKeyword, true},
	{"in", ReservedKeyword, true},
	{"include", UnreservedKeyword, true},
	{"including", UnreservedKeyword, true},
	{"increment", UnreservedKeyword, true},
	{"indent", UnreservedKeyword, true},
	{"index", UnreservedKeyword, true},
	{"indexes", UnreservedKeyword, true},
	{"inherit", UnreservedKeyword, true},
	{"inherits", UnreservedKeyword, true},
	{"initially", ReservedKeyword, true},
	{"inline", UnreservedKeyword, true},
	{"inner", TypeFuncNameKeyword, true},
	{"inout", ColNameKeyword, true},
	{"input", UnreservedKeyword, true},
	{"insensitive", UnreservedKeyword, true},
	{"insert", UnreservedKeyword, true},
	{"instead", UnreservedKeyword, true},
	{"int", ColNameKeyword, true},
	{"integer", ColNameKeyword, true},
	{"intersect", ReservedKeyword, false},
	{"interval", ColNameKeyword, true},
	{"into", ReservedKeyword, false},
	{"invoker", UnreservedKeyword, true},
	{"is", TypeFuncNameKeyword, true},
	{"isnull", TypeFuncNameKeyword, false},
	{"isolation", UnreservedKeyword, true},
	{"join", TypeFuncNameKeyword, true},
	{"json", ColNameKeyword, true},
	{"json_array", ColNameKeyword, true},
	{"json_arrayagg", ColNameKeyword, true},
	{"json_exists", ColNameKeyword, true},
	{"json_object", ColNameKeyword, true},
	{"json_objectagg", ColNameKeyword, true},
	{"json_query", ColNameKeyword, true},
	{"json_scalar", ColNameKeyword, true},
	{"json_serialize", ColNameKeyword, true},
	{"json_table", ColNameKeyword, true},
	{"json_value", ColNameKeyword, true},
	{"keep", UnreservedKeyword, true},
	{"key", UnreservedKeyword, true},
	{"keys", UnreservedKeyword, true},
	{"label", UnreservedKeyword, true},
	{"language", UnreservedKeyword, true},
	{"large", UnreservedKeyword, true},
	{"last", UnreservedKeyword, true},
	{"lateral", ReservedKeyword, true},
	{"leading", ReservedKeyword, true},
	{"leakproof", UnreservedKeyword, true},
	{"least", ColNameKeyword, true},
	{"left", TypeFuncNameKeyword, true},
	{"level", UnreservedKeyword, true},
	{"like", TypeFuncNameKeyword, true},
	{"limit", ReservedKeyword, false},
	{"listen", UnreservedKeyword, true},
	{"load", UnreservedKeyword, true},
	{"local", UnreservedKeyword, true},
	{"localtime", ReservedKeyword, true},
	{"localtimestamp", ReservedKeyword, true},
	{"location", UnreservedKeyword, true},
	{"lock", UnreservedKeyword, true},
	{"locked", UnreservedKeyword, true},
	{"logged", UnreservedKeyword, true},
	{"mapping", UnreservedKeyword, true},
	{"match", UnreservedKeyword, true},
	{"matched", UnreservedKeyword, true},
	{"materialized", UnreservedKeyword, true},
	{"maxvalue", UnreservedKeyword, true},
	{"merge", UnreservedKeyword, true},
	{"merge_action", ColNameKeyword, true},
	{"method", UnreservedKeyword, true},
	{"minute", UnreservedKeyword, false},
	{"minvalue", UnreservedKeyword, true},
	{"mode", UnreservedKeyword, true},
	{"month", UnreservedKeyword, false},
	{"move", UnreservedKeyword, true},
	{"name", UnreservedKeyword, true},
	{"names", UnreservedKeyword, true},
	{"national", ColNameKeyword, true},
	{"natural", TypeFuncNameKeyword, true},
	{"nchar", ColNameKeyword, true},
	{"nested", UnreservedKeyword, true},
	{"new", UnreservedKeyword, true},
	{"next", UnreservedKeyword, true},
	{"nfc", UnreservedKeyword, true},
	{"nfd", UnreservedKeyword, true},
	{"nfkc", UnreservedKeyword, true},
	{"nfkd", UnreservedKeyword, true},
	{"no", UnreservedKeyword, true},
	{"none", ColNameKeyword, true},
	{"normalize", ColNameKeyword, true},
	{"normalized", UnreservedKeyword, true},
	{"not", ReservedKeyword, true},
	{"nothing", UnreservedKeyword, true},
	{"notify", UnreservedKeyword, true},
	{"notnull", TypeFuncNameKeyword, false},
	{"nowait", UnreservedKeyword, true},
	{"null", ReservedKeyword, true},
	{"nullif", ColNameKeyword, true},
	{"nulls", UnreservedKeyword, true},
	{"numeric", ColNameKeyword, true},
	{"object", UnreservedKeyword, true},
	{"of", UnreservedKeyword, true},
	{"off", UnreservedKeyword, true},
	{"offset", ReservedKeyword, false},
	{"oids", UnreservedKeyword, true},
	{"old", UnreservedKeyword, true},
	{"omit", UnreservedKeyword, true},
	{"on", ReservedKeyword, false},
	{"only", ReservedKeyword, true},
	{"operator", UnreservedKeyword, true},
	{"option", UnreservedKeyword, true},
	{"options", UnreservedKeyword, true},
	{"or", ReservedKeyword, true},
	{"order", ReservedKeyword, false},
	{"ordinality", UnreservedKeyword, true},
	{"others", UnreservedKeyword, true},
	{"out", ColNameKeyword, true},
	{"outer", TypeFuncNameKeyword, true},
	{"over", UnreservedKeyword, false},
	{"overlaps", TypeFuncNameKeyword, false},
	{"overlay", ColNameKeyword, true},
	{"overriding", UnreservedKeyword, true},
	{"owned", UnreservedKeyword, true},
	{"owner", UnreservedKeyword, true},
	{"parallel", UnreservedKeyword, true},
	{"parameter", UnreservedKeyword, true},
	{"parser", UnreservedKeyword, true},
	{"partial", UnreservedKeyword, true},
	{"partition", UnreservedKeyword, true},
	{"passing", UnreservedKeyword, true},
	{"password", UnreservedKeyword, true},
	{"path", UnreservedKeyword, true},
	{"placing", ReservedKeyword, true},
	{"plan", UnreservedKeyword, true},
	{"plans", UnreservedKeyword, true},
	{"policy", UnreservedKeyword, true},
	{"position", ColNameKeyword, true},
	{"preceding", UnreservedKeyword, true},
	{"precision", ColNameKeyword, false},
	{"prepare", UnreservedKeyword, true},
	{"prepared", UnreservedKeyword, true},
	{"preserve", UnreservedKeyword, true},
	{"primary", ReservedKeyword, true},
	{"prior", UnreservedKeyword, true},
	{"privileges", UnreservedKeyword, true},
	{"procedural", UnreservedKeyword, true},
	{"procedure", UnreservedKeyword, true},
	{"procedures", UnreservedKeyword, true},
	{"program", UnreservedKeyword, true},
	{"publication", UnreservedKeyword, true},
	{"quote", UnreservedKeyword, true},
	{"quotes", UnreservedKeyword, true},
	{"range", UnreservedKeyword, true},
	{"read", UnreservedKeyword, true},
	{"real", ColNameKeyword, true},
	{"reassign", UnreservedKeyword, true},
	{"recheck", UnreservedKeyword, true},
	{"recursive", UnreservedKeyword, true},
	{"ref", UnreservedKeyword, true},
	{"references", ReservedKeyword, true},
	{"referencing", UnreservedKeyword, true},
	{"refresh", UnreservedKeyword, true},
	{"reindex", UnreservedKeyword, true},
	{"relative", UnreservedKeyword, true},
	{"release", UnreservedKeyword, true},
	{"rename", UnreservedKeyword, true},
	{"repeatable", UnreservedKeyword, true},
	{"replace", UnreservedKeyword, true},
	{"replica", UnreservedKeyword, true},
	{"reset", UnreservedKeyword, true},
	{"restart", UnreservedKeyword, true},
	{"restrict", UnreservedKeyword, true},
	{"return", UnreservedKeyword, true},
	{"returning", ReservedKeyword, false},
	{"returns", UnreservedKeyword, true},
	{"revoke", UnreservedKeyword, true},
	{"right", TypeFuncNameKeyword, true},
	{"role", UnreservedKeyword, true},
	{"rollback", UnreservedKeyword, true},
	{"rollup", UnreservedKeyword, true},
	{"routine", UnreservedKeyword, true},
	{"routines", UnreservedKeyword, true},
	{"row", ColNameKeyword, true},
	{"rows", UnreservedKeyword, true},
	{"rule", UnreservedKeyword, true},
	{"savepoint", UnreservedKeyword, true},
	{"scalar", UnreservedKeyword, true},
	{"schema", UnreservedKeyword, true},
	{"schemas", UnreservedKeyword, true},
	{"scroll", UnreservedKeyword, true},
	{"search", UnreservedKeyword, true},
	{"second", UnreservedKeyword, false},
	{"security", UnreservedKeyword, true},
	{"select", ReservedKeyword, true},
	{"sequence", UnreservedKeyword, true},
	{"sequences", UnreservedKeyword, true},
	{"serializable", UnreservedKeyword, true},
	{"server", UnreservedKeyword, true},
	{"session", UnreservedKeyword, true},
	{"session_user", ReservedKeyword, true},
	{"set", UnreservedKeyword, true},
	{"setof", ColNameKeyword, true},
	{"sets", UnreservedKeyword, true},
	{"share", UnreservedKeyword, true},
	{"show", UnreservedKeyword, true},
	{"similar", TypeFuncNameKeyword, true},
	{"simple", UnreservedKeyword, true},
	{"skip", UnreservedKeyword, true},
	{"smallint", ColNameKeyword, true},
	{"snapshot", UnreservedKeyword, true},
	{"some", ReservedKeyword, true},
	{"source", UnreservedKeyword, true},
	{"sql", UnreservedKeyword, true},
	{"stable", UnreservedKeyword, true},
	{"standalone", UnreservedKeyword, true},
	{"start", UnreservedKeyword, true},
	{"statement", UnreservedKeyword, true},
	{"statistics", UnreservedKeyword, true},
	{"stdin", UnreservedKeyword, true},
	{"stdout", UnreservedKeyword, true},
	{"storage", UnreservedKeyword, true},
	{"stored", UnreservedKeyword, true},
	{"strict", UnreservedKeyword, true},
	{"string", UnreservedKeyword, true},
	{"strip", UnreservedKeyword, true},
	{"subscription", UnreservedKeyword, true},
	{"substring", ColNameKeyword, true},
	{"support", UnreservedKeyword, true},
	{"symmetric", ReservedKeyword, true},
	{"sysid", UnreservedKeyword, true},
	{"system", UnreservedKeyword, true},
	{"system_user", ReservedKeyword, true},
	{"table", ReservedKeyword, true},
	{"tables", UnreservedKeyword, true},
	{"tablesample", TypeFuncNameKeyword, true},
	{"tablespace", UnreservedKeyword, true},
	{"target", UnreservedKeyword, true},
	{"temp", UnreservedKeyword, true},
	{"template", UnreservedKeyword, true},
	{"temporary", UnreservedKeyword, true},
	{"text", UnreservedKeyword, true},
	{"then", ReservedKeyword, true},
	{"ties", UnreservedKeyword, true},
	{"time", ColNameKeyword, true},
	{"timestamp", ColNameKeyword, true},
	{"to", ReservedKeyword, false},
	{"trailing", ReservedKeyword, true},
	{"transaction", UnreservedKeyword, true},
	{"transform", UnreservedKeyword, true},
	{"treat", ColNameKeyword, true},
	{"trigger", UnreservedKeyword, true},
	{"trim", ColNameKeyword, true},
	{"true", ReservedKeyword, true},
	{"truncate", UnreservedKeyword, true},
	{"trusted", UnreservedKeyword, true},
	{"type", UnreservedKeyword, true},
	{"types", UnreservedKeyword, true},
	{"uescape", UnreservedKeyword, true},
	{"unbounded", UnreservedKeyword, true},
	{"uncommitted", UnreservedKeyword, true},
	{"unconditional", UnreservedKeyword, true},
	{"unencrypted", UnreservedKeyword, true},
	{"union", ReservedKeyword, false},
	{"unique", ReservedKeyword, true},
	{"unknown", UnreservedKeyword, true},
	{"unlisten", UnreservedKeyword, true},
	{"unlogged", UnreservedKeyword, true},
	{"until", UnreservedKeyword, true},
	{"update", UnreservedKeyword, true},
	{"user", ReservedKeyword, true},
	{"using", ReservedKeyword, true},
	{"vacuum", UnreservedKeyword, true},
	{"valid", UnreservedKeyword, true},
	{"validate", UnreservedKeyword, true},
	{"validator", UnreservedKeyword, true},
	{"value", UnreservedKeyword, true},
	{"values", ColNameKeyword, true},
	{"varchar", ColNameKeyword, true},
	{"variadic", ReservedKeyword, true},
	{"varying", UnreservedKeyword, false},
	{"verbose", TypeFuncNameKeyword, true},
	{"version", UnreservedKeyword, true},
	{"view", UnreservedKeyword, true},
	{"views", UnreservedKeyword, true},
	{"volatile", UnreservedKeyword, true},
	{"when", ReservedKeyword, true},
	{"where", ReservedKeyword, false},
	{"whitespace", UnreservedKeyword, true},
	{"window", ReservedKeyword, false},
	{"with", ReservedKeyword, false},
	{"within", UnreservedKeyword, false},
	{"without", UnreservedKeyword, false},
	{"work", UnreservedKeyword, true},
	{"wrapper", UnreservedKeyword, true},
	{"write", UnreservedKeyword, true},
	{"xml", UnreservedKeyword, true},
	{"xmlattributes", ColNameKeyword, true},
	{"xmlconcat", ColNameKeyword, true},
	{"xmlelement", ColNameKeyword, true},
	{"xmlexists", ColNameKeyword, true},
	{"xmlforest", ColNameKeyword, true},
	{"xmlnamespaces", ColNameKeyword, true},
	{"xmlparse", ColNameKeyword, true},
	{"xmlpi", ColNameKeyword, true},
	{"xmlroot", ColNameKeyword, true},
	{"xmlserialize", ColNameKeyword, true},
	{"xmltable", ColNameKeyword, true},
	{"year", UnreservedKeyword, false},
	{"yes", UnreservedKeyword, true},
	{"zone", UnreservedKeyword, true},
}

var keywordClassByName = func() map[string]KeywordClass {
	m := make(map[string]KeywordClass, len(keywordClasses))
	for _, kw := range keywordClasses {
		m[kw.Name] = kw
	}
	return m
}()

// LookupKeywordClass returns the classification of name (which must be lower
// case) and whether it is a keyword.
func LookupKeywordClass(name string) (KeywordClass, bool) {
	kw, ok := keywordClassByName[name]
	return kw, ok
}

// LookupKeywordCategory returns the category of name (which must be lower case)
// and whether it is a keyword.
func LookupKeywordCategory(name string) (KeywordCategory, bool) {
	kw, ok := keywordClassByName[name]
	return kw.Category, ok
}

// IsKeyword reports whether name (lower case) is a SQL keyword.
func IsKeyword(name string) bool {
	_, ok := keywordClassByName[name]
	return ok
}

// IsReservedKeyword reports whether name (lower case) is a reserved keyword.
func IsReservedKeyword(name string) bool {
	kw, ok := keywordClassByName[name]
	return ok && kw.Category == ReservedKeyword
}

// KeywordNames returns every keyword name (unsorted). Used to guard the parser
// token table against drifting from this classification.
func KeywordNames() []string {
	names := make([]string, len(keywordClasses))
	for i, kw := range keywordClasses {
		names[i] = kw.Name
	}
	return names
}
