// Package keywords provides PostgreSQL SQL keyword recognition and categorization.
// Ported from postgres/src/common/keywords.c and postgres/src/include/parser/kwlist.h
package keywords

import (
	"sort"
	"strings"
)

// KeywordCategory represents the different categories of SQL keywords.
// Ported from postgres/src/include/parser/kwlist.h
type KeywordCategory int

const (
	// UNRESERVED_KEYWORD - can be used as column name, function name, etc.
	// Ported from postgres/src/include/parser/kwlist.h
	UnreservedKeyword KeywordCategory = iota
	
	// COL_NAME_KEYWORD - can be used as column name but not function name
	// Ported from postgres/src/include/parser/kwlist.h
	ColNameKeyword
	
	// TYPE_FUNC_NAME_KEYWORD - can be used as function name or type name
	// Ported from postgres/src/include/parser/kwlist.h
	TypeFuncNameKeyword
	
	// RESERVED_KEYWORD - fully reserved, cannot be used as identifier
	// Ported from postgres/src/include/parser/kwlist.h
	ReservedKeyword
)

// Token represents SQL token types.
// These correspond to the token definitions in postgres/src/backend/parser/gram.y
type Token int

// Token constants ported from postgres/src/backend/parser/gram.y (lines 705-812)
const (
	// Basic tokens
	IDENT Token = iota + 256 // Start after ASCII range
	UIDENT
	FCONST
	SCONST
	USCONST
	BCONST
	XCONST
	Op
	ICONST
	PARAM
	
	// Special operators
	TYPECAST
	DOT_DOT
	COLON_EQUALS
	EQUALS_GREATER
	LESS_EQUALS
	GREATER_EQUALS
	NOT_EQUALS
	
	// Keywords - ported from postgres/src/backend/parser/gram.y:705
	ABORT_P
	ABSENT
	ABSOLUTE_P
	ACCESS
	ACTION
	ADD_P
	ADMIN
	AFTER
	AGGREGATE
	ALL
	ALSO
	ALTER
	ALWAYS
	ANALYSE
	ANALYZE
	AND
	ANY
	ARRAY
	AS
	ASC
	ASENSITIVE
	ASSERTION
	ASSIGNMENT
	ASYMMETRIC
	AT
	ATOMIC
	ATTACH
	ATTRIBUTE
	AUTHORIZATION
	BACKWARD
	BEFORE
	BEGIN_P
	BETWEEN
	BIGINT
	BINARY
	BIT
	BOOLEAN_P
	BOTH
	BREADTH
	BY
	CACHE
	CALL
	CALLED
	CASCADE
	CASCADED
	CASE
	CAST
	CATALOG_P
	CHAIN
	CHAR_P
	CHARACTER
	CHARACTERISTICS
	CHECK
	CHECKPOINT
	CLASS
	CLOSE
	CLUSTER
	COALESCE
	COLLATE
	COLLATION
	COLUMN
	COLUMNS
	COMMENT
	COMMENTS
	COMMIT
	COMMITTED
	COMPRESSION
	CONCURRENTLY
	CONDITIONAL
	CONFIGURATION
	CONFLICT
	CONNECTION
	CONSTRAINT
	CONSTRAINTS
	CONTENT_P
	CONTINUE_P
	CONVERSION_P
	COPY
	COST
	CREATE
	CROSS
	CSV
	CUBE
	CURRENT_P
	CURRENT_CATALOG
	CURRENT_DATE
	CURRENT_ROLE
	CURRENT_SCHEMA
	CURRENT_TIME
	CURRENT_TIMESTAMP
	CURRENT_USER
	CURSOR
	CYCLE
	DATA_P
	DATABASE
	DAY_P
	DEALLOCATE
	DEC
	DECIMAL_P
	DECLARE
	DEFAULT
	DEFAULTS
	DEFERRABLE
	DEFERRED
	DEFINER
	DELETE_P
	DELIMITER
	DELIMITERS
	DEPENDS
	DEPTH
	DESC
	DETACH
	DICTIONARY
	DISABLE_P
	DISCARD
	DISTINCT
	DO
	DOCUMENT_P
	DOMAIN_P
	DOUBLE_P
	DROP
	EACH
	ELSE
	ENABLE_P
	ENCODING
	ENCRYPTED
	END_P
	ENUM_P
	ESCAPE
	EVENT
	EXCEPT
	EXCLUDE
	EXCLUDING
	EXCLUSIVE
	EXECUTE
	EXISTS
	EXPLAIN
	EXPRESSION
	EXTENSION
	EXTERNAL
	EXTRACT
	FALSE_P
	FAMILY
	FETCH
	FILTER
	FINALIZE
	FIRST_P
	FLOAT_P
	FOLLOWING
	FOR
	FORCE
	FOREIGN
	FORWARD
	FREEZE
	FROM
	FULL
	FUNCTION
	FUNCTIONS
	GENERATED
	GLOBAL
	GRANT
	GRANTED
	GREATEST
	GROUP_P
	GROUPING
	GROUPS
	HANDLER
	HAVING
	HEADER_P
	HOLD
	HOUR_P
	IDENTITY_P
	IF_P
	ILIKE
	IMMEDIATE
	IMMUTABLE
	IMPLICIT_P
	IMPORT_P
	IN_P
	INCLUDE_P
	INCLUDING
	INCREMENT
	INDEX
	INDEXES
	INHERIT
	INHERITS
	INITIALLY
	INLINE_P
	INNER_P
	INOUT
	INPUT_P
	INSENSITIVE
	INSERT
	INSTEAD
	INT_P
	INTEGER
	INTERSECT
	INTERVAL
	INTO
	INVOKER
	IS
	ISNULL
	ISOLATION
	JOIN
	JSON
	JSON_ARRAY
	JSON_ARRAYAGG
	JSON_EXISTS
	JSON_OBJECT
	JSON_OBJECTAGG
	JSON_QUERY
	JSON_SCALAR
	JSON_SERIALIZE
	JSON_TABLE
	JSON_VALUE
	KEEP
	KEY
	KEYS
	LABEL
	LANGUAGE
	LARGE_P
	LAST_P
	LATERAL_P
	LEADING
	LEAKPROOF
	LEAST
	LEFT
	LEVEL
	LIKE
	LIMIT
	LISTEN
	LOAD
	LOCAL
	LOCALTIME
	LOCALTIMESTAMP
	LOCATION
	LOCK_P
	LOCKED
	LOGGED
	MAPPING
	MATCH
	MATCHED
	MATERIALIZED
	MAXVALUE
	MERGE
	METHOD
	MINUTE_P
	MINVALUE
	MODE
	MONTH_P
	MOVE
	NAME_P
	NAMES
	NATIONAL
	NATURAL
	NCHAR
	NESTED
	NEW
	NEXT
	NFC
	NFD
	NFKC
	NFKD
	NO
	NONE
	NORMALIZE
	NORMALIZED
	NOT
	NOTHING
	NOTIFY
	NOTNULL
	NOWAIT
	NULL_P
	NULLIF
	NULLS_P
	NUMERIC
	OBJECT_P
	OF
	OFF
	OFFSET
	OIDS
	OLD
	ON
	ONLY
	OPERATOR
	OPTION
	OPTIONS
	OR
	ORDER
	ORDINALITY
	OTHERS
	OUT_P
	OUTER_P
	OVER
	OVERLAPS
	OVERLAY
	OVERRIDING
	OWNED
	OWNER
	PARALLEL
	PARAMETER
	PARSER
	PARTIAL
	PARTITION
	PASSING
	PASSWORD
	PATH
	PATHS
	PAUSE
	PLACING
	PLAN
	PLANS
	POLICY
	POSITION
	PRECEDING
	PRECISION
	PREPARE
	PREPARED
	PRESERVE
	PRIMARY
	PRIOR
	PRIVILEGES
	PROCEDURAL
	PROCEDURE
	PROCEDURES
	PROGRAM
	PUBLICATION
	QUOTE
	RANGE
	READ_P
	REAL
	REASSIGN
	RECHECK
	RECURSIVE
	REF
	REFERENCES
	REFERENCING
	REFRESH
	REINDEX
	RELATIVE_P
	RELEASE
	RENAME
	REPEATABLE
	REPLACE
	REPLICA
	RESET
	RESPECT_P
	RESTART
	RESTRICT
	RETURN
	RETURNING
	RETURNS
	REVOKE
	RIGHT
	ROLE
	ROLLBACK
	ROLLUP
	ROUTINE
	ROUTINES
	ROW
	ROWS
	RULE
	SAVEPOINT
	SCALAR
	SCHEMA
	SCHEMAS
	SCOPE
	SCROLL
	SEARCH
	SECOND_P
	SECURITY
	SELECT
	SEQUENCE
	SEQUENCES
	SERIALIZABLE
	SERVER
	SESSION
	SESSION_USER
	SET
	SETS
	SETOF
	SHARE
	SHOW
	SIMILAR
	SIMPLE
	SKIP
	SMALLINT
	SNAPSHOT
	SOME
	SQL_P
	STABLE
	STANDALONE_P
	START
	STATEMENT
	STATISTICS
	STDIN
	STDOUT
	STORAGE
	STORED
	STRICT_P
	STRIP_P
	SUBSCRIPTION
	SUBSTRING
	SUPPORT
	SYMMETRIC
	SYSID
	SYSTEM_P
	SYSTEM_USER
	TABLE
	TABLES
	TABLESAMPLE
	TABLESPACE
	TEMP
	TEMPLATE
	TEMPORARY
	TEXT_P
	THEN
	TIES
	TIME
	TIMESTAMP
	TO
	TRAILING
	TRANSACTION
	TRANSFORM
	TREAT
	TRIGGER
	TRIM
	TRUE_P
	TRUNCATE
	TRUSTED
	TYPE_P
	TYPES_P
	UESCAPE
	UNBOUNDED
	UNCOMMITTED
	UNENCRYPTED
	UNION
	UNIQUE
	UNKNOWN
	UNLISTEN
	UNLOGGED
	UNTIL
	UPDATE
	USER
	USING
	VACUUM
	VALID
	VALIDATE
	VALIDATOR
	VALUE_P
	VALUES
	VARCHAR
	VARIADIC
	VARYING
	VERBOSE
	VERSION_P
	VIEW
	VIEWS
	VOLATILE
	WHEN
	WHERE
	WHITESPACE_P
	WINDOW
	WITH
	WITHIN
	WITHOUT
	WORK
	WRAPPER
	WRITE_P
	XML_P
	XMLATTRIBUTES
	XMLCONCAT
	XMLELEMENT
	XMLEXISTS
	XMLFOREST
	XMLNAMESPACES
	XMLPARSE
	XMLPI
	XMLROOT
	XMLSERIALIZE
	XMLTABLE
	YEAR_P
	YES_P
	ZONE
)

// KeywordInfo represents information about a SQL keyword.
// Ported from postgres/src/include/parser/kwlist.h structure
type KeywordInfo struct {
	Name         string          // Keyword name (lowercase)
	Token        Token           // Token value
	Category     KeywordCategory // Keyword category
	CanBareLabel bool           // Can be used as a bare label
}

// PostgreSQL keyword list ported from postgres/src/include/parser/kwlist.h
// Note: This is a partial list for Phase 1 - full list will be generated in Phase 2
var Keywords = []KeywordInfo{
	// Ported from postgres/src/include/parser/kwlist.h:28-150
	{"abort", ABORT_P, UnreservedKeyword, true},
	{"absent", ABSENT, UnreservedKeyword, true},
	{"absolute", ABSOLUTE_P, UnreservedKeyword, true},
	{"access", ACCESS, UnreservedKeyword, true},
	{"action", ACTION, UnreservedKeyword, true},
	{"add", ADD_P, UnreservedKeyword, true},
	{"admin", ADMIN, UnreservedKeyword, true},
	{"after", AFTER, UnreservedKeyword, true},
	{"aggregate", AGGREGATE, UnreservedKeyword, true},
	{"all", ALL, ReservedKeyword, true},
	{"also", ALSO, UnreservedKeyword, true},
	{"alter", ALTER, UnreservedKeyword, true},
	{"always", ALWAYS, UnreservedKeyword, true},
	{"analyse", ANALYSE, ReservedKeyword, true},
	{"analyze", ANALYZE, ReservedKeyword, true},
	{"and", AND, ReservedKeyword, true},
	{"any", ANY, ReservedKeyword, true},
	{"array", ARRAY, ReservedKeyword, false},
	{"as", AS, ReservedKeyword, false},
	{"asc", ASC, ReservedKeyword, true},
	{"asensitive", ASENSITIVE, UnreservedKeyword, true},
	{"assertion", ASSERTION, UnreservedKeyword, true},
	{"assignment", ASSIGNMENT, UnreservedKeyword, true},
	{"asymmetric", ASYMMETRIC, ReservedKeyword, true},
	{"at", AT, UnreservedKeyword, true},
	{"atomic", ATOMIC, UnreservedKeyword, true},
	{"attach", ATTACH, UnreservedKeyword, true},
	{"attribute", ATTRIBUTE, UnreservedKeyword, true},
	{"authorization", AUTHORIZATION, TypeFuncNameKeyword, true},
	{"backward", BACKWARD, UnreservedKeyword, true},
	{"before", BEFORE, UnreservedKeyword, true},
	{"begin", BEGIN_P, UnreservedKeyword, true},
	{"between", BETWEEN, ColNameKeyword, true},
	{"bigint", BIGINT, ColNameKeyword, true},
	{"binary", BINARY, TypeFuncNameKeyword, true},
	{"bit", BIT, ColNameKeyword, true},
	{"boolean", BOOLEAN_P, ColNameKeyword, true},
	{"both", BOTH, ReservedKeyword, true},
	{"breadth", BREADTH, UnreservedKeyword, true},
	{"by", BY, UnreservedKeyword, true},
	{"cache", CACHE, UnreservedKeyword, true},
	{"call", CALL, UnreservedKeyword, true},
	{"called", CALLED, UnreservedKeyword, true},
	{"cascade", CASCADE, UnreservedKeyword, true},
	{"cascaded", CASCADED, UnreservedKeyword, true},
	{"case", CASE, ReservedKeyword, true},
	{"cast", CAST, ReservedKeyword, true},
	{"catalog", CATALOG_P, UnreservedKeyword, true},
	{"chain", CHAIN, UnreservedKeyword, true},
	{"char", CHAR_P, ColNameKeyword, false},
	{"character", CHARACTER, ColNameKeyword, false},
	{"characteristics", CHARACTERISTICS, UnreservedKeyword, true},
	{"check", CHECK, ReservedKeyword, true},
	{"checkpoint", CHECKPOINT, UnreservedKeyword, true},
	{"class", CLASS, UnreservedKeyword, true},
	{"close", CLOSE, UnreservedKeyword, true},
	{"cluster", CLUSTER, UnreservedKeyword, true},
	{"coalesce", COALESCE, ColNameKeyword, true},
	{"collate", COLLATE, ReservedKeyword, true},
	{"collation", COLLATION, TypeFuncNameKeyword, true},
	{"column", COLUMN, ReservedKeyword, true},
	{"columns", COLUMNS, UnreservedKeyword, true},
	{"comment", COMMENT, UnreservedKeyword, true},
	{"comments", COMMENTS, UnreservedKeyword, true},
	{"commit", COMMIT, UnreservedKeyword, true},
	{"committed", COMMITTED, UnreservedKeyword, true},
	{"compression", COMPRESSION, UnreservedKeyword, true},
	{"concurrently", CONCURRENTLY, TypeFuncNameKeyword, true},
	{"conditional", CONDITIONAL, UnreservedKeyword, true},
	{"configuration", CONFIGURATION, UnreservedKeyword, true},
	{"conflict", CONFLICT, UnreservedKeyword, true},
	{"connection", CONNECTION, UnreservedKeyword, true},
	{"constraint", CONSTRAINT, ReservedKeyword, true},
	{"constraints", CONSTRAINTS, UnreservedKeyword, true},
	{"content", CONTENT_P, UnreservedKeyword, true},
	{"continue", CONTINUE_P, UnreservedKeyword, true},
	{"conversion", CONVERSION_P, UnreservedKeyword, true},
	{"copy", COPY, UnreservedKeyword, true},
	{"cost", COST, UnreservedKeyword, true},
	{"create", CREATE, ReservedKeyword, false},
	{"cross", CROSS, TypeFuncNameKeyword, true},
	{"csv", CSV, UnreservedKeyword, true},
	{"cube", CUBE, UnreservedKeyword, true},
	{"current", CURRENT_P, UnreservedKeyword, true},
	{"current_catalog", CURRENT_CATALOG, ReservedKeyword, true},
	{"current_date", CURRENT_DATE, ReservedKeyword, true},
	{"current_role", CURRENT_ROLE, ReservedKeyword, true},
	{"current_schema", CURRENT_SCHEMA, TypeFuncNameKeyword, true},
	{"current_time", CURRENT_TIME, ReservedKeyword, true},
	{"current_timestamp", CURRENT_TIMESTAMP, ReservedKeyword, true},
	{"current_user", CURRENT_USER, ReservedKeyword, true},
	{"cursor", CURSOR, UnreservedKeyword, true},
	{"cycle", CYCLE, UnreservedKeyword, true},
	{"data", DATA_P, UnreservedKeyword, true},
	{"database", DATABASE, UnreservedKeyword, true},
	{"day", DAY_P, UnreservedKeyword, false},
	{"deallocate", DEALLOCATE, UnreservedKeyword, true},
	{"dec", DEC, ColNameKeyword, true},
	{"decimal", DECIMAL_P, ColNameKeyword, true},
	{"declare", DECLARE, UnreservedKeyword, true},
	{"default", DEFAULT, ReservedKeyword, true},
	{"defaults", DEFAULTS, UnreservedKeyword, true},
	{"deferrable", DEFERRABLE, ReservedKeyword, true},
	{"deferred", DEFERRED, UnreservedKeyword, true},
	{"definer", DEFINER, UnreservedKeyword, true},
	{"delete", DELETE_P, UnreservedKeyword, true},
	{"delimiter", DELIMITER, UnreservedKeyword, true},
	{"delimiters", DELIMITERS, UnreservedKeyword, true},
	{"depends", DEPENDS, UnreservedKeyword, true},
	{"depth", DEPTH, UnreservedKeyword, true},
	{"desc", DESC, ReservedKeyword, true},
	{"detach", DETACH, UnreservedKeyword, true},
	{"dictionary", DICTIONARY, UnreservedKeyword, true},
	{"disable", DISABLE_P, UnreservedKeyword, true},
	{"discard", DISCARD, UnreservedKeyword, true},
	{"distinct", DISTINCT, ReservedKeyword, true},
	{"do", DO, ReservedKeyword, true},
	{"document", DOCUMENT_P, UnreservedKeyword, true},
	{"domain", DOMAIN_P, UnreservedKeyword, true},
	{"double", DOUBLE_P, UnreservedKeyword, true},
	{"drop", DROP, UnreservedKeyword, true},
	{"each", EACH, UnreservedKeyword, true},
	{"else", ELSE, ReservedKeyword, true},
}

// keywordLookupMap provides fast keyword lookup by name.
// Built from the Keywords slice for O(1) lookups.
var keywordLookupMap map[string]*KeywordInfo

// init initializes the keyword lookup map.
// Ported from postgres/src/common/keywords.c lookup functionality
func init() {
	keywordLookupMap = make(map[string]*KeywordInfo, len(Keywords))
	for i := range Keywords {
		keywordLookupMap[Keywords[i].Name] = &Keywords[i]
	}
}

// LookupKeyword searches for a keyword by name (case-insensitive).
// Returns the keyword info if found, nil otherwise.
// Ported from postgres/src/common/kwlookup.c:ScanKeywordLookup functionality
func LookupKeyword(name string) *KeywordInfo {
	// Convert to lowercase for lookup (PostgreSQL keywords are case-insensitive)
	lowerName := strings.ToLower(name)
	return keywordLookupMap[lowerName]
}

// IsKeyword returns true if the given name is a SQL keyword.
// Ported from postgres keyword lookup functionality
func IsKeyword(name string) bool {
	return LookupKeyword(name) != nil
}

// IsReservedKeyword returns true if the given name is a reserved keyword.
// Ported from postgres keyword categorization logic
func IsReservedKeyword(name string) bool {
	if kw := LookupKeyword(name); kw != nil {
		return kw.Category == ReservedKeyword
	}
	return false
}

// GetKeywordNames returns a sorted slice of all keyword names.
// Useful for debugging and testing.
func GetKeywordNames() []string {
	names := make([]string, len(Keywords))
	for i, kw := range Keywords {
		names[i] = kw.Name
	}
	sort.Strings(names)
	return names
}

// GetKeywordsByCategory returns all keywords in a specific category.
func GetKeywordsByCategory(category KeywordCategory) []KeywordInfo {
	var result []KeywordInfo
	for _, kw := range Keywords {
		if kw.Category == category {
			result = append(result, kw)
		}
	}
	return result
}

// String returns the string representation of a KeywordCategory.
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

// String returns the string representation of a Token.
func (t Token) String() string {
	// This is a basic implementation - full token name mapping would be extensive
	if kw := findKeywordByToken(t); kw != nil {
		return strings.ToUpper(kw.Name)
	}
	return "UNKNOWN_TOKEN"
}

// findKeywordByToken is a helper to find a keyword by its token value.
func findKeywordByToken(token Token) *KeywordInfo {
	for i := range Keywords {
		if Keywords[i].Token == token {
			return &Keywords[i]
		}
	}
	return nil
}