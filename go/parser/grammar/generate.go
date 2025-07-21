/*
PostgreSQL Parser Generation
Ported from vitess/go/vt/sqlparser/generate.go patterns

This file contains go:generate directives for parser generation using goyacc.
The PostgreSQL grammar will be ported from postgres/src/backend/parser/gram.y
following the same patterns used by Vitess for their SQL parser.
*/

package grammar

// Generate the PostgreSQL parser using goyacc
// Ported from vitess/go/vt/sqlparser/generate.go:21
// This will process postgres.y grammar file and generate postgres.go parser

//go:generate goyacc -o postgres.go postgres.y

// TODO: In Phase 3, add additional generation directives for:
// - AST helper generation
// - Code formatting
// - Error handling customization
//
// Following Vitess patterns:
// //go:generate go run ../../tools/asthelpergen/main --in . --iface ...
// //go:generate go run ../../tools/astfmtgen ...
//
// These will be implemented once we have the basic parser structure in place.