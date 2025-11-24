// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2025, Supabase, Inc
//
//	Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//	Portions Copyright (c) 1994, The Regents of the University of California
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
package parser

// Generate the PostgreSQL parser using goyacc
// Ported from vitess/go/vt/sqlparser/generate.go:21
// This will process postgres.y grammar file and generate postgres.go parser

//go:generate goyacc -o postgres.go postgres.y
//go:generate sh -c "grep -v -e '^//line yaccpar:' -e '^//line yacctab:' postgres.go > postgres.go.tmp && mv postgres.go.tmp postgres.go"
//go:generate go tool goimports -w postgres.go
//go:generate go tool gofumpt -w postgres.go

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
