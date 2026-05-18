// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2026, Supabase, Inc
//
//	Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//	Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.

package replparser

// NOTE: we deliberately omit goyacc's `-f` (fast-append) optimization.
// `-f` rewrites `$$ = append($1, $2)` to in-place unsafe pointer
// manipulation, which interacts poorly with our recursive list productions
// (e.g. create_slot_legacy_opt_list) and made `ParseReplicationCommand`
// hang under `go test`. The optimization is not load-bearing for this
// small grammar.

//go:generate go run ../goyacc -p replYy -o grammar.go grammar.y
//go:generate go tool goimports -w grammar.go
//go:generate go tool gofumpt -w grammar.go
