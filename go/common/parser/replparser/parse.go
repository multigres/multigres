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
//
// Entry points for the replication-command parser.
// Mirrors postgres/src/backend/replication/walsender.c (PG 17.6) where
// exec_replication_command tokenizes, peeks the first token, and dispatches
// to the replication parser.

package replparser

import (
	"errors"

	"github.com/multigres/multigres/go/common/parser/ast"
)

var errReplSyntax = errors.New("syntax error in replication command")

// ParseReplicationCommand parses a single replication-protocol command and
// returns the resulting AST node.
//
// The accepted grammar is logical-replication only; physical-replication
// commands (BASE_BACKUP, TIMELINE_HISTORY, UPLOAD_MANIFEST, physical
// START_REPLICATION) are intentionally unparseable in this implementation.
func ParseReplicationCommand(input string) (ast.Stmt, error) {
	l := newReplLexer(input)
	if rc := replYyParse(l); rc != 0 && l.err == nil {
		l.err = errReplSyntax
	}
	if l.err != nil {
		return nil, l.err
	}
	return l.result, nil
}

// IsReplicationCommand reports whether the first token of input is one of
// the replication-protocol keywords.
//
// Mirrors PG's replication_scanner_is_replication_command
// (postgres/src/backend/replication/repl_scanner.l:294-318).
func IsReplicationCommand(input string) bool {
	l := newReplLexer(input)
	var lval replYySymType
	switch l.Lex(&lval) {
	case K_IDENTIFY_SYSTEM,
		K_START_REPLICATION,
		K_CREATE_REPLICATION_SLOT,
		K_DROP_REPLICATION_SLOT,
		K_ALTER_REPLICATION_SLOT,
		K_READ_REPLICATION_SLOT,
		K_SHOW:
		return true
	}
	return false
}
