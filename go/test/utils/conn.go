// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package utils

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// GetBackendPID returns the PostgreSQL backend process ID for the given connection.
// When a session is pinned, consecutive calls on the same *sql.Conn must return
// the same PID; when unpinned, the pool is free to assign a different backend.
func GetBackendPID(t *testing.T, conn *sql.Conn) int {
	t.Helper()
	var pid int
	err := conn.QueryRowContext(t.Context(), "SELECT pg_backend_pid()").Scan(&pid)
	require.NoError(t, err, "failed to get backend pid")
	return pid
}

// RequirePinned asserts that two consecutive backend PID queries return the
// same value, proving the session is routed to a single reserved connection
// (session pinned). Returns the stable PID.
func RequirePinned(t *testing.T, conn *sql.Conn) int {
	t.Helper()
	pid1 := GetBackendPID(t, conn)
	pid2 := GetBackendPID(t, conn)
	require.Equal(t, pid1, pid2, "session should be pinned: backend PID must be stable")
	return pid1
}

// PgxExec runs sql on conn in conn's configured protocol mode and returns its
// error. Use it instead of conn.Exec when the protocol matters: pgx's Exec
// always uses the simple protocol for a statement without arguments, whatever
// DefaultQueryExecMode says, while Query honours it.
func PgxExec(ctx context.Context, conn *pgx.Conn, sql string, args ...any) error {
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	rows.Close()
	return rows.Err()
}

// RequirePgxExec is PgxExec that fails the test on error.
func RequirePgxExec(t *testing.T, ctx context.Context, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	require.NoError(t, PgxExec(ctx, conn, sql, args...), "%s", sql)
}
