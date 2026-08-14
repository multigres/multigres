// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgregresstest

import (
	"database/sql"
	_ "embed"
	"fmt"
	"testing"
)

// regressPreseedSQL holds the helper-function definitions installed before the
// regression suite. The path is pinned to pg17, matching pgMajorDir(); add a
// sibling embed when a new major is introduced.
//
//go:embed testdata/pg17/regress_preseed.sql
var regressPreseedSQL string

// preseedRegressHelpers installs a small set of benign scaffolding helper
// functions directly on the primary (bypassing multigateway) before the
// regression suite runs. This is the same "create setup the gateway rejects by
// design directly on the primary" pattern the harness already uses for
// CREATE DATABASE scratch DBs (RunExternalTests) and the isolation PID-mapping
// shim (installPIDMappingFunction).
//
// Every function in regress_preseed.sql — EXPLAIN wrappers, check_ddl_rewrite,
// eval, check_estimated_rows — contains a dynamic EXECUTE, so multigateway's
// Tier-1 PL/pgSQL body analysis rejects their runtime CREATE. Without them, each
// test that CALLS such a helper cascades into "function does not exist", burying
// the real signal (does memoize / incremental sort / extended-stats estimation
// work?) under thousands of derived errors. Seeding them lets those substantive
// tests run; the test's own CREATE is still rejected by the gateway, so its
// "not supported through the connection pooler" line remains the single, honest
// divergence recorded in the patch.
//
// The seed cannot collide with the test's own CREATE: the gateway rejects that
// CREATE before it reaches the backend, so the seeded definition is the only one.
// The DDL runs on the primary and replicates to standbys via WAL, so every pooled
// backend sees the helper.
func (pb *PostgresBuilder) preseedRegressHelpers(t *testing.T, directPgPort int, password string) error {
	t.Helper()
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		directPgPort, password)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(regressPreseedSQL); err != nil {
		return fmt.Errorf("exec preseed: %w", err)
	}
	t.Logf("pre-seeded regression helper functions on primary (port %d)", directPgPort)
	return nil
}
