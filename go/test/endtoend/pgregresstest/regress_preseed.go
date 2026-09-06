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
	"context"
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

// postgisPreseedSQL holds the PostGIS core test-helper functions re-seeded
// before each PostGIS test (the patched run_test.pl runs it via psql on a gateway
// unsafe connection — see MG_POSTGIS_PRESEED_CONNINFO). PostGIS test files
// CREATE OR REPLACE their own helpers — which the enforcing gateway rejects — and
// DROP them at the end, so they must be re-seeded per test. Extracted from
// postgis 3.6.3 test sources.
//
//go:embed testdata/pg17/external/postgis_preseed.sql
var postgisPreseedSQL string

// hypopgPreseedSQL holds the do_explain helper installed before the hypopg
// pg_regress suite. See hypopg_preseed.sql and externalPreseeds.
//
//go:embed testdata/pg17/external/hypopg_preseed.sql
var hypopgPreseedSQL string

// externalPreseeds maps an ExternalExtension.PreseedFile name to the embedded
// SQL run through a gateway unsafe connection before that extension's suite. Add
// an embed var and an entry here for each extension whose test helpers the
// enforcing gateway rejects by design (a dynamic EXECUTE in a helper body) but
// which the suite then depends on. Keyed by the PreseedFile value in the spec.
var externalPreseeds = map[string]string{
	"hypopg_preseed.sql": hypopgPreseedSQL,
}

// preseedRegressHelpers installs a small set of benign scaffolding helper
// functions through a gateway unsafe connection before the regression suite
// runs — the same pooled path the tests use, with no direct-postgres access.
//
// Almost every function in regress_preseed.sql — EXPLAIN wrappers,
// check_ddl_rewrite, eval, check_estimated_rows, hash_join_batches — contains a
// dynamic EXECUTE, so multigateway's Tier-1 PL/pgSQL body analysis rejects their
// runtime CREATE. Without them, each test that CALLS such a helper cascades into
// "function does not exist" (or, when the CREATE sits in an explicit transaction,
// a rejected statement the backend never saw), burying the real signal (does
// memoize / incremental sort / extended-stats estimation / hash-join batching
// work?) under thousands of derived errors. Seeding them lets those substantive
// tests run; the test's own CREATE is still rejected by the gateway, so its
// "not supported through the connection pooler" line remains the single, honest
// divergence recorded in the patch. (find_hash is the lone exception: it has no
// dynamic EXECUTE and the gateway allows the test's own CREATE — it is seeded
// only so hash_join_batches' body resolves at preseed time.)
//
// The seed cannot collide with the test's own CREATE: the gateway rejects that
// CREATE before it reaches the backend, so the seeded definition is the only one.
// The DDL runs on the primary and replicates to standbys via WAL, so every pooled
// backend sees the helper.
func (pb *PostgresBuilder) preseedRegressHelpers(t *testing.T, ctx context.Context, gatewayPort int, password string) error {
	t.Helper()
	if err := execViaGatewayUnsafeConnection(ctx, gatewayPort, password, regressPreseedSQL); err != nil {
		return fmt.Errorf("exec preseed: %w", err)
	}
	t.Logf("pre-seeded regression helper functions via gateway unsafe connection")
	return nil
}
