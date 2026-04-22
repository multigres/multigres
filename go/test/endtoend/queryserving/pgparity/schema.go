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

package pgparity

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// resetSchema drops and recreates the public schema so each test file starts
// from a clean state. We apply this through the direct postgres connection
// (bypassing multigateway) so the cleanup itself cannot be affected by a
// multigateway bug that we're trying to detect.
//
// Both targets in the suite route to the same backend, so a single reset is
// visible to both.
func resetSchema(ctx context.Context, pg Target) error {
	conn, err := pgx.Connect(ctx, pg.DSN())
	if err != nil {
		return fmt.Errorf("connect to postgres for schema reset: %w", err)
	}
	defer conn.Close(ctx)

	// DROP SCHEMA CASCADE cleans up anything test files created. Recreate
	// public with the same ownership/grants Postgres gives it by default.
	stmts := []string{
		`DROP SCHEMA IF EXISTS public CASCADE`,
		`CREATE SCHEMA public`,
		`GRANT ALL ON SCHEMA public TO public`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("exec %q: %w", s, err)
		}
	}
	return nil
}
