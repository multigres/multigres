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

package pgregresstest

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/pgbuilder"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// targetCluster owns the backing cluster (frontend + PostgreSQL) for one
// Target. Construct via newTargetCluster; the caller drives Setup, the per-
// suite runners, optional ReinitializeBetweenSuites, and Teardown.
//
// Implementations are intentionally not thread-safe: the test runs targets
// sequentially so the shared PostgresBuilder, build dir, and (for the
// multigateway target) the package-scoped setupManager are all single-owner
// at any moment.
type targetCluster interface {
	// Target reports which Target this cluster serves.
	Target() Target
	// Setup brings the cluster up. ctx bounds the wall-clock time available
	// for initdb + first listener readiness.
	Setup(t *testing.T, ctx context.Context) error
	// FrontendPort is the port pg_regress and pg_isolation_regress connect
	// to (multigateway port or pgbouncer listen port depending on target).
	FrontendPort() int
	// DirectPgPort is the backend PostgreSQL port. Used to install the
	// isolation lock-detection shim so it bypasses the pooler.
	DirectPgPort(t *testing.T) int
	// Password is the superuser password to authenticate with.
	Password() string
	// ReinitializeBetweenSuites resets cluster state between regression and
	// isolation. The regression suite can leave PostgreSQL in a degraded
	// state (crashed backends, stale pools, mutated catalog); a fresh
	// cluster keeps isolation results meaningful.
	ReinitializeBetweenSuites(t *testing.T, ctx context.Context) error
	// Teardown stops every process this cluster started. Idempotent and
	// safe to call on a partially-initialised cluster.
	Teardown()
}

// newTargetCluster constructs the targetCluster appropriate for the requested
// Target. The PostgresBuilder is required so pgbouncer targets can place
// their per-target data directories under the builder's already-namespaced
// OutputDir.
func newTargetCluster(t *testing.T, target Target, builder *PostgresBuilder) (targetCluster, error) {
	t.Helper()
	switch target {
	case TargetMultigateway:
		return &multigatewayCluster{}, nil
	case TargetPgbouncerSession:
		return &pgbouncerCluster{
			target:  TargetPgbouncerSession,
			mode:    PgbouncerModeSession,
			builder: builder,
		}, nil
	case TargetPgbouncerTx:
		return &pgbouncerCluster{
			target:  TargetPgbouncerTx,
			mode:    PgbouncerModeTransaction,
			builder: builder,
		}, nil
	default:
		return nil, fmt.Errorf("no cluster implementation for target %q", target)
	}
}

// multigatewayCluster reuses the package-scoped shared setup managed by
// TestMain. Setup is essentially a stamp on that singleton, Teardown is a
// no-op (TestMain owns the lifecycle), and ReinitializeBetweenSuites delegates
// to shardsetup's full reinit.
type multigatewayCluster struct {
	setup *shardsetup.ShardSetup
}

func (c *multigatewayCluster) Target() Target { return TargetMultigateway }

func (c *multigatewayCluster) Setup(t *testing.T, _ context.Context) error {
	c.setup = getSharedSetup(t)
	c.setup.SetupTest(t)
	return nil
}

func (c *multigatewayCluster) FrontendPort() int { return c.setup.MultigatewayPgPort }

func (c *multigatewayCluster) DirectPgPort(t *testing.T) int {
	return c.setup.GetPrimary(t).Pgctld.PgPort
}

func (c *multigatewayCluster) Password() string { return shardsetup.TestPostgresPassword }

func (c *multigatewayCluster) ReinitializeBetweenSuites(t *testing.T, _ context.Context) error {
	c.setup.ReinitializeCluster(t)
	return nil
}

func (c *multigatewayCluster) Teardown() {
	// Shared setup is reused across tests in this package; TestMain handles
	// teardown. Intentionally a no-op here.
}

// pgbouncerCluster owns a per-target standalone PostgreSQL + pgbouncer pair.
// Each pgbouncer-* target spins up its own initdb'd cluster so regression
// mutations from one target cannot leak into another. ReinitializeBetweenSuites
// is implemented as a full stop + fresh initdb so isolation gets a clean
// cluster, mirroring shardsetup.ReinitializeCluster semantics.
type pgbouncerCluster struct {
	target  Target
	mode    PgbouncerMode
	builder *PostgresBuilder

	pg  *pgbuilder.Standalone
	pgb *Pgbouncer

	// reinitCount disambiguates data dir suffixes across reinit cycles, so
	// each fresh standalone lives in a unique directory and initdb does
	// not fail on an existing PGDATA.
	reinitCount int
}

func (c *pgbouncerCluster) Target() Target { return c.target }

func (c *pgbouncerCluster) Setup(t *testing.T, ctx context.Context) error {
	return c.bringUp(t, ctx)
}

func (c *pgbouncerCluster) bringUp(t *testing.T, ctx context.Context) error {
	t.Helper()
	pgSubdir := fmt.Sprintf("standalone-%s", c.target)
	pgbSubdir := fmt.Sprintf("pgbouncer-%s", c.target)
	if c.reinitCount > 0 {
		pgSubdir = fmt.Sprintf("%s-r%d", pgSubdir, c.reinitCount)
		pgbSubdir = fmt.Sprintf("%s-r%d", pgbSubdir, c.reinitCount)
	}

	pg, err := pgbuilder.StartStandaloneIn(t, ctx, c.builder.Builder, pgSubdir, "pgregress-"+string(c.target))
	if err != nil {
		return fmt.Errorf("start standalone PostgreSQL for %s: %w", c.target, err)
	}
	if err := applyRegressionGUCs(t, pg); err != nil {
		_ = pg.Stop()
		return fmt.Errorf("apply regression GUCs for %s: %w", c.target, err)
	}
	pgbDataDir := filepath.Join(c.builder.OutputDir, pgbSubdir)
	pgb, err := StartPgbouncer(t, ctx, c.mode, pgbDataDir, pg.Port, pg.Password)
	if err != nil {
		_ = pg.Stop()
		return fmt.Errorf("start pgbouncer for %s: %w", c.target, err)
	}
	c.pg = pg
	c.pgb = pgb
	return nil
}

// applyRegressionGUCs sets the role-level defaults pg_regress relies on (set
// via PGDATESTYLE / PGTZ env vars by pg_regress itself). For the multigateway
// target these flow client → multigateway → backend correctly, but pgbouncer
// drops at least DateStyle on the loopback path — leaving every date/time
// test diffing ISO output against the upstream "Postgres, MDY" fixtures.
//
// ALTER ROLE attaches the settings to the postgres role, so every connection
// by that role to any database picks them up — including the fresh
// "regression" database pg_regress creates a moment later via CREATE DATABASE
// (database-level ALTERs on template1 do not carry to the new DB; CREATE
// DATABASE clones the data files but not pg_db_role_setting rows). Applied
// directly to PG, bypassing the pooler.
func applyRegressionGUCs(t *testing.T, pg *pgbuilder.Standalone) error {
	t.Helper()
	connStr := fmt.Sprintf("host=127.0.0.1 port=%d user=%s password=%s dbname=%s sslmode=disable connect_timeout=5",
		pg.Port, pg.User, pg.Password, pg.Database)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("open admin connection: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Values mirror pg_regress.c: PGDATESTYLE=Postgres, MDY and
	// PGTZ=PST8PDT. IntervalStyle and extra_float_digits are not set by
	// pg_regress but are pinned here for parity with the shipped fixtures.
	stmts := []string{
		fmt.Sprintf("ALTER ROLE %s SET DateStyle = 'Postgres, MDY'", pg.User),
		fmt.Sprintf("ALTER ROLE %s SET TimeZone = 'PST8PDT'", pg.User),
		fmt.Sprintf("ALTER ROLE %s SET IntervalStyle = 'postgres'", pg.User),
		fmt.Sprintf("ALTER ROLE %s SET extra_float_digits = 1", pg.User),
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt, err)
		}
	}
	t.Logf("Applied regression GUC defaults to role %q on port %d", pg.User, pg.Port)
	return nil
}

func (c *pgbouncerCluster) FrontendPort() int {
	if c.pgb == nil {
		return 0
	}
	return c.pgb.ListenPort
}

func (c *pgbouncerCluster) DirectPgPort(t *testing.T) int {
	t.Helper()
	if c.pg == nil {
		return 0
	}
	return c.pg.Port
}

func (c *pgbouncerCluster) Password() string {
	if c.pg == nil {
		return ""
	}
	return c.pg.Password
}

func (c *pgbouncerCluster) ReinitializeBetweenSuites(t *testing.T, ctx context.Context) error {
	t.Helper()
	c.Teardown()
	c.reinitCount++
	return c.bringUp(t, ctx)
}

func (c *pgbouncerCluster) Teardown() {
	if c.pgb != nil {
		_ = c.pgb.Stop()
		c.pgb = nil
	}
	if c.pg != nil {
		_ = c.pg.Stop()
		c.pg = nil
	}
}
