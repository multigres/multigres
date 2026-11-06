// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package shardref defines multigres.shard_ref, the composite type that names
// a shard: (database, table_group, shard), the SQL mirror of
// clustermetadata.ShardRef with the same field order. Any shard-local record
// that needs to name a shard without a foreign key into the sharding catalog
// uses it (e.g. multigres.migration.migration_target). It is created on every
// shard.
package shardref

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// TypeName is the name of the type in the multigres schema.
const TypeName = "shard_ref"

// CreateTypeSQL creates multigres.shard_ref. CREATE TYPE has no IF NOT EXISTS
// clause, so run it only through Ensure.
const CreateTypeSQL = `CREATE TYPE multigres.shard_ref AS (
	database TEXT,
	table_group TEXT,
	shard TEXT
)`

// legacyTypeName is the type's name on clusters built from main between the
// commit that wired the migration coordinator into the multipooler (which
// created it as multigres.shard_key) and the rename to shard_ref. It collides
// with the sharding catalog's multigres.shard_key table.
const legacyTypeName = "shard_key"

// CheckLegacyTypeSQL reports, as one row of two booleans, whether the legacy
// multigres.shard_key exists as a standalone composite ($1 = legacy name) and
// whether any type named multigres.shard_ref exists ($2 = current name).
const CheckLegacyTypeSQL = `SELECT
	EXISTS (SELECT 1 FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_class c ON c.oid = t.typrelid
		WHERE n.nspname = 'multigres' AND t.typname = $1 AND c.relkind = 'c'),
	EXISTS (SELECT 1 FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname = 'multigres' AND t.typname = $2)`

// CheckTypeSQL returns the typtype and, for a relation's row type or a
// standalone composite, the relkind (otherwise an empty string) of the type
// named $1 in the multigres schema; no row if there is none.
const CheckTypeSQL = `SELECT t.typtype::text, coalesce(c.relkind::text, '')
	FROM pg_type t
	JOIN pg_namespace n ON n.oid = t.typnamespace
	LEFT JOIN pg_class c ON c.oid = t.typrelid
	WHERE n.nspname = 'multigres' AND t.typname = $1`

// LegacyRename reports what Ensure found for the legacy multigres.shard_key
// type.
type LegacyRename int

const (
	// LegacyAbsent means there was no legacy standalone composite to rename.
	LegacyAbsent LegacyRename = iota
	// LegacyRenamed means the legacy type was renamed to shard_ref.
	LegacyRenamed
	// LegacyConflict means both the legacy type and shard_ref exist, so the
	// legacy type was left alone.
	LegacyConflict
)

// Ensure makes multigres.shard_ref exist as a standalone composite type. It
// first renames a legacy multigres.shard_key composite to shard_ref, so that
// existing columns of the legacy type (which reference it by OID) follow the
// rename rather than being stranded next to a fresh shard_ref. It is
// idempotent and safe to run from any caller in any order.
//
// The legacy rename exists only for clusters built from main between the
// migration-coordinator wiring and this rename, and can be removed once none
// remain.
func Ensure(ctx context.Context, qs executor.InternalQueryService) (LegacyRename, error) {
	legacy, err := renameLegacyType(ctx, qs)
	if err != nil {
		return legacy, err
	}
	return legacy, EnsureCompositeType(ctx, qs, TypeName, CreateTypeSQL)
}

// renameLegacyType renames a legacy multigres.shard_key standalone composite
// to shard_ref when shard_ref does not exist yet. A multigres.shard_key that
// is not a standalone composite (the sharding catalog's table) is not the
// legacy type and is never touched.
func renameLegacyType(ctx context.Context, qs executor.InternalQueryService) (LegacyRename, error) {
	res, err := qs.QueryAdminArgs(ctx, CheckLegacyTypeSQL, legacyTypeName, TypeName)
	if err != nil {
		return LegacyAbsent, fmt.Errorf("check for legacy type multigres.%s: %w", legacyTypeName, err)
	}
	var legacyExists, currentExists bool
	if err := executor.ScanSingleRow(res, &legacyExists, &currentExists); err != nil {
		return LegacyAbsent, fmt.Errorf("check for legacy type multigres.%s: %w", legacyTypeName, err)
	}
	switch {
	case !legacyExists:
		return LegacyAbsent, nil
	case currentExists:
		return LegacyConflict, nil
	}
	if _, err := qs.QueryAdmin(ctx, "ALTER TYPE multigres."+legacyTypeName+" RENAME TO "+TypeName); err != nil {
		return LegacyAbsent, fmt.Errorf("rename type multigres.%s to %s: %w", legacyTypeName, TypeName, err)
	}
	return LegacyRenamed, nil
}

// EnsureCompositeType creates the standalone composite type multigres.typeName
// with createSQL if no type of that name exists. CREATE TYPE has no IF NOT
// EXISTS clause, so it checks pg_type itself.
//
// An existing type is accepted only if it is a standalone composite. Every
// table, view, materialized view, and other relation also has a pg_type row
// under its own name (its row type), so a same-named relation would otherwise
// pass the check and leave the composite silently uncreated; it is reported
// as a conflict instead.
func EnsureCompositeType(ctx context.Context, qs executor.InternalQueryService, typeName, createSQL string) error {
	res, err := qs.QueryAdminArgs(ctx, CheckTypeSQL, typeName)
	if err != nil {
		return fmt.Errorf("check type multigres.%s exists: %w", typeName, err)
	}
	if res == nil || len(res.Rows) == 0 {
		if _, err := qs.QueryAdmin(ctx, createSQL); err != nil {
			return fmt.Errorf("create type multigres.%s: %w", typeName, err)
		}
		return nil
	}
	var typtype, relkind string
	if err := executor.ScanSingleRow(res, &typtype, &relkind); err != nil {
		return fmt.Errorf("check type multigres.%s exists: %w", typeName, err)
	}
	if relkind == "c" {
		return nil
	}
	return fmt.Errorf("type multigres.%s conflicts with an existing %s of the same name", typeName, describeType(typtype, relkind))
}

// describeType names the kind of object behind a pg_type row, for errors.
func describeType(typtype, relkind string) string {
	switch relkind {
	case "r":
		return "table"
	case "p":
		return "partitioned table"
	case "v":
		return "view"
	case "m":
		return "materialized view"
	case "f":
		return "foreign table"
	case "":
		// Not a relation's row type.
	default:
		return "relation (relkind " + relkind + ")"
	}
	switch typtype {
	case "b":
		return "base type"
	case "d":
		return "domain"
	case "e":
		return "enum type"
	case "r":
		return "range type"
	case "m":
		return "multirange type"
	case "p":
		return "pseudo-type"
	default:
		return "type (typtype " + typtype + ")"
	}
}
