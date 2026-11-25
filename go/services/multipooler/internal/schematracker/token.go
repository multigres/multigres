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

package schematracker

import (
	"context"
	"errors"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// pinSettingsQuery fixes, for the rest of the token transaction, every setting
// that changes the text the token is hashed from. regclass, format_type and the
// pg_get_*def functions schema-qualify a name only when it is not visible on the
// search_path, and deparsed constants in defaults, constraints and views are
// printed with their type's output function, which reads the date, interval,
// float, bytea and money settings. Without the pins the token would move with
// the admin role's settings and not only with the schema.
const pinSettingsQuery = `
SELECT pg_catalog.set_config('search_path', 'pg_catalog, pg_temp', true),
       pg_catalog.set_config('DateStyle', 'ISO, YMD', true),
       pg_catalog.set_config('IntervalStyle', 'postgres', true),
       pg_catalog.set_config('TimeZone', 'UTC', true),
       pg_catalog.set_config('extra_float_digits', '1', true),
       pg_catalog.set_config('bytea_output', 'hex', true),
       pg_catalog.set_config('lc_monetary', 'C', true),
       pg_catalog.set_config('quote_all_identifiers', 'off', true)`

// schemaHashQuery hashes a canonical dump of the user schema: one line per schema,
// relation, column, default, index, constraint, rewrite rule and policy, named
// rather than identified by OID, sorted bytewise and hashed with sha256.
//
// It covers the catalog rows the gateway's catalog cache will load. Objects in
// pg_* schemas, information_schema and the multigres sidecar schema are left
// out: they are not user schema, and the sidecar changes on its own (heartbeat,
// sharding catalog). Statistics (reltuples, relpages) are left out too, since
// they move without any DDL.
//
// The token is compared only with earlier tokens from the same pooler, so the
// dump may change between releases; a changed dump costs one extra reload.
const schemaHashQuery = `
WITH rels AS (
  SELECT c.oid
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
    AND n.nspname NOT LIKE 'pg\_%'
    AND n.nspname NOT IN ('information_schema', 'multigres')
),
lines(line) AS (
  SELECT pg_catalog.format('schema %I', n.nspname)
  FROM pg_catalog.pg_namespace n
  WHERE n.nspname NOT LIKE 'pg\_%'
    AND n.nspname NOT IN ('information_schema', 'multigres')
  UNION ALL
  SELECT pg_catalog.format('rel %s %s %s %s %s %s',
           c.oid::pg_catalog.regclass, c.relkind, c.relrowsecurity, c.relforcerowsecurity,
           pg_catalog.pg_get_partkeydef(c.oid), pg_catalog.pg_get_expr(c.relpartbound, c.oid))
  FROM pg_catalog.pg_class c
  JOIN rels r ON r.oid = c.oid
  UNION ALL
  SELECT pg_catalog.format('att %s %s %I %s %s %s %s %s',
           a.attrelid::pg_catalog.regclass, a.attnum, a.attname,
           pg_catalog.format_type(a.atttypid, a.atttypmod),
           a.attnotnull, a.attidentity, a.attgenerated,
           CASE WHEN a.attcollation <> 0 THEN a.attcollation::pg_catalog.regcollation END)
  FROM pg_catalog.pg_attribute a
  JOIN rels r ON r.oid = a.attrelid
  WHERE a.attnum > 0 AND NOT a.attisdropped
  UNION ALL
  SELECT pg_catalog.format('default %s %s %s',
           d.adrelid::pg_catalog.regclass, d.adnum, pg_catalog.pg_get_expr(d.adbin, d.adrelid))
  FROM pg_catalog.pg_attrdef d
  JOIN rels r ON r.oid = d.adrelid
  UNION ALL
  SELECT pg_catalog.format('index %s %s', i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid))
  FROM pg_catalog.pg_index i
  JOIN rels r ON r.oid = i.indrelid
  UNION ALL
  SELECT pg_catalog.format('constraint %s %I %s',
           k.conrelid::pg_catalog.regclass, k.conname, pg_catalog.pg_get_constraintdef(k.oid))
  FROM pg_catalog.pg_constraint k
  JOIN rels r ON r.oid = k.conrelid
  UNION ALL
  SELECT pg_catalog.format('rule %s %I %s %s',
           w.ev_class::pg_catalog.regclass, w.rulename, w.ev_enabled, pg_catalog.pg_get_ruledef(w.oid))
  FROM pg_catalog.pg_rewrite w
  JOIN rels r ON r.oid = w.ev_class
  UNION ALL
  SELECT pg_catalog.format('policy %s %I %s %s %s %s %s',
           p.polrelid::pg_catalog.regclass, p.polname, p.polcmd, p.polpermissive,
           (SELECT pg_catalog.string_agg(s.name, ',' ORDER BY s.name COLLATE "C")
              FROM (SELECT CASE WHEN u.role = 0 THEN 'public'
                                ELSE u.role::pg_catalog.regrole::pg_catalog.text END AS name
                      FROM pg_catalog.unnest(p.polroles) AS u(role)) s),
           pg_catalog.pg_get_expr(p.polqual, p.polrelid),
           pg_catalog.pg_get_expr(p.polwithcheck, p.polrelid))
  FROM pg_catalog.pg_policy p
  JOIN rels r ON r.oid = p.polrelid
)
SELECT pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
         COALESCE(pg_catalog.string_agg(line, E'\n' ORDER BY line COLLATE "C"), ''), 'UTF8')), 'hex')
FROM lines`

// computeToken returns the current schema token. It runs on the admin pool so
// that a saturated regular pool cannot delay it, inside a transaction so the
// settings pinned by pinSettingsQuery apply to schemaHashQuery and are discarded
// with the rollback.
func computeToken(ctx context.Context, qs executor.InternalQueryService) (string, error) {
	tx, err := qs.BeginAdmin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Query(ctx, pinSettingsQuery); err != nil {
		return "", err
	}
	result, err := tx.Query(ctx, schemaHashQuery)
	if err != nil {
		return "", err
	}
	rows := result.StructuredRows()
	if len(rows) != 1 || len(rows[0].Values) != 1 || rows[0].Values[0] == nil {
		return "", errors.New("schema token query returned no value")
	}
	return string(rows[0].Values[0]), nil
}
