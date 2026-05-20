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

package parser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConstraintNameRoundTrip locks in the fix for constraint names that require
// quoting (spaces, reserved words, uppercase). multigateway feeds multi-statement
// batches through ParseSQL -> per-statement SqlString -> Postgres, so the deparsed
// output must itself be valid SQL. Before the fix, a constraint named
// `"resource_id not empty"` deparsed as `CONSTRAINT resource_id not empty CHECK ...`,
// causing Postgres to fail with "syntax error at or near 'not'".
func TestConstraintNameRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		// Substring that must appear in the deparsed output. Each case targets one
		// of the constraint-name emission sites in the AST deparser.
		mustContain string
	}{
		{
			name: "CHECK constraint with spaces in name",
			sql: `create table t (
				resource_id text null,
				constraint "resource_id not empty" check (resource_id is null or char_length(resource_id) > 0)
			)`,
			mustContain: `CONSTRAINT "resource_id not empty" CHECK`,
		},
		{
			name:        "PRIMARY KEY constraint with spaces in name",
			sql:         `create table t (id uuid, constraint "primary key" primary key (id))`,
			mustContain: `CONSTRAINT "primary key" PRIMARY KEY`,
		},
		{
			name:        "UNIQUE constraint with spaces in name",
			sql:         `create table t (a int, constraint "unique a" unique (a))`,
			mustContain: `CONSTRAINT "unique a" UNIQUE`,
		},
		{
			name: "FOREIGN KEY constraint with spaces in name",
			sql: `create table t (
				parent_id uuid,
				constraint "fk to parent" foreign key (parent_id) references parents(id)
			)`,
			mustContain: `CONSTRAINT "fk to parent"`,
		},
		{
			name: "EXCLUDE constraint with spaces in name",
			sql: `create table t (
				during tsrange,
				constraint "no overlap" exclude using gist (during with &&)
			)`,
			mustContain: `CONSTRAINT "no overlap" EXCLUDE`,
		},
		{
			name:        "ALTER TABLE RENAME CONSTRAINT with spaces",
			sql:         `alter table t rename constraint "old name" to "new name"`,
			mustContain: `RENAME CONSTRAINT "old name" TO "new name"`,
		},
		{
			name:        "ALTER DOMAIN DROP CONSTRAINT with spaces",
			sql:         `alter domain d drop constraint "bad value"`,
			mustContain: `DROP CONSTRAINT "bad value"`,
		},
		{
			name:        "ALTER DOMAIN VALIDATE CONSTRAINT with spaces",
			sql:         `alter domain d validate constraint "bad value"`,
			mustContain: `VALIDATE CONSTRAINT "bad value"`,
		},
		{
			name:        "INSERT ON CONFLICT ON CONSTRAINT with spaces",
			sql:         `insert into t (id) values (1) on conflict on constraint "primary key" do nothing`,
			mustContain: `ON CONSTRAINT "primary key"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			asts, err := ParseSQL(tc.sql)
			require.NoError(t, err, "input SQL must parse")
			require.Len(t, asts, 1)

			deparsed := asts[0].SqlString()
			require.Contains(t, deparsed, tc.mustContain,
				"deparsed SQL must quote the constraint name; got: %s", deparsed)

			// The deparsed SQL must itself round-trip cleanly. If the constraint name
			// were emitted unquoted, this re-parse would fail with a syntax error.
			_, err = ParseSQL(deparsed)
			require.NoError(t, err, "deparsed SQL must re-parse: %s", deparsed)
		})
	}
}

// TestMultiStatementDeparseRoundTrip exercises the multigateway code path:
// pop sends an entire .up.sql file as a single Query message, the gateway parses
// it with ParseSQL, and then sends each statement's SqlString() to multipooler.
// Every deparsed statement must be valid SQL.
func TestMultiStatementDeparseRoundTrip(t *testing.T) {
	// Excerpt from the GoTrue add_saml migration that originally surfaced the bug.
	const migration = `create table if not exists auth.sso_providers (
  id uuid not null,
  resource_id text null,
  created_at timestamptz null,
  updated_at timestamptz null,
  primary key (id),
  constraint "resource_id not empty" check (resource_id is null or char_length(resource_id) > 0)
);
comment on table auth.sso_providers is 'Auth: Manages SSO identity provider information; see saml_providers for SAML.';
create table if not exists auth.sso_domains (
  id uuid not null,
  sso_provider_id uuid not null,
  domain text not null,
  created_at timestamptz null,
  updated_at timestamptz null,
  primary key (id),
  foreign key (sso_provider_id) references auth.sso_providers(id) on delete cascade,
  constraint "domain not empty" check (char_length(domain) > 0)
);`

	asts, err := ParseSQL(migration)
	require.NoError(t, err)
	require.Len(t, asts, 3)

	for i, stmt := range asts {
		deparsed := stmt.SqlString()
		_, err := ParseSQL(deparsed)
		require.NoError(t, err, "statement %d deparsed to invalid SQL: %s", i, deparsed)
	}

	// Sanity check: the semicolon inside the comment string literal must not have
	// split the batch. If it had, we'd see more than 3 statements (or the comment
	// statement would fail to parse).
	require.Contains(t, asts[1].SqlString(), "saml_providers for SAML.",
		"comment statement's string literal must survive batch parsing intact")
	require.False(t,
		strings.Contains(asts[1].SqlString(), "; see") &&
			!strings.Contains(asts[1].SqlString(), "'"),
		"comment text must remain inside the quoted string literal")
}
