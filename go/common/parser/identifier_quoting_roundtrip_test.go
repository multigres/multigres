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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIdentifierQuotingOnDeparse covers deparser identifier-emission sites that
// must quote names containing spaces, reserved words, or uppercase letters.
func TestIdentifierQuotingOnDeparse(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		mustContain string
	}{
		{
			name:        "CHECK constraint with spaces in name",
			sql:         `create table t (a text, constraint "resource_id not empty" check (a is not null))`,
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
			name:        "FOREIGN KEY constraint with spaces in name",
			sql:         `create table t (parent_id uuid, constraint "fk to parent" foreign key (parent_id) references parents(id))`,
			mustContain: `CONSTRAINT "fk to parent"`,
		},
		{
			name:        "EXCLUDE constraint with spaces in name",
			sql:         `create table t (during tsrange, constraint "no overlap" exclude using gist (during with &&))`,
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
		{
			name:        "CREATE TABLESPACE with quoted name",
			sql:         `create tablespace "my space" location '/data/ts'`,
			mustContain: `CREATE TABLESPACE "my space"`,
		},
		{
			name:        "ALTER TABLESPACE with quoted name",
			sql:         `alter tablespace "my space" set (seq_page_cost = 1.5)`,
			mustContain: `ALTER TABLESPACE "my space"`,
		},
		{
			name:        "DROP TABLESPACE with quoted name",
			sql:         `drop tablespace "my space"`,
			mustContain: `DROP TABLESPACE "my space"`,
		},
		{
			name:        "ALTER TABLE ALL IN TABLESPACE with quoted names",
			sql:         `alter table all in tablespace "old space" owned by "some role" set tablespace "new space"`,
			mustContain: `ALL IN TABLESPACE "old space"`,
		},
		{
			name:        "ALTER TABLE ALL IN TABLESPACE quotes SET TABLESPACE target",
			sql:         `alter table all in tablespace "old space" set tablespace "new space"`,
			mustContain: `SET TABLESPACE "new space"`,
		},
		{
			name:        "ALTER TABLE ALL IN TABLESPACE OWNED BY quotes role",
			sql:         `alter table all in tablespace "old space" owned by "weird role" set tablespace "new space"`,
			mustContain: `OWNED BY "weird role"`,
		},
		{
			name:        "GRANT role quotes role name",
			sql:         `grant "weird role" to "another role"`,
			mustContain: `"weird role"`,
		},
		{
			name:        "GRANT role quotes grantee",
			sql:         `grant "weird role" to "another role"`,
			mustContain: `"another role"`,
		},
		{
			name:        "CLUSTER ... USING with quoted index name",
			sql:         `cluster t using "weird index"`,
			mustContain: `USING "weird index"`,
		},
		{
			name:        "CREATE INDEX USING with quoted access method",
			sql:         `create index on t using "weird am" (a)`,
			mustContain: `USING "weird am"`,
		},
		{
			name:        "ALTER TYPE RENAME ATTRIBUTE with quoted names",
			sql:         `alter type t rename attribute "old attr" to "new attr"`,
			mustContain: `RENAME ATTRIBUTE "old attr" TO "new attr"`,
		},
		{
			name:        "ALTER DATABASE RENAME quotes database name",
			sql:         `alter database "old db" rename to "new db"`,
			mustContain: `"old db"`,
		},
		{
			name:        "ALTER DATABASE RENAME quotes new database name",
			sql:         `alter database "old db" rename to "new db"`,
			mustContain: `RENAME TO "new db"`,
		},
		{
			name:        "ALTER TABLE OWNER TO quotes role",
			sql:         `alter table t owner to "weird role"`,
			mustContain: `OWNER TO "weird role"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			asts, err := ParseSQL(tc.sql)
			require.NoError(t, err, "input SQL must parse")
			require.Len(t, asts, 1)

			deparsed := asts[0].SqlString()
			require.Contains(t, deparsed, tc.mustContain,
				"deparsed SQL must quote the identifier; got: %s", deparsed)

			_, err = ParseSQL(deparsed)
			require.NoError(t, err, "deparsed SQL must re-parse: %s", deparsed)
		})
	}
}
