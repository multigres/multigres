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

// Round-trip regression anchors for the AST deparser. Each subtest verifies:
//
//  1. The input SQL parses.
//  2. The deparser output contains the expected quoted identifier
//     (proves the emission site goes through QuoteIdentifier).
//  3. The deparsed SQL re-parses cleanly (proves the output is valid SQL).
//
// Subtests are grouped by emission site category so a regression in one area
// surfaces as a focused cluster of failures.
//
// Complementary coverage:
//   - identifier_quoting_fuzz_test.go runs reflection-based mutation across
//     the entire test corpus and catches missed sites the curated cases here
//     do not name explicitly.
//   - testdata/ddl_cases.json contains JSON round-trip fixtures driven by
//     TestParseTestSuite.

func TestIdentifierQuotingOnDeparse(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		mustContain string
	}{
		// ----------------------------------------------------------------
		// CONSTRAINT names (CHECK / PRIMARY KEY / UNIQUE / FOREIGN KEY /
		// EXCLUDE, plus RENAME / DROP / VALIDATE / ON CONFLICT references).
		// ----------------------------------------------------------------
		{
			name:        "CHECK constraint name",
			sql:         `create table t (id int, constraint "weird c" check (id > 0))`,
			mustContain: `CONSTRAINT "weird c" CHECK`,
		},
		{
			name:        "PRIMARY KEY constraint name",
			sql:         `create table t (id int, constraint "weird pk" primary key (id))`,
			mustContain: `CONSTRAINT "weird pk" PRIMARY KEY`,
		},
		{
			name:        "UNIQUE constraint name",
			sql:         `create table t (id int, constraint "weird u" unique (id))`,
			mustContain: `CONSTRAINT "weird u" UNIQUE`,
		},
		{
			name:        "FOREIGN KEY constraint name",
			sql:         `create table t (a int, constraint "weird fk" foreign key (a) references p(id))`,
			mustContain: `CONSTRAINT "weird fk" FOREIGN KEY`,
		},
		{
			name:        "EXCLUDE constraint name + access method",
			sql:         `create table t (during tsrange, constraint "no overlap" exclude using "weird am" (during with &&))`,
			mustContain: `CONSTRAINT "no overlap" EXCLUDE USING "weird am"`,
		},
		{
			name:        "ALTER TABLE RENAME CONSTRAINT (both names)",
			sql:         `alter table t rename constraint "old c" to "new c"`,
			mustContain: `RENAME CONSTRAINT "old c" TO "new c"`,
		},
		{
			name:        "ALTER DOMAIN DROP CONSTRAINT",
			sql:         `alter domain d drop constraint "weird c"`,
			mustContain: `DROP CONSTRAINT "weird c"`,
		},
		{
			name:        "ALTER DOMAIN VALIDATE CONSTRAINT",
			sql:         `alter domain d validate constraint "weird c"`,
			mustContain: `VALIDATE CONSTRAINT "weird c"`,
		},
		{
			name:        "INSERT ON CONFLICT ON CONSTRAINT",
			sql:         `insert into t (id) values (1) on conflict on constraint "weird pk" do nothing`,
			mustContain: `ON CONSTRAINT "weird pk"`,
		},

		// ----------------------------------------------------------------
		// TABLESPACE names (CREATE / ALTER / DROP and ALTER TABLE ALL IN /
		// SET TABLESPACE).
		// ----------------------------------------------------------------
		{
			name:        "CREATE TABLESPACE name",
			sql:         `create tablespace "my space" location '/data/ts'`,
			mustContain: `CREATE TABLESPACE "my space"`,
		},
		{
			name:        "ALTER TABLESPACE name",
			sql:         `alter tablespace "my space" set (seq_page_cost = 1.5)`,
			mustContain: `ALTER TABLESPACE "my space"`,
		},
		{
			name:        "DROP TABLESPACE name",
			sql:         `drop tablespace "my space"`,
			mustContain: `DROP TABLESPACE "my space"`,
		},
		{
			name:        "ALTER TABLE ALL IN TABLESPACE source name",
			sql:         `alter table all in tablespace "old space" set tablespace "new space"`,
			mustContain: `ALL IN TABLESPACE "old space"`,
		},
		{
			name:        "ALTER TABLE ALL IN TABLESPACE target name",
			sql:         `alter table all in tablespace "old space" set tablespace "new space"`,
			mustContain: `SET TABLESPACE "new space"`,
		},
		{
			name:        "ALTER TABLE ALL IN TABLESPACE OWNED BY role",
			sql:         `alter table all in tablespace "old space" owned by "weird role" set tablespace "new space"`,
			mustContain: `OWNED BY "weird role"`,
		},
		{
			name:        "CREATE TABLE TABLESPACE clause",
			sql:         `create table t (a int) tablespace "weird ts"`,
			mustContain: `TABLESPACE "weird ts"`,
		},

		// ----------------------------------------------------------------
		// ROLE / GRANT / REVOKE / OWNER.
		// ----------------------------------------------------------------
		{
			name:        "ALTER TABLE OWNER TO role",
			sql:         `alter table t owner to "weird role"`,
			mustContain: `OWNER TO "weird role"`,
		},
		{
			name:        "GRANT role list",
			sql:         `grant "weird role" to "another role"`,
			mustContain: `"weird role"`,
		},
		{
			name:        "GRANT grantee list",
			sql:         `grant "weird role" to "another role"`,
			mustContain: `"another role"`,
		},
		{
			name:        "REVOKE ADMIN OPTION FOR granted role",
			sql:         `revoke admin option for "weird r1" from "weird r2"`,
			mustContain: `"weird r1"`,
		},
		{
			name:        "REVOKE ADMIN OPTION FOR grantee role",
			sql:         `revoke admin option for "weird r1" from "weird r2"`,
			mustContain: `FROM "weird r2"`,
		},
		{
			name:        "DROP ROLE list",
			sql:         `drop role "weird r1", "weird r2"`,
			mustContain: `"weird r1", "weird r2"`,
		},
		{
			name:        "ALTER GROUP ADD USER list",
			sql:         `alter group testgroup add user "weird u1", "weird u2"`,
			mustContain: `"weird u1", "weird u2"`,
		},
		{
			name:        "ALTER ROLE RENAME (both names)",
			sql:         `alter role "old r" rename to "new r"`,
			mustContain: `"old r"`,
		},

		// ----------------------------------------------------------------
		// CURSORS / PORTALS / PREPARED STATEMENTS.
		// ----------------------------------------------------------------
		{
			name:        "DECLARE CURSOR portal name",
			sql:         `declare "weird cursor" cursor for select 1`,
			mustContain: `DECLARE "weird cursor"`,
		},
		{
			name:        "FETCH FROM portal name",
			sql:         `fetch from "weird cursor"`,
			mustContain: `FROM "weird cursor"`,
		},
		{
			name:        "CLOSE portal name",
			sql:         `close "weird cursor"`,
			mustContain: `CLOSE "weird cursor"`,
		},
		{
			name:        "PREPARE statement name",
			sql:         `prepare "weird stmt" as select 1`,
			mustContain: `PREPARE "weird stmt"`,
		},
		{
			name:        "EXECUTE statement name",
			sql:         `execute "weird stmt"`,
			mustContain: `EXECUTE "weird stmt"`,
		},
		{
			name:        "DEALLOCATE statement name",
			sql:         `deallocate "weird stmt"`,
			mustContain: `DEALLOCATE "weird stmt"`,
		},
		{
			name:        "WHERE CURRENT OF cursor name",
			sql:         `update t set a = 1 where current of "weird cursor"`,
			mustContain: `CURRENT OF "weird cursor"`,
		},

		// ----------------------------------------------------------------
		// NOTIFY / LISTEN / UNLISTEN channel names.
		// ----------------------------------------------------------------
		{
			name:        "NOTIFY channel name",
			sql:         `notify "weird channel"`,
			mustContain: `NOTIFY "weird channel"`,
		},
		{
			name:        "NOTIFY with payload",
			sql:         `notify "weird channel", 'hi'`,
			mustContain: `NOTIFY "weird channel"`,
		},
		{
			name:        "LISTEN channel name",
			sql:         `listen "weird channel"`,
			mustContain: `LISTEN "weird channel"`,
		},
		{
			name:        "UNLISTEN channel name",
			sql:         `unlisten "weird channel"`,
			mustContain: `UNLISTEN "weird channel"`,
		},

		// ----------------------------------------------------------------
		// SAVEPOINTS.
		// ----------------------------------------------------------------
		{
			name:        "SAVEPOINT name",
			sql:         `savepoint "weird sp"`,
			mustContain: `SAVEPOINT "weird sp"`,
		},
		{
			name:        "RELEASE SAVEPOINT name",
			sql:         `release savepoint "weird sp"`,
			mustContain: `SAVEPOINT "weird sp"`,
		},
		{
			name:        "ROLLBACK TO SAVEPOINT name",
			sql:         `rollback to savepoint "weird sp"`,
			mustContain: `SAVEPOINT "weird sp"`,
		},

		// ----------------------------------------------------------------
		// DATABASES.
		// ----------------------------------------------------------------
		{
			name:        "CREATE DATABASE name",
			sql:         `create database "weird db"`,
			mustContain: `CREATE DATABASE "weird db"`,
		},
		{
			name:        "DROP DATABASE name",
			sql:         `drop database "weird db"`,
			mustContain: `DROP DATABASE "weird db"`,
		},
		{
			name:        "ALTER DATABASE RENAME (old name)",
			sql:         `alter database "old db" rename to "new db"`,
			mustContain: `"old db"`,
		},
		{
			name:        "ALTER DATABASE RENAME (new name)",
			sql:         `alter database "old db" rename to "new db"`,
			mustContain: `RENAME TO "new db"`,
		},

		// ----------------------------------------------------------------
		// SCHEMAS / TYPES.
		// ----------------------------------------------------------------
		{
			name:        "ALTER SCHEMA RENAME",
			sql:         `alter schema "old s" rename to "new s"`,
			mustContain: `"old s"`,
		},
		{
			name:        "ALTER TYPE RENAME ATTRIBUTE (both names)",
			sql:         `alter type t rename attribute "old a" to "new a"`,
			mustContain: `RENAME ATTRIBUTE "old a" TO "new a"`,
		},

		// ----------------------------------------------------------------
		// EVENT TRIGGERS & TRIGGERS.
		// ----------------------------------------------------------------
		{
			name:        "CREATE EVENT TRIGGER trigger and event names",
			sql:         `create event trigger "weird trig" on "weird event" execute function f()`,
			mustContain: `CREATE EVENT TRIGGER "weird trig" ON "weird event"`,
		},
		{
			name:        "ALTER EVENT TRIGGER name",
			sql:         `alter event trigger "weird trig" enable`,
			mustContain: `ALTER EVENT TRIGGER "weird trig"`,
		},
		{
			name:        "CREATE TRIGGER name",
			sql:         `create trigger "weird trig" before insert on t for each row execute function f()`,
			mustContain: `TRIGGER "weird trig"`,
		},
		{
			name:        "CREATE TRIGGER UPDATE OF column list",
			sql:         `create trigger trg before update of "weird col" on t for each row execute function f()`,
			mustContain: `UPDATE OF "weird col"`,
		},
		{
			name:        "CREATE TRIGGER REFERENCING transition AS",
			sql:         `create trigger trg after update on t referencing new table as "weird t" for each statement execute function f()`,
			mustContain: `AS "weird t"`,
		},

		// ----------------------------------------------------------------
		// FOREIGN DATA WRAPPERS / SERVERS / USER MAPPINGS.
		// ----------------------------------------------------------------
		{
			name:        "CREATE FDW name",
			sql:         `create foreign data wrapper "weird fdw"`,
			mustContain: `CREATE FOREIGN DATA WRAPPER "weird fdw"`,
		},
		{
			name:        "ALTER FDW name",
			sql:         `alter foreign data wrapper "weird fdw" options (add "k" 'v')`,
			mustContain: `ALTER FOREIGN DATA WRAPPER "weird fdw"`,
		},
		{
			name:        "CREATE SERVER (server + FDW names)",
			sql:         `create server "weird srv" foreign data wrapper "weird fdw"`,
			mustContain: `CREATE SERVER "weird srv" FOREIGN DATA WRAPPER "weird fdw"`,
		},
		{
			name:        "ALTER SERVER name + VERSION literal",
			sql:         `alter server "weird srv" version '1.0'`,
			mustContain: `ALTER SERVER "weird srv" VERSION '1.0'`,
		},
		{
			name:        "CREATE FOREIGN TABLE SERVER",
			sql:         `create foreign table ft (a int) server "weird srv"`,
			mustContain: `SERVER "weird srv"`,
		},
		{
			name:        "CREATE USER MAPPING SERVER",
			sql:         `create user mapping for current_user server "weird srv"`,
			mustContain: `SERVER "weird srv"`,
		},
		{
			name:        "ALTER USER MAPPING SERVER",
			sql:         `alter user mapping for current_user server "weird srv" options (add "k" 'v')`,
			mustContain: `SERVER "weird srv"`,
		},
		{
			name:        "DROP USER MAPPING SERVER",
			sql:         `drop user mapping for current_user server "weird srv"`,
			mustContain: `SERVER "weird srv"`,
		},

		// ----------------------------------------------------------------
		// ACCESS METHODS / OPERATOR CLASSES / OPERATOR FAMILIES.
		// ----------------------------------------------------------------
		{
			name:        "CREATE ACCESS METHOD name",
			sql:         `create access method "weird am" type index handler my_handler`,
			mustContain: `CREATE ACCESS METHOD "weird am"`,
		},
		{
			name:        "CREATE INDEX USING access method",
			sql:         `create index on t using "weird am" (a)`,
			mustContain: `USING "weird am"`,
		},
		{
			name:        "CLUSTER USING index name",
			sql:         `cluster t using "weird index"`,
			mustContain: `USING "weird index"`,
		},
		{
			name:        "CREATE OPERATOR CLASS USING access method",
			sql:         `create operator class my_class for type int using "weird am" as operator 1 <`,
			mustContain: `USING "weird am"`,
		},
		{
			name:        "CREATE OPERATOR FAMILY USING access method",
			sql:         `create operator family my_family using "weird am"`,
			mustContain: `USING "weird am"`,
		},
		{
			name:        "ALTER OPERATOR FAMILY USING access method",
			sql:         `alter operator family my_family using "weird am" add operator 1 <`,
			mustContain: `USING "weird am"`,
		},

		// ----------------------------------------------------------------
		// PUBLICATIONS & SUBSCRIPTIONS.
		// ----------------------------------------------------------------
		{
			name:        "CREATE PUBLICATION name",
			sql:         `create publication "weird pub" for all tables`,
			mustContain: `CREATE PUBLICATION "weird pub"`,
		},
		{
			name:        "CREATE PUBLICATION FOR TABLES IN SCHEMA",
			sql:         `create publication pub for tables in schema "weird schema"`,
			mustContain: `TABLES IN SCHEMA "weird schema"`,
		},
		{
			name:        "ALTER PUBLICATION name",
			sql:         `alter publication "weird pub" add table t`,
			mustContain: `ALTER PUBLICATION "weird pub"`,
		},
		{
			name:        "ALTER PUBLICATION ADD TABLES IN SCHEMA",
			sql:         `alter publication pub add tables in schema "weird schema"`,
			mustContain: `TABLES IN SCHEMA "weird schema"`,
		},
		{
			name:        "CREATE SUBSCRIPTION name + CONNECTION literal",
			sql:         `create subscription "weird sub" connection 'host=h' publication p`,
			mustContain: `CREATE SUBSCRIPTION "weird sub" CONNECTION 'host=h'`,
		},
		{
			name:        "ALTER SUBSCRIPTION name",
			sql:         `alter subscription "weird sub" enable`,
			mustContain: `ALTER SUBSCRIPTION "weird sub"`,
		},

		// ----------------------------------------------------------------
		// LANGUAGES / POLICIES.
		// ----------------------------------------------------------------
		{
			name:        "CREATE LANGUAGE with HANDLER",
			sql:         `create trusted language "weird lang" handler plpgsql_call_handler`,
			mustContain: `LANGUAGE "weird lang"`,
		},
		{
			name:        "ALTER POLICY policy name",
			sql:         `alter policy "weird policy" on t to public`,
			mustContain: `ALTER POLICY "weird policy"`,
		},

		// ----------------------------------------------------------------
		// REINDEX / SET CONSTRAINTS / LOCK / TRUNCATE — container walks
		// that route through RangeVar.SqlString.
		// ----------------------------------------------------------------
		{
			name:        "REINDEX TABLE preserves schema qualification",
			sql:         `reindex table "weird schema"."weird table"`,
			mustContain: `"weird schema"."weird table"`,
		},
		{
			name:        "REINDEX SCHEMA name",
			sql:         `reindex schema "weird schema"`,
			mustContain: `REINDEX SCHEMA "weird schema"`,
		},
		{
			name:        "SET CONSTRAINTS list",
			sql:         `set constraints "weird c1", "weird c2" deferred`,
			mustContain: `"weird c1", "weird c2"`,
		},
		{
			name:        "LOCK TABLE list",
			sql:         `lock table "weird t1", "weird t2" in access exclusive mode`,
			mustContain: `"weird t1", "weird t2"`,
		},
		{
			name:        "TRUNCATE relation list",
			sql:         `truncate table "weird t1", "weird t2"`,
			mustContain: `"weird t1", "weird t2"`,
		},
		{
			name:        "TRUNCATE preserves ONLY qualifier",
			sql:         `truncate only "weird t"`,
			mustContain: `ONLY "weird t"`,
		},

		// ----------------------------------------------------------------
		// ALTER TABLE sub-commands.
		// ----------------------------------------------------------------
		{
			name:        "ALTER TABLE SET ACCESS METHOD",
			sql:         `alter table t set access method "weird am"`,
			mustContain: `SET ACCESS METHOD "weird am"`,
		},
		{
			name:        "ALTER TABLE REPLICA IDENTITY USING INDEX",
			sql:         `alter table t replica identity using index "weird idx"`,
			mustContain: `REPLICA IDENTITY USING INDEX "weird idx"`,
		},

		// ----------------------------------------------------------------
		// CREATE FUNCTION / STATISTICS / TABLE AS sub-nodes.
		// ----------------------------------------------------------------
		{
			name:        "CREATE FUNCTION parameter name",
			sql:         `create function f("weird p" int) returns int language sql as $$select 1$$`,
			mustContain: `"weird p" INT`,
		},
		{
			name:        "CREATE STATISTICS ON column reference",
			sql:         `create statistics s on "weird col" from t`,
			mustContain: `"weird col"`,
		},
		{
			name:        "CREATE TABLE AS USING access method",
			sql:         `create table t using "weird am" as select 1`,
			mustContain: `USING "weird am"`,
		},

		// ----------------------------------------------------------------
		// Expression-level fixes: named args, windows, MERGE column lists.
		// ----------------------------------------------------------------
		{
			name:        "Named function argument",
			sql:         `select f("weird arg" => 1)`,
			mustContain: `"weird arg" => 1`,
		},
		{
			name:        "WINDOW clause window name",
			sql:         `select x from t window "weird w" as ()`,
			mustContain: `WINDOW "weird w" AS`,
		},
		{
			name:        "OVER references quoted window name",
			sql:         `select row_number() over "weird w" from t window "weird w" as ()`,
			mustContain: `OVER "weird w"`,
		},
		{
			name:        "MERGE INSERT target column list",
			sql:         `merge into t using s on t.id = s.id when not matched then insert ("weird col") values (s.x)`,
			mustContain: `INSERT ("weird col")`,
		},
		{
			name:        "MERGE UPDATE SET target column",
			sql:         `merge into t using s on t.id = s.id when matched then update set "weird col" = s.x`,
			mustContain: `"weird col" = s.x`,
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
