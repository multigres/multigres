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

package handler

import (
	"strings"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// commitsAtExecute reports whether PostgreSQL commits the implicit transaction
// as soon as an extended-protocol Execute of stmt completes, instead of
// holding it open until Sync. exec_execute_message (postgres.c) commits at
// once for a transaction statement (is_xact_command) and for a statement
// that set XACT_FLAGS_NEEDIMMEDIATECOMMIT, which every caller of
// PreventInTransactionBlock does; every other completed Execute sets
// XACT_FLAGS_PIPELINING, which makes a later CREATE DATABASE in the same
// pipeline fail with "cannot be executed within a pipeline". The gateway
// mirrors that flag for CREATE and DROP TABLEGROUP (notePipelinedExecute), so
// it has to know the same set.
//
// PostgreSQL decides at execution time, on the transformed statement and the
// catalog; this reads the raw parse tree. Every PreventInTransactionBlock call
// in the PostgreSQL 17 backend (REL_17_STABLE, found by grep), and how each is
// handled here:
//
//   - transaction statements (is_xact_command, which also covers utility.c's
//     COMMIT PREPARED and ROLLBACK PREPARED): handled, *ast.TransactionStmt.
//   - utility.c: CREATE DATABASE, DROP DATABASE, CREATE TABLESPACE, DROP
//     TABLESPACE, ALTER SYSTEM: handled, unconditional. CREATE INDEX
//     CONCURRENTLY, DROP INDEX CONCURRENTLY, ALTER TABLE ... DETACH PARTITION
//     CONCURRENTLY: handled via the Concurrent flags.
//   - discard.c: DISCARD ALL: handled, unconditional.
//   - vacuum.c: VACUUM (any form): handled via VacuumStmt.IsVacuumcmd. A bare
//     ANALYZE calls IsInTransactionBlock instead, which only reads the flags,
//     so it is pipelining like any other statement.
//   - dbcommands.c: ALTER DATABASE ... SET TABLESPACE: handled via the
//     "tablespace" option.
//   - cluster.c: CLUSTER without a table: handled via Relation == nil.
//     CLUSTER of a partitioned table: declined, below.
//   - indexcmds.c: REINDEX SCHEMA, SYSTEM and DATABASE, and REINDEX ...
//     CONCURRENTLY: handled via the kind and the "concurrently" option.
//     REINDEX TABLE or INDEX of a partitioned relation (ReindexPartitions):
//     declined, below.
//   - subscriptioncmds.c: CREATE SUBSCRIPTION with create_slot (the default
//     when connect is true), ALTER SUBSCRIPTION ... REFRESH PUBLICATION,
//     SET/ADD/DROP PUBLICATION with refresh (the default), and SET (failover):
//     handled from the kind and options. DROP SUBSCRIPTION: declined, below.
//   - walsender.c: replication-protocol commands: out of scope, they are not
//     SQL the gateway executes.
//
// Declined: DROP SUBSCRIPTION (commits at once only when the subscription has
// a replication slot), CLUSTER of a partitioned table and REINDEX of a
// partitioned table or index. Each depends on catalog state the parse tree
// does not carry, so each is treated as pipelining. The only effect is that
// tablegroup DDL after one of them in the same pipeline is refused where
// PostgreSQL might have allowed it; nothing else changes.
//
// Statements that look non-transactional but call neither function, such as
// REFRESH MATERIALIZED VIEW CONCURRENTLY, are pipelining in PostgreSQL and
// here.
func commitsAtExecute(stmt ast.Stmt) bool {
	switch s := stmt.(type) {
	case *ast.TransactionStmt:
		return true
	case *ast.DiscardStmt:
		return s.Target == ast.DISCARD_ALL
	case *ast.CreatedbStmt, *ast.DropdbStmt, *ast.CreateTableSpaceStmt, *ast.DropTableSpaceStmt, *ast.AlterSystemStmt:
		return true
	case *ast.VacuumStmt:
		return s.IsVacuumcmd
	case *ast.ClusterStmt:
		return s.Relation == nil
	case *ast.ReindexStmt:
		switch s.Kind {
		case ast.REINDEX_OBJECT_SCHEMA, ast.REINDEX_OBJECT_SYSTEM, ast.REINDEX_OBJECT_DATABASE:
			return true
		}
		return optionBool(s.Params, "concurrently", false)
	case *ast.IndexStmt:
		return s.Concurrent
	case *ast.DropStmt:
		return s.RemoveType == ast.OBJECT_INDEX && s.Concurrent
	case *ast.AlterDatabaseStmt:
		return hasOption(s.Options, "tablespace")
	case *ast.AlterTableStmt:
		if s.Cmds == nil {
			return false
		}
		for _, item := range s.Cmds.Items {
			cmd, ok := item.(*ast.AlterTableCmd)
			if !ok || cmd.Subtype != ast.AT_DetachPartition {
				continue
			}
			if part, ok := cmd.Def.(*ast.PartitionCmd); ok && part.Concurrent {
				return true
			}
		}
		return false
	case *ast.CreateSubscriptionStmt:
		connect := optionBool(s.Options, "connect", true)
		return optionBool(s.Options, "create_slot", connect)
	case *ast.AlterSubscriptionStmt:
		switch s.Kind {
		case ast.ALTER_SUBSCRIPTION_REFRESH:
			return true
		case ast.ALTER_SUBSCRIPTION_SET_PUBLICATION, ast.ALTER_SUBSCRIPTION_ADD_PUBLICATION, ast.ALTER_SUBSCRIPTION_DROP_PUBLICATION:
			return optionBool(s.Options, "refresh", true)
		case ast.ALTER_SUBSCRIPTION_OPTIONS:
			return hasOption(s.Options, "failover")
		}
		return false
	}
	return false
}

// hasOption reports whether a DefElem list names option.
func hasOption(options *ast.NodeList, name string) bool {
	return findOption(options, name) != nil
}

// optionBool reads a boolean option from a DefElem list with the values
// PostgreSQL's defGetBoolean accepts: a bare option name is true, the integers
// 0 and 1, and the strings true, false, on and off in any case. A missing
// option is def; so is a value defGetBoolean would reject, which PostgreSQL
// reports as an error before the statement runs.
func optionBool(options *ast.NodeList, name string, def bool) bool {
	opt := findOption(options, name)
	if opt == nil {
		return def
	}
	switch v := opt.Arg.(type) {
	case nil:
		return true
	case *ast.Boolean:
		return v.BoolVal
	case *ast.Integer:
		switch v.IVal {
		case 0:
			return false
		case 1:
			return true
		}
	case *ast.String:
		switch strings.ToLower(v.SVal) {
		case "true", "on":
			return true
		case "false", "off":
			return false
		}
	}
	return def
}

func findOption(options *ast.NodeList, name string) *ast.DefElem {
	if options == nil {
		return nil
	}
	for _, item := range options.Items {
		if opt, ok := item.(*ast.DefElem); ok && opt.Defname == name {
			return opt
		}
	}
	return nil
}
