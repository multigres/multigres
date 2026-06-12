// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2025, Supabase, Inc
//
//	Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//	Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestTransactionStmts tests transaction control statements.
func TestTransactionStmts(t *testing.T) {
	t.Run("TransactionStmtKind", func(t *testing.T) {
		tests := []struct {
			kind     TransactionStmtKind
			expected string
		}{
			{TRANS_STMT_BEGIN, "BEGIN"},
			{TRANS_STMT_START, "START"},
			{TRANS_STMT_COMMIT, "COMMIT"},
			{TRANS_STMT_ROLLBACK, "ROLLBACK"},
			{TRANS_STMT_SAVEPOINT, "SAVEPOINT"},
			{TRANS_STMT_RELEASE, "RELEASE"},
			{TRANS_STMT_ROLLBACK_TO, "ROLLBACK_TO"},
			{TRANS_STMT_PREPARE, "PREPARE"},
			{TRANS_STMT_COMMIT_PREPARED, "COMMIT_PREPARED"},
			{TRANS_STMT_ROLLBACK_PREPARED, "ROLLBACK_PREPARED"},
		}

		for _, tt := range tests {
			assert.Equal(t, tt.expected, tt.kind.String())
		}
	})

	t.Run("BeginStmt", func(t *testing.T) {
		stmt := NewBeginStmt()

		assert.Equal(t, T_TransactionStmt, stmt.NodeTag())
		assert.Equal(t, "BEGIN", stmt.StatementType())
		assert.Equal(t, TRANS_STMT_BEGIN, stmt.Kind)
		assert.Contains(t, stmt.String(), "BEGIN")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("CommitStmt", func(t *testing.T) {
		stmt := NewCommitStmt()

		assert.Equal(t, TRANS_STMT_COMMIT, stmt.Kind)
		assert.Equal(t, "COMMIT", stmt.StatementType())
		assert.Contains(t, stmt.String(), "COMMIT")
	})

	t.Run("RollbackStmt", func(t *testing.T) {
		stmt := NewRollbackStmt()

		assert.Equal(t, TRANS_STMT_ROLLBACK, stmt.Kind)
		assert.Equal(t, "ROLLBACK", stmt.StatementType())
		assert.Contains(t, stmt.String(), "ROLLBACK")
	})

	t.Run("SavepointStmt", func(t *testing.T) {
		stmt := NewSavepointStmt("sp1")

		assert.Equal(t, TRANS_STMT_SAVEPOINT, stmt.Kind)
		assert.Equal(t, "sp1", stmt.SavepointName)
		assert.Equal(t, "SAVEPOINT", stmt.StatementType())
		assert.Contains(t, stmt.String(), "SAVEPOINT")
		assert.Contains(t, stmt.String(), "sp1")
	})

	t.Run("ReleaseStmt", func(t *testing.T) {
		stmt := NewReleaseStmt("sp1")

		assert.Equal(t, TRANS_STMT_RELEASE, stmt.Kind)
		assert.Equal(t, "sp1", stmt.SavepointName)
		assert.Equal(t, "RELEASE", stmt.StatementType())
		assert.Contains(t, stmt.String(), "RELEASE")
	})

	t.Run("RollbackToStmt", func(t *testing.T) {
		stmt := NewRollbackToStmt("sp1")

		assert.Equal(t, TRANS_STMT_ROLLBACK_TO, stmt.Kind)
		assert.Equal(t, "sp1", stmt.SavepointName)
		assert.Equal(t, "ROLLBACK_TO", stmt.StatementType())
		assert.Contains(t, stmt.String(), "ROLLBACK_TO")
	})
}

// TestGrantStmts tests GRANT/REVOKE statements.
func TestGrantStmts(t *testing.T) {
	t.Run("GrantTargetType", func(t *testing.T) {
		assert.Equal(t, "OBJECT", ACL_TARGET_OBJECT.String())
		assert.Equal(t, "ALL_IN_SCHEMA", ACL_TARGET_ALL_IN_SCHEMA.String())
		assert.Equal(t, "DEFAULTS", ACL_TARGET_DEFAULTS.String())
	})

	t.Run("AccessPriv", func(t *testing.T) {
		cols := NewNodeList(NewString("id"), NewString("name"))
		priv := NewAccessPriv("SELECT", cols)

		assert.Equal(t, T_AccessPriv, priv.NodeTag())
		assert.Equal(t, "SELECT", priv.PrivName)
		assert.Equal(t, cols, priv.Cols)
		assert.Contains(t, priv.String(), "SELECT")
		assert.Contains(t, priv.String(), "2 cols")

		// Test interface compliance
		var _ Node = priv
	})

	t.Run("GrantStmt", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		privilege := NewAccessPriv("SELECT", nil)
		grantee := NewRoleSpec(ROLESPEC_CSTRING, "alice")

		stmt := NewGrantStmt(OBJECT_TABLE, NewNodeList(relation), NewNodeList(privilege), NewNodeList(grantee))

		assert.Equal(t, T_GrantStmt, stmt.NodeTag())
		assert.Equal(t, "GRANT", stmt.StatementType())
		assert.True(t, stmt.IsGrant)
		assert.Equal(t, OBJECT_TABLE, stmt.Objtype)
		assert.Equal(t, ACL_TARGET_OBJECT, stmt.Targtype)
		assert.Equal(t, 1, stmt.Objects.Len())
		assert.Equal(t, 1, len(stmt.Privileges.Items))
		assert.Equal(t, 1, len(stmt.Grantees.Items))
		assert.Contains(t, stmt.String(), "GRANT")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("RevokeStmt", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		privilege := NewAccessPriv("INSERT", nil)
		grantee := NewRoleSpec(ROLESPEC_CSTRING, "bob")

		stmt := NewRevokeStmt(OBJECT_TABLE, NewNodeList(relation), NewNodeList(privilege), NewNodeList(grantee))

		assert.False(t, stmt.IsGrant)
		assert.Equal(t, "REVOKE", stmt.StatementType())
		assert.Contains(t, stmt.String(), "REVOKE")
	})

	t.Run("GrantRoleStmt", func(t *testing.T) {
		role := NewRoleSpec(ROLESPEC_CSTRING, "admin")
		grantee := NewRoleSpec(ROLESPEC_CSTRING, "alice")

		stmt := NewGrantRoleStmt(NewNodeList(role), NewNodeList(grantee))

		assert.Equal(t, T_GrantRoleStmt, stmt.NodeTag())
		assert.Equal(t, "GRANT_ROLE", stmt.StatementType())
		assert.True(t, stmt.IsGrant)
		assert.Equal(t, 1, len(stmt.GrantedRoles.Items))
		assert.Equal(t, 1, len(stmt.GranteeRoles.Items))
		assert.Contains(t, stmt.String(), "GRANT")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("RevokeRoleStmt", func(t *testing.T) {
		role := NewRoleSpec(ROLESPEC_CSTRING, "admin")
		grantee := NewRoleSpec(ROLESPEC_CSTRING, "alice")

		stmt := NewRevokeRoleStmt(NewNodeList(role), NewNodeList(grantee))

		assert.False(t, stmt.IsGrant)
		assert.Equal(t, "REVOKE_ROLE", stmt.StatementType())
		assert.Contains(t, stmt.String(), "REVOKE")
	})

	t.Run("AlterDefaultPrivilegesStmt", func(t *testing.T) {
		// Test with FOR ROLE and IN SCHEMA options
		roleList := NewNodeList(NewRoleSpec(ROLESPEC_CSTRING, "alice"))
		schemaList := NewNodeList(NewString("public"))
		options := NewNodeList(
			NewDefElem("roles", roleList),
			NewDefElem("schemas", schemaList),
		)

		// Create GRANT action
		privilege := NewAccessPriv("SELECT", nil)
		grantee := NewRoleSpec(ROLESPEC_CSTRING, "bob")
		action := NewGrantStmt(OBJECT_TABLE, nil, NewNodeList(privilege), NewNodeList(grantee))
		action.Targtype = ACL_TARGET_DEFAULTS

		stmt := NewAlterDefaultPrivilegesStmt(options, action)

		assert.Equal(t, T_AlterDefaultPrivilegesStmt, stmt.NodeTag())
		assert.Equal(t, "ALTER_DEFAULT_PRIVILEGES", stmt.StatementType())
		assert.Equal(t, 2, len(stmt.Options.Items))
		assert.Equal(t, action, stmt.Action)
		assert.Contains(t, stmt.String(), "GRANT")
		assert.Contains(t, stmt.String(), "2 options")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("AlterDefaultPrivilegesStmt_Revoke", func(t *testing.T) {
		// Test REVOKE action without options
		options := &NodeList{Items: []Node{}}

		// Create REVOKE action
		privilege := NewAccessPriv("INSERT", nil)
		grantee := NewRoleSpec(ROLESPEC_CSTRING, "charlie")
		action := NewRevokeStmt(OBJECT_FUNCTION, nil, NewNodeList(privilege), NewNodeList(grantee))
		action.Targtype = ACL_TARGET_DEFAULTS

		stmt := NewAlterDefaultPrivilegesStmt(options, action)

		assert.Equal(t, T_AlterDefaultPrivilegesStmt, stmt.NodeTag())
		assert.Equal(t, "ALTER_DEFAULT_PRIVILEGES", stmt.StatementType())
		assert.Equal(t, 0, len(stmt.Options.Items))
		assert.Equal(t, action, stmt.Action)
		assert.Contains(t, stmt.String(), "REVOKE")
		assert.Contains(t, stmt.String(), "0 options")
	})

	t.Run("AlterDefaultPrivilegesStmt_SqlString", func(t *testing.T) {
		// Test SQL string generation
		roleList := NewNodeList(NewString("alice"))
		schemaList := NewNodeList(NewString("public"))
		options := NewNodeList(
			NewDefElem("roles", roleList),
			NewDefElem("schemas", schemaList),
		)

		// Create GRANT ALL PRIVILEGES action
		grantee := NewRoleSpec(ROLESPEC_CSTRING, "bob")
		action := NewGrantStmt(OBJECT_TABLE, nil, nil, NewNodeList(grantee))
		action.Targtype = ACL_TARGET_DEFAULTS

		stmt := NewAlterDefaultPrivilegesStmt(options, action)
		sqlStr := stmt.SqlString()

		assert.Contains(t, sqlStr, "ALTER DEFAULT PRIVILEGES")
		assert.Contains(t, sqlStr, "FOR ROLE alice")
		assert.Contains(t, sqlStr, "IN SCHEMA public")
		assert.Contains(t, sqlStr, "GRANT ALL PRIVILEGES")
		assert.Contains(t, sqlStr, "ON TABLES")
		assert.Contains(t, sqlStr, "TO bob")
	})

	t.Run("AlterDefaultPrivilegesStmt_SqlString_Functions", func(t *testing.T) {
		// Test with FUNCTIONS target and specific privileges
		options := &NodeList{Items: []Node{}}

		privilege := NewAccessPriv("EXECUTE", nil)
		grantee := NewRoleSpec(ROLESPEC_CSTRING, "dev_role")
		action := NewGrantStmt(OBJECT_FUNCTION, nil, NewNodeList(privilege), NewNodeList(grantee))
		action.Targtype = ACL_TARGET_DEFAULTS
		action.GrantOption = true

		stmt := NewAlterDefaultPrivilegesStmt(options, action)
		sqlStr := stmt.SqlString()

		assert.Contains(t, sqlStr, "ALTER DEFAULT PRIVILEGES")
		assert.Contains(t, sqlStr, "GRANT EXECUTE")
		assert.Contains(t, sqlStr, "ON FUNCTIONS")
		assert.Contains(t, sqlStr, "TO dev_role")
		assert.Contains(t, sqlStr, "WITH GRANT OPTION")
	})
}

// TestRoleStmts tests role management statements.
func TestRoleStmts(t *testing.T) {
	t.Run("RoleStatementType", func(t *testing.T) {
		assert.Equal(t, "ROLE", ROLESTMT_ROLE.String())
		assert.Equal(t, "USER", ROLESTMT_USER.String())
		assert.Equal(t, "GROUP", ROLESTMT_GROUP.String())
	})

	t.Run("CreateRoleStmt", func(t *testing.T) {
		passwordOpt := NewDefElem("password", NewString("secret"))
		stmt := NewCreateRoleStmt(ROLESTMT_ROLE, "alice", NewNodeList(passwordOpt))

		assert.Equal(t, T_CreateRoleStmt, stmt.NodeTag())
		assert.Equal(t, "CREATE_ROLE", stmt.StatementType())
		assert.Equal(t, ROLESTMT_ROLE, stmt.StmtType)
		assert.Equal(t, "alice", stmt.Role)
		assert.Equal(t, 1, len(stmt.Options.Items))
		assert.Contains(t, stmt.String(), "alice")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("AlterRoleStmt", func(t *testing.T) {
		role := NewRoleSpec(ROLESPEC_CSTRING, "alice")
		loginOpt := NewDefElem("login", NewBoolean(true))
		stmt := NewAlterRoleStmt(role, NewNodeList(loginOpt))

		assert.Equal(t, T_AlterRoleStmt, stmt.NodeTag())
		assert.Equal(t, "ALTER_ROLE", stmt.StatementType())
		assert.Equal(t, role, stmt.Role)
		assert.Equal(t, 1, len(stmt.Options.Items))
		assert.Contains(t, stmt.String(), "alice")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("DropRoleStmt", func(t *testing.T) {
		role1 := NewRoleSpec(ROLESPEC_CSTRING, "alice")
		role2 := NewRoleSpec(ROLESPEC_CSTRING, "bob")
		stmt := NewDropRoleStmt(NewNodeList(role1, role2), true)

		assert.Equal(t, T_DropRoleStmt, stmt.NodeTag())
		assert.Equal(t, "DROP_ROLE", stmt.StatementType())
		assert.Equal(t, 2, len(stmt.Roles.Items))
		assert.True(t, stmt.MissingOk)
		assert.Contains(t, stmt.String(), "IF EXISTS")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})
}

// TestConfigurationStmts tests SET/SHOW/RESET statements.
func TestConfigurationStmts(t *testing.T) {
	t.Run("VariableSetKind", func(t *testing.T) {
		tests := []struct {
			kind     VariableSetKind
			expected string
		}{
			{VAR_SET_VALUE, "SET_VALUE"},
			{VAR_SET_DEFAULT, "SET_DEFAULT"},
			{VAR_SET_CURRENT, "SET_CURRENT"},
			{VAR_SET_MULTI, "SET_MULTI"},
			{VAR_RESET, "RESET"},
			{VAR_RESET_ALL, "RESET_ALL"},
		}

		for _, tt := range tests {
			assert.Equal(t, tt.expected, tt.kind.String())
		}
	})

	t.Run("SetStmt", func(t *testing.T) {
		value := NewString("off")
		stmt := NewSetStmt("autocommit", NewNodeList(value))

		assert.Equal(t, T_VariableSetStmt, stmt.NodeTag())
		assert.Equal(t, "SET", stmt.StatementType())
		assert.Equal(t, VAR_SET_VALUE, stmt.Kind)
		assert.Equal(t, "autocommit", stmt.Name)
		assert.Equal(t, 1, stmt.Args.Len())
		assert.False(t, stmt.IsLocal)
		assert.Contains(t, stmt.String(), "autocommit")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("LocalSetStmt", func(t *testing.T) {
		value := NewString("on")
		stmt := NewVariableSetStmt(VAR_SET_VALUE, "log_statement", NewNodeList(value), true)

		assert.True(t, stmt.IsLocal)
		assert.Contains(t, stmt.String(), "LOCAL")
	})

	t.Run("ResetStmt", func(t *testing.T) {
		stmt := NewResetStmt("timezone")

		assert.Equal(t, VAR_RESET, stmt.Kind)
		assert.Equal(t, "RESET", stmt.StatementType())
		assert.Equal(t, "timezone", stmt.Name)
		assert.Nil(t, stmt.Args)
		assert.Contains(t, stmt.String(), "timezone")
	})

	t.Run("ShowStmt", func(t *testing.T) {
		stmt := NewVariableShowStmt("timezone")

		assert.Equal(t, T_VariableShowStmt, stmt.NodeTag())
		assert.Equal(t, "SHOW", stmt.StatementType())
		assert.Equal(t, "timezone", stmt.Name)
		assert.Contains(t, stmt.String(), "timezone")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})
}

// TestQueryAnalysisStmts tests EXPLAIN/PREPARE/EXECUTE statements.
func TestQueryAnalysisStmts(t *testing.T) {
	t.Run("ExplainStmt", func(t *testing.T) {
		query := NewSelectStmt()
		analyzeOpt := NewDefElem("analyze", NewBoolean(true))
		stmt := NewExplainStmt(query, NewNodeList(analyzeOpt))

		assert.Equal(t, T_ExplainStmt, stmt.NodeTag())
		assert.Equal(t, "EXPLAIN", stmt.StatementType())
		assert.Equal(t, query, stmt.Query)
		assert.Equal(t, 1, stmt.Options.Len())
		assert.Contains(t, stmt.String(), "1 options")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("PrepareStmt", func(t *testing.T) {
		query := NewSelectStmt()
		argtype := NewTypeName([]string{"integer"})
		argtypes := NewNodeList()
		argtypes.Append(argtype)
		stmt := NewPrepareStmt("get_user", argtypes, query)

		assert.Equal(t, T_PrepareStmt, stmt.NodeTag())
		assert.Equal(t, "PREPARE", stmt.StatementType())
		assert.Equal(t, "get_user", stmt.Name)
		assert.Equal(t, 1, stmt.Argtypes.Len())
		assert.Equal(t, query, stmt.Query)
		assert.Contains(t, stmt.String(), "get_user")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("ExecuteStmt", func(t *testing.T) {
		param := NewInteger(123)
		stmt := NewExecuteStmt("get_user", NewNodeList(param))

		assert.Equal(t, T_ExecuteStmt, stmt.NodeTag())
		assert.Equal(t, "EXECUTE", stmt.StatementType())
		assert.Equal(t, "get_user", stmt.Name)
		assert.Equal(t, 1, stmt.Params.Len())
		assert.Contains(t, stmt.String(), "get_user")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("DeallocateStmt", func(t *testing.T) {
		stmt := NewDeallocateStmt("get_user")

		assert.Equal(t, T_DeallocateStmt, stmt.NodeTag())
		assert.Equal(t, "DEALLOCATE", stmt.StatementType())
		assert.Equal(t, "get_user", stmt.Name)
		assert.Contains(t, stmt.String(), "get_user")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("DeallocateAllStmt", func(t *testing.T) {
		stmt := NewDeallocateAllStmt()

		assert.Equal(t, "", stmt.Name)
		assert.Contains(t, stmt.String(), "ALL")
	})
}

// TestCopyStmts tests COPY statements.
func TestCopyStmts(t *testing.T) {
	t.Run("CopyFromStmt", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		stmt := NewCopyFromStmt(relation, "/tmp/users.csv")

		assert.Equal(t, T_CopyStmt, stmt.NodeTag())
		assert.Equal(t, "COPY", stmt.StatementType())
		assert.Equal(t, relation, stmt.Relation)
		assert.True(t, stmt.IsFrom)
		assert.Equal(t, "/tmp/users.csv", stmt.Filename)
		assert.Contains(t, stmt.String(), "users")
		assert.Contains(t, stmt.String(), "FROM")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("CopyToStmt", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		stmt := NewCopyToStmt(relation, "/tmp/users_out.csv")

		assert.False(t, stmt.IsFrom)
		assert.Contains(t, stmt.String(), "TO")
	})

	t.Run("CopyWithQuery", func(t *testing.T) {
		query := NewSelectStmt()
		stmt := NewCopyStmt(nil, query, false)

		assert.Nil(t, stmt.Relation)
		assert.Equal(t, query, stmt.Query)
		assert.Contains(t, stmt.String(), "query")
	})
}

// TestMaintenanceStmts tests maintenance statements.
func TestMaintenanceStmts(t *testing.T) {
	t.Run("VacuumRelation", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		colList := NewNodeList(NewString("name"), NewString("email"))
		vr := NewVacuumRelation(relation, colList)

		assert.Equal(t, T_VacuumRelation, vr.NodeTag())
		assert.Equal(t, relation, vr.Relation)
		assert.Equal(t, 2, vr.VaCols.Len())
		assert.Contains(t, vr.String(), "users")

		// Test interface compliance
		var _ Node = vr
	})

	t.Run("VacuumStmt", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		vr := NewVacuumRelation(relation, nil)
		verboseOpt := NewDefElem("verbose", NewBoolean(true))
		options := NewNodeList(verboseOpt)
		rels := NewNodeList(vr)
		stmt := NewVacuumStmt(options, rels)

		assert.Equal(t, T_VacuumStmt, stmt.NodeTag())
		assert.Equal(t, "VACUUM", stmt.StatementType())
		assert.True(t, stmt.IsVacuumcmd)
		assert.Equal(t, 1, stmt.Options.Len())
		assert.Equal(t, 1, stmt.Rels.Len())
		assert.Contains(t, stmt.String(), "VACUUM")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("AnalyzeStmt", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		vr := NewVacuumRelation(relation, nil)
		rels := NewNodeList(vr)
		stmt := NewAnalyzeStmt(nil, rels)

		assert.False(t, stmt.IsVacuumcmd)
		assert.Equal(t, "ANALYZE", stmt.StatementType())
		assert.Contains(t, stmt.String(), "ANALYZE")
	})

	t.Run("ReindexObjectType", func(t *testing.T) {
		tests := []struct {
			objType  ReindexObjectType
			expected string
		}{
			{REINDEX_OBJECT_INDEX, "INDEX"},
			{REINDEX_OBJECT_TABLE, "TABLE"},
			{REINDEX_OBJECT_SCHEMA, "SCHEMA"},
			{REINDEX_OBJECT_SYSTEM, "SYSTEM"},
			{REINDEX_OBJECT_DATABASE, "DATABASE"},
		}

		for _, tt := range tests {
			assert.Equal(t, tt.expected, tt.objType.String())
		}
	})

	t.Run("ReindexIndexStmt", func(t *testing.T) {
		relation := NewRangeVar("idx_users_email", "", "")
		stmt := NewReindexIndexStmt(relation)

		assert.Equal(t, T_ReindexStmt, stmt.NodeTag())
		assert.Equal(t, "REINDEX", stmt.StatementType())
		assert.Equal(t, REINDEX_OBJECT_INDEX, stmt.Kind)
		assert.Equal(t, relation, stmt.Relation)
		assert.Contains(t, stmt.String(), "INDEX")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("ReindexTableStmt", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		stmt := NewReindexTableStmt(relation)

		assert.Equal(t, REINDEX_OBJECT_TABLE, stmt.Kind)
		assert.Contains(t, stmt.String(), "TABLE")
	})

	t.Run("ReindexDatabaseStmt", func(t *testing.T) {
		stmt := NewReindexDatabaseStmt("mydb")

		assert.Equal(t, REINDEX_OBJECT_DATABASE, stmt.Kind)
		assert.Equal(t, "mydb", stmt.Name)
		assert.Nil(t, stmt.Relation)
		assert.Contains(t, stmt.String(), "DATABASE")
	})

	t.Run("ClusterStmt", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		stmt := NewClusterStmt(relation, "idx_users_id", nil)

		assert.Equal(t, T_ClusterStmt, stmt.NodeTag())
		assert.Equal(t, "CLUSTER", stmt.StatementType())
		assert.Equal(t, relation, stmt.Relation)
		assert.Equal(t, "idx_users_id", stmt.Indexname)
		assert.Contains(t, stmt.String(), "users")
		assert.Contains(t, stmt.String(), "idx_users_id")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})
}

// TestAdministrativeStmts tests administrative statements.
func TestAdministrativeStmts(t *testing.T) {
	t.Run("CheckPointStmt", func(t *testing.T) {
		stmt := NewCheckPointStmt()

		assert.Equal(t, T_CheckPointStmt, stmt.NodeTag())
		assert.Equal(t, "CHECKPOINT", stmt.StatementType())
		assert.Contains(t, stmt.String(), "CheckPointStmt")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("DiscardMode", func(t *testing.T) {
		tests := []struct {
			mode     DiscardMode
			expected string
		}{
			{DISCARD_ALL, "ALL"},
			{DISCARD_PLANS, "PLANS"},
			{DISCARD_SEQUENCES, "SEQUENCES"},
			{DISCARD_TEMP, "TEMP"},
		}

		for _, tt := range tests {
			assert.Equal(t, tt.expected, tt.mode.String())
		}
	})

	t.Run("DiscardStmt", func(t *testing.T) {
		stmt := NewDiscardStmt(DISCARD_ALL)

		assert.Equal(t, T_DiscardStmt, stmt.NodeTag())
		assert.Equal(t, "DISCARD", stmt.StatementType())
		assert.Equal(t, DISCARD_ALL, stmt.Target)
		assert.Contains(t, stmt.String(), "ALL")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("LoadStmt", func(t *testing.T) {
		stmt := NewLoadStmt("module_name")

		assert.Equal(t, T_LoadStmt, stmt.NodeTag())
		assert.Equal(t, "LOAD", stmt.StatementType())
		assert.Equal(t, "module_name", stmt.Filename)
		assert.Contains(t, stmt.String(), "module_name")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("NotifyStmt", func(t *testing.T) {
		stmt := NewNotifyStmt("channel1", "message payload")

		assert.Equal(t, T_NotifyStmt, stmt.NodeTag())
		assert.Equal(t, "NOTIFY", stmt.StatementType())
		assert.Equal(t, "channel1", stmt.Conditionname)
		assert.Equal(t, "message payload", stmt.Payload)
		assert.Contains(t, stmt.String(), "channel1")
		assert.Contains(t, stmt.String(), "message payload")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("NotifyStmtWithoutPayload", func(t *testing.T) {
		stmt := NewNotifyStmt("channel1", "")

		assert.Equal(t, "", stmt.Payload)
		assert.NotContains(t, stmt.String(), "''")
	})

	t.Run("ListenStmt", func(t *testing.T) {
		stmt := NewListenStmt("channel1")

		assert.Equal(t, T_ListenStmt, stmt.NodeTag())
		assert.Equal(t, "LISTEN", stmt.StatementType())
		assert.Equal(t, "channel1", stmt.Conditionname)
		assert.Contains(t, stmt.String(), "channel1")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("UnlistenStmt", func(t *testing.T) {
		stmt := NewUnlistenStmt("channel1")

		assert.Equal(t, T_UnlistenStmt, stmt.NodeTag())
		assert.Equal(t, "UNLISTEN", stmt.StatementType())
		assert.Equal(t, "channel1", stmt.Conditionname)
		assert.Contains(t, stmt.String(), "channel1")

		// Test interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("UnlistenAllStmt", func(t *testing.T) {
		stmt := NewUnlistenAllStmt()

		assert.Equal(t, "*", stmt.Conditionname)
		assert.Contains(t, stmt.String(), "*")
	})
}

// TestUtilityComplexExamples tests complex utility statement examples.
func TestUtilityComplexExamples(t *testing.T) {
	t.Run("CompleteTransaction", func(t *testing.T) {
		// BEGIN; SAVEPOINT sp1; ROLLBACK TO sp1; RELEASE sp1; COMMIT;

		beginStmt := NewBeginStmt()
		savepointStmt := NewSavepointStmt("sp1")
		rollbackToStmt := NewRollbackToStmt("sp1")
		releaseStmt := NewReleaseStmt("sp1")
		commitStmt := NewCommitStmt()

		statements := []Stmt{beginStmt, savepointStmt, rollbackToStmt, releaseStmt, commitStmt}

		for _, stmt := range statements {
			assert.Equal(t, T_TransactionStmt, stmt.NodeTag())
			// All should be valid transaction statements
			assert.Contains(t, []string{"BEGIN", "SAVEPOINT", "ROLLBACK_TO", "RELEASE", "COMMIT"}, stmt.StatementType())
		}
	})

	t.Run("CompleteRoleManagement", func(t *testing.T) {
		// CREATE ROLE alice WITH PASSWORD 'secret' LOGIN;
		// GRANT SELECT ON users TO alice;
		// ALTER ROLE alice SET timezone = 'UTC';
		// DROP ROLE alice;

		// Create role
		passwordOpt := NewDefElem("password", NewString("secret"))
		loginOpt := NewDefElem("login", NewBoolean(true))
		createStmt := NewCreateRoleStmt(ROLESTMT_ROLE, "alice", NewNodeList(passwordOpt, loginOpt))

		// Grant privilege
		relation := NewRangeVar("users", "", "")
		privilege := NewAccessPriv("SELECT", nil)
		grantee := NewRoleSpec(ROLESPEC_CSTRING, "alice")
		grantStmt := NewGrantStmt(OBJECT_TABLE, NewNodeList(relation), NewNodeList(privilege), NewNodeList(grantee))

		// Alter role
		role := NewRoleSpec(ROLESPEC_CSTRING, "alice")
		alterStmt := NewAlterRoleStmt(role, NewNodeList())

		// Drop role
		dropStmt := NewDropRoleStmt(NewNodeList(role), false)

		assert.Equal(t, "alice", createStmt.Role)
		assert.Equal(t, 2, len(createStmt.Options.Items))
		assert.True(t, grantStmt.IsGrant)
		assert.Equal(t, "alice", alterStmt.Role.Rolename)
		assert.Equal(t, 1, len(dropStmt.Roles.Items))
	})

	t.Run("ComplexVacuum", func(t *testing.T) {
		// VACUUM (VERBOSE, ANALYZE) users (name, email);

		relation := NewRangeVar("users", "", "")
		colList := NewNodeList(NewString("name"), NewString("email"))
		vr := NewVacuumRelation(relation, colList)

		verboseOpt := NewDefElem("verbose", NewBoolean(true))
		analyzeOpt := NewDefElem("analyze", NewBoolean(true))
		options := NewNodeList(verboseOpt, analyzeOpt)
		rels := NewNodeList(vr)

		stmt := NewVacuumStmt(options, rels)

		assert.True(t, stmt.IsVacuumcmd)
		assert.Equal(t, 2, stmt.Options.Len())
		assert.Equal(t, 1, stmt.Rels.Len())

		// Check that the first relation has the expected columns
		firstRel := stmt.Rels.Items[0].(*VacuumRelation)
		assert.Equal(t, 2, firstRel.VaCols.Len())
		assert.Contains(t, stmt.String(), "VACUUM")
	})

	t.Run("PreparedStmtWorkflow", func(t *testing.T) {
		// PREPARE get_user (integer) AS SELECT * FROM users WHERE id = $1;
		// EXECUTE get_user (123);
		// DEALLOCATE get_user;

		query := NewSelectStmt()
		argtype := NewTypeName([]string{"integer"})
		argtypes := NewNodeList()
		argtypes.Append(argtype)
		prepareStmt := NewPrepareStmt("get_user", argtypes, query)

		param := NewInteger(123)
		executeStmt := NewExecuteStmt("get_user", NewNodeList(param))

		deallocateStmt := NewDeallocateStmt("get_user")

		assert.Equal(t, "get_user", prepareStmt.Name)
		assert.Equal(t, 1, prepareStmt.Argtypes.Len())
		assert.Equal(t, "get_user", executeStmt.Name)
		assert.Equal(t, 1, executeStmt.Params.Len())
		assert.Equal(t, "get_user", deallocateStmt.Name)
	})
}
