// Package ast provides PostgreSQL DDL statement node tests.
package ast

import (
	"testing"
	
	"github.com/stretchr/testify/assert"
)

// TestDDLSupportingTypes tests the supporting types for DDL statements.
func TestDDLSupportingTypes(t *testing.T) {
	t.Run("ObjectType", func(t *testing.T) {
		tests := []struct {
			objType  ObjectType
			expected string
		}{
			{OBJECT_TABLE, "TABLE"},
			{OBJECT_INDEX, "INDEX"},
			{OBJECT_VIEW, "VIEW"},
			{OBJECT_SCHEMA, "SCHEMA"},
			{OBJECT_DATABASE, "DATABASE"},
			{OBJECT_FUNCTION, "FUNCTION"},
		}

		for _, tt := range tests {
			assert.Equal(t, tt.expected, tt.objType.String())
		}
	})

	t.Run("DropBehavior", func(t *testing.T) {
		assert.Equal(t, "RESTRICT", DROP_RESTRICT.String())
		assert.Equal(t, "CASCADE", DROP_CASCADE.String())
	})

	t.Run("ConstrType", func(t *testing.T) {
		tests := []struct {
			constrType ConstrType
			expected   string
		}{
			{CONSTR_PRIMARY, "PRIMARY_KEY"},
			{CONSTR_FOREIGN, "FOREIGN_KEY"},
			{CONSTR_UNIQUE, "UNIQUE"},
			{CONSTR_CHECK, "CHECK"},
			{CONSTR_NOTNULL, "NOT_NULL"},
		}

		for _, tt := range tests {
			assert.Equal(t, tt.expected, tt.constrType.String())
		}
	})

	t.Run("ViewCheckOption", func(t *testing.T) {
		assert.Equal(t, "NO_CHECK", NO_CHECK_OPTION.String())
		assert.Equal(t, "LOCAL", LOCAL_CHECK_OPTION.String())
		assert.Equal(t, "CASCADED", CASCADED_CHECK_OPTION.String())
	})

	t.Run("AlterTableType", func(t *testing.T) {
		tests := []struct {
			alterType AlterTableType
			expected  string
		}{
			{AT_AddColumn, "ADD_COLUMN"},
			{AT_DropColumn, "DROP_COLUMN"},
			{AT_AddConstraint, "ADD_CONSTRAINT"},
			{AT_DropConstraint, "DROP_CONSTRAINT"},
			{AT_AlterColumnType, "ALTER_COLUMN_TYPE"},
		}

		for _, tt := range tests {
			assert.Equal(t, tt.expected, tt.alterType.String())
		}
	})

	t.Run("SortByDir", func(t *testing.T) {
		assert.Equal(t, "DEFAULT", SORTBY_DEFAULT.String())
		assert.Equal(t, "ASC", SORTBY_ASC.String())
		assert.Equal(t, "DESC", SORTBY_DESC.String())
	})

	t.Run("SortByNulls", func(t *testing.T) {
		assert.Equal(t, "DEFAULT", SORTBY_NULLS_DEFAULT.String())
		assert.Equal(t, "NULLS_FIRST", SORTBY_NULLS_FIRST.String())
		assert.Equal(t, "NULLS_LAST", SORTBY_NULLS_LAST.String())
	})

	t.Run("RoleSpecType", func(t *testing.T) {
		tests := []struct {
			roleType RoleSpecType
			expected string
		}{
			{ROLESPEC_CSTRING, "CSTRING"},
			{ROLESPEC_CURRENT_USER, "CURRENT_USER"},
			{ROLESPEC_SESSION_USER, "SESSION_USER"},
			{ROLESPEC_PUBLIC, "PUBLIC"},
		}

		for _, tt := range tests {
			assert.Equal(t, tt.expected, tt.roleType.String())
		}
	})
}

// TestRoleSpec tests the RoleSpec node.
func TestRoleSpec(t *testing.T) {
	t.Run("NamedRole", func(t *testing.T) {
		roleSpec := NewRoleSpec(ROLESPEC_CSTRING, "postgres")

		assert.Equal(t, T_RoleSpec, roleSpec.NodeTag())
		assert.Equal(t, ROLESPEC_CSTRING, roleSpec.Roletype)
		assert.Equal(t, "postgres", roleSpec.Rolename)
		assert.Contains(t, roleSpec.String(), "postgres")

		// Test interface compliance
		var _ Node = roleSpec
	})

	t.Run("CurrentUser", func(t *testing.T) {
		roleSpec := NewRoleSpec(ROLESPEC_CURRENT_USER, "")

		assert.Equal(t, ROLESPEC_CURRENT_USER, roleSpec.Roletype)
		assert.Equal(t, "", roleSpec.Rolename)
		assert.Contains(t, roleSpec.String(), "CURRENT_USER")
	})
}

// TestDefElem tests the DefElem node.
func TestDefElem(t *testing.T) {
	t.Run("BasicDefElem", func(t *testing.T) {
		value := NewString("btree")
		defElem := NewDefElem("access_method", value)

		assert.Equal(t, T_DefElem, defElem.NodeTag())
		assert.Equal(t, "access_method", defElem.Defname)
		assert.Equal(t, value, defElem.Arg)
		assert.Equal(t, DEFELEM_UNSPEC, defElem.Defaction)
		assert.Contains(t, defElem.String(), "access_method")

		// Test interface compliance
		var _ Node = defElem
	})

	t.Run("DefElemWithAction", func(t *testing.T) {
		value := NewInteger(100)
		defElem := NewDefElem("fillfactor", value)
		defElem.Defaction = DEFELEM_SET

		assert.Equal(t, DEFELEM_SET, defElem.Defaction)
		assert.Contains(t, defElem.String(), "SET")
	})
}

// TestConstraint tests the Constraint node.
func TestConstraint(t *testing.T) {
	t.Run("PrimaryKeyConstraint", func(t *testing.T) {
		constraint := NewPrimaryKeyConstraint("pk_users", []string{"id"})

		assert.Equal(t, T_Constraint, constraint.NodeTag())
		assert.Equal(t, CONSTR_PRIMARY, constraint.Contype)
		assert.Equal(t, "pk_users", constraint.Conname)
		assert.Equal(t, []string{"id"}, constraint.Keys)
		assert.Contains(t, constraint.String(), "PRIMARY_KEY")
		assert.Contains(t, constraint.String(), "pk_users")

		// Test interface compliance
		var _ Node = constraint
	})

	t.Run("ForeignKeyConstraint", func(t *testing.T) {
		pktable := NewRangeVar("departments", nil, nil)
		constraint := NewForeignKeyConstraint("fk_dept", []string{"dept_id"}, pktable, []string{"id"})

		assert.Equal(t, CONSTR_FOREIGN, constraint.Contype)
		assert.Equal(t, "fk_dept", constraint.Conname)
		assert.Equal(t, []string{"dept_id"}, constraint.FkAttrs)
		assert.Equal(t, pktable, constraint.Pktable)
		assert.Equal(t, []string{"id"}, constraint.PkAttrs)
		assert.Contains(t, constraint.String(), "FOREIGN_KEY")
	})

	t.Run("UniqueConstraint", func(t *testing.T) {
		constraint := NewUniqueConstraint("uk_email", []string{"email"})

		assert.Equal(t, CONSTR_UNIQUE, constraint.Contype)
		assert.Equal(t, "uk_email", constraint.Conname)
		assert.Equal(t, []string{"email"}, constraint.Keys)
		assert.Contains(t, constraint.String(), "UNIQUE")
	})

	t.Run("CheckConstraint", func(t *testing.T) {
		checkExpr := NewBinaryOp(521, NewVar(1, 1, 23), NewConst(23, 0, false)) // age > 0
		constraint := NewCheckConstraint("chk_age", checkExpr)

		assert.Equal(t, CONSTR_CHECK, constraint.Contype)
		assert.Equal(t, "chk_age", constraint.Conname)
		assert.Equal(t, checkExpr, constraint.RawExpr)
		assert.Contains(t, constraint.String(), "CHECK")
	})

	t.Run("NotNullConstraint", func(t *testing.T) {
		constraint := NewNotNullConstraint("nn_name")

		assert.Equal(t, CONSTR_NOTNULL, constraint.Contype)
		assert.Equal(t, "nn_name", constraint.Conname)
		assert.Contains(t, constraint.String(), "NOT_NULL")
	})
}

// TestAlterTableStmt tests the AlterTableStmt node.
func TestAlterTableStmt(t *testing.T) {
	t.Run("AddColumn", func(t *testing.T) {
		relation := NewRangeVar("users", nil, nil)
		columnDef := NewString("VARCHAR(100)") // Simplified column definition
		addColumnCmd := NewAddColumnCmd("email", columnDef)
		
		alterStmt := NewAlterTableStmt(relation, []*AlterTableCmd{addColumnCmd})

		assert.Equal(t, T_AlterTableStmt, alterStmt.NodeTag())
		assert.Equal(t, "AlterTableStmt", alterStmt.StatementType())
		assert.Equal(t, relation, alterStmt.Relation)
		assert.Len(t, alterStmt.Cmds, 1)
		assert.Equal(t, OBJECT_TABLE, alterStmt.Objtype)
		assert.Contains(t, alterStmt.String(), "users")
		assert.Contains(t, alterStmt.String(), "1 cmds")

		// Test interface compliance
		var _ Node = alterStmt
		var _ Statement = alterStmt
		
		// Test the command
		cmd := alterStmt.Cmds[0]
		assert.Equal(t, T_AlterTableCmd, cmd.NodeTag())
		assert.Equal(t, AT_AddColumn, cmd.Subtype)
		assert.Equal(t, "email", cmd.Name)
		assert.Equal(t, columnDef, cmd.Def)
		assert.Contains(t, cmd.String(), "ADD_COLUMN")
		assert.Contains(t, cmd.String(), "email")
	})

	t.Run("DropColumn", func(t *testing.T) {
		relation := NewRangeVar("users", nil, nil)
		dropColumnCmd := NewDropColumnCmd("old_field", DROP_CASCADE)
		
		alterStmt := NewAlterTableStmt(relation, []*AlterTableCmd{dropColumnCmd})

		cmd := alterStmt.Cmds[0]
		assert.Equal(t, AT_DropColumn, cmd.Subtype)
		assert.Equal(t, "old_field", cmd.Name)
		assert.Equal(t, DROP_CASCADE, cmd.Behavior)
		assert.Contains(t, cmd.String(), "DROP_COLUMN")
	})

	t.Run("AddConstraint", func(t *testing.T) {
		relation := NewRangeVar("users", nil, nil)
		constraint := NewPrimaryKeyConstraint("pk_users", []string{"id"})
		addConstraintCmd := NewAddConstraintCmd(constraint)
		
		alterStmt := NewAlterTableStmt(relation, []*AlterTableCmd{addConstraintCmd})

		cmd := alterStmt.Cmds[0]
		assert.Equal(t, AT_AddConstraint, cmd.Subtype)
		assert.Equal(t, "pk_users", cmd.Name)
		assert.Equal(t, constraint, cmd.Def)
		assert.Contains(t, cmd.String(), "ADD_CONSTRAINT")
	})

	t.Run("DropConstraint", func(t *testing.T) {
		relation := NewRangeVar("users", nil, nil)
		dropConstraintCmd := NewDropConstraintCmd("old_constraint", DROP_RESTRICT)
		
		alterStmt := NewAlterTableStmt(relation, []*AlterTableCmd{dropConstraintCmd})

		cmd := alterStmt.Cmds[0]
		assert.Equal(t, AT_DropConstraint, cmd.Subtype)
		assert.Equal(t, "old_constraint", cmd.Name)
		assert.Equal(t, DROP_RESTRICT, cmd.Behavior)
		assert.Contains(t, cmd.String(), "DROP_CONSTRAINT")
	})
}

// TestIndexStmt tests the IndexStmt node.
func TestIndexStmt(t *testing.T) {
	t.Run("BasicIndex", func(t *testing.T) {
		relation := NewRangeVar("users", nil, nil)
		idxElem := NewIndexElem("email")
		indexStmt := NewIndexStmt("idx_users_email", relation, []*IndexElem{idxElem})

		assert.Equal(t, T_IndexStmt, indexStmt.NodeTag())
		assert.Equal(t, "IndexStmt", indexStmt.StatementType())
		assert.Equal(t, "idx_users_email", indexStmt.Idxname)
		assert.Equal(t, relation, indexStmt.Relation)
		assert.Equal(t, "btree", indexStmt.AccessMethod)
		assert.Len(t, indexStmt.IndexParams, 1)
		assert.False(t, indexStmt.Unique)
		assert.Contains(t, indexStmt.String(), "idx_users_email")
		assert.Contains(t, indexStmt.String(), "users")

		// Test interface compliance
		var _ Node = indexStmt
		var _ Statement = indexStmt
	})

	t.Run("UniqueIndex", func(t *testing.T) {
		relation := NewRangeVar("users", nil, nil)
		idxElem := NewIndexElem("email")
		indexStmt := NewUniqueIndex("uk_users_email", relation, []*IndexElem{idxElem})

		assert.True(t, indexStmt.Unique)
		assert.Contains(t, indexStmt.String(), "UNIQUE")
	})

	t.Run("CompositeIndex", func(t *testing.T) {
		relation := NewRangeVar("orders", nil, nil)
		idxElem1 := NewIndexElem("customer_id")
		idxElem2 := NewDescIndexElem("order_date")
		indexStmt := NewIndexStmt("idx_orders_customer_date", relation, []*IndexElem{idxElem1, idxElem2})

		assert.Len(t, indexStmt.IndexParams, 2)
		
		// Test first element
		elem1 := indexStmt.IndexParams[0]
		assert.Equal(t, "customer_id", elem1.Name)
		assert.Equal(t, SORTBY_DEFAULT, elem1.Ordering)
		
		// Test second element (descending)
		elem2 := indexStmt.IndexParams[1]
		assert.Equal(t, "order_date", elem2.Name)
		assert.Equal(t, SORTBY_DESC, elem2.Ordering)
		assert.Contains(t, elem2.String(), "DESC")
	})

	t.Run("ExpressionIndex", func(t *testing.T) {
		relation := NewRangeVar("users", nil, nil)
		lowerExpr := NewFuncExpr(870, 25, []Node{NewVar(1, 1, 25)}) // lower(email)
		idxElem := NewExpressionIndexElem(lowerExpr)
		indexStmt := NewIndexStmt("idx_users_lower_email", relation, []*IndexElem{idxElem})

		elem := indexStmt.IndexParams[0]
		assert.Equal(t, "", elem.Name) // No column name for expression index
		assert.Equal(t, lowerExpr, elem.Expr)
		assert.Contains(t, elem.String(), "expr")
	})
}

// TestIndexElem tests the IndexElem node.
func TestIndexElem(t *testing.T) {
	t.Run("BasicIndexElem", func(t *testing.T) {
		idxElem := NewIndexElem("username")

		assert.Equal(t, T_IndexElem, idxElem.NodeTag())
		assert.Equal(t, "username", idxElem.Name)
		assert.Equal(t, SORTBY_DEFAULT, idxElem.Ordering)
		assert.Equal(t, SORTBY_NULLS_DEFAULT, idxElem.NullsOrdering)
		assert.Contains(t, idxElem.String(), "username")

		// Test interface compliance
		var _ Node = idxElem
	})

	t.Run("DescendingIndexElem", func(t *testing.T) {
		idxElem := NewDescIndexElem("created_at")

		assert.Equal(t, "created_at", idxElem.Name)
		assert.Equal(t, SORTBY_DESC, idxElem.Ordering)
		assert.Contains(t, idxElem.String(), "DESC")
	})

	t.Run("IndexElemWithNullsFirst", func(t *testing.T) {
		idxElem := NewIndexElem("priority")
		idxElem.NullsOrdering = SORTBY_NULLS_FIRST

		assert.Equal(t, SORTBY_NULLS_FIRST, idxElem.NullsOrdering)
	})
}

// TestViewStmt tests the ViewStmt node.
func TestViewStmt(t *testing.T) {
	t.Run("CreateView", func(t *testing.T) {
		view := NewRangeVar("user_summary", nil, nil)
		query := NewString("SELECT id, name FROM users") // Simplified query representation
		viewStmt := NewViewStmt(view, query, false)

		assert.Equal(t, T_ViewStmt, viewStmt.NodeTag())
		assert.Equal(t, "ViewStmt", viewStmt.StatementType())
		assert.Equal(t, view, viewStmt.View)
		assert.Equal(t, query, viewStmt.Query)
		assert.False(t, viewStmt.Replace)
		assert.Equal(t, NO_CHECK_OPTION, viewStmt.WithCheckOption)
		assert.Contains(t, viewStmt.String(), "user_summary")
		assert.NotContains(t, viewStmt.String(), "OR REPLACE")

		// Test interface compliance
		var _ Node = viewStmt
		var _ Statement = viewStmt
	})

	t.Run("CreateOrReplaceView", func(t *testing.T) {
		view := NewRangeVar("user_summary", nil, nil)
		query := NewString("SELECT id, name, email FROM users")
		viewStmt := NewViewStmt(view, query, true)

		assert.True(t, viewStmt.Replace)
		assert.Contains(t, viewStmt.String(), "OR REPLACE")
	})

	t.Run("ViewWithCheckOption", func(t *testing.T) {
		view := NewRangeVar("active_users", nil, nil)
		query := NewString("SELECT * FROM users WHERE active = true")
		viewStmt := NewViewStmt(view, query, false)
		viewStmt.WithCheckOption = LOCAL_CHECK_OPTION

		assert.Equal(t, LOCAL_CHECK_OPTION, viewStmt.WithCheckOption)
	})

	t.Run("ViewWithAliases", func(t *testing.T) {
		view := NewRangeVar("user_info", nil, nil)
		query := NewString("SELECT id, name FROM users")
		viewStmt := NewViewStmt(view, query, false)
		viewStmt.Aliases = []string{"user_id", "full_name"}

		assert.Equal(t, []string{"user_id", "full_name"}, viewStmt.Aliases)
	})
}

// TestDomainStmts tests domain-related statements.
func TestDomainStmts(t *testing.T) {
	t.Run("CreateDomainStmt", func(t *testing.T) {
		domainname := []string{"public", "email_domain"}
		typeName := NewTypeName([]string{"varchar"})
		createDomainStmt := NewCreateDomainStmt(domainname, typeName)

		assert.Equal(t, T_CreateDomainStmt, createDomainStmt.NodeTag())
		assert.Equal(t, "CreateDomainStmt", createDomainStmt.StatementType())
		assert.Equal(t, domainname, createDomainStmt.Domainname)
		assert.Equal(t, typeName, createDomainStmt.TypeName)
		assert.Contains(t, createDomainStmt.String(), "email_domain")

		// Test interface compliance
		var _ Node = createDomainStmt
		var _ Statement = createDomainStmt
	})

	t.Run("AlterDomainStmt", func(t *testing.T) {
		typeName := []string{"public", "email_domain"}
		alterDomainStmt := NewAlterDomainStmt('T', typeName) // 'T' for alter default

		assert.Equal(t, T_AlterDomainStmt, alterDomainStmt.NodeTag())
		assert.Equal(t, "AlterDomainStmt", alterDomainStmt.StatementType())
		assert.Equal(t, byte('T'), alterDomainStmt.Subtype)
		assert.Equal(t, typeName, alterDomainStmt.TypeName)
		assert.Equal(t, DROP_RESTRICT, alterDomainStmt.Behavior)
		assert.Contains(t, alterDomainStmt.String(), "email_domain")

		// Test interface compliance
		var _ Node = alterDomainStmt
		var _ Statement = alterDomainStmt
	})
}

// TestSchemaStmt tests the CreateSchemaStmt node.
func TestSchemaStmt(t *testing.T) {
	t.Run("CreateSchema", func(t *testing.T) {
		createSchemaStmt := NewCreateSchemaStmt("analytics", false)

		assert.Equal(t, T_CreateSchemaStmt, createSchemaStmt.NodeTag())
		assert.Equal(t, "CreateSchemaStmt", createSchemaStmt.StatementType())
		assert.Equal(t, "analytics", createSchemaStmt.Schemaname)
		assert.False(t, createSchemaStmt.IfNotExists)
		assert.Contains(t, createSchemaStmt.String(), "analytics")
		assert.NotContains(t, createSchemaStmt.String(), "IF NOT EXISTS")

		// Test interface compliance
		var _ Node = createSchemaStmt
		var _ Statement = createSchemaStmt
	})

	t.Run("CreateSchemaIfNotExists", func(t *testing.T) {
		createSchemaStmt := NewCreateSchemaStmt("temp_schema", true)

		assert.True(t, createSchemaStmt.IfNotExists)
		assert.Contains(t, createSchemaStmt.String(), "IF NOT EXISTS")
	})

	t.Run("CreateSchemaWithOwner", func(t *testing.T) {
		createSchemaStmt := NewCreateSchemaStmt("private_schema", false)
		owner := NewRoleSpec(ROLESPEC_CSTRING, "alice")
		createSchemaStmt.Authrole = owner

		assert.Equal(t, owner, createSchemaStmt.Authrole)
	})
}

// TestExtensionStmt tests the CreateExtensionStmt node.
func TestExtensionStmt(t *testing.T) {
	t.Run("CreateExtension", func(t *testing.T) {
		createExtStmt := NewCreateExtensionStmt("uuid-ossp", false)

		assert.Equal(t, T_CreateExtensionStmt, createExtStmt.NodeTag())
		assert.Equal(t, "CreateExtensionStmt", createExtStmt.StatementType())
		assert.Equal(t, "uuid-ossp", createExtStmt.Extname)
		assert.False(t, createExtStmt.IfNotExists)
		assert.Contains(t, createExtStmt.String(), "uuid-ossp")
		assert.NotContains(t, createExtStmt.String(), "IF NOT EXISTS")

		// Test interface compliance
		var _ Node = createExtStmt
		var _ Statement = createExtStmt
	})

	t.Run("CreateExtensionIfNotExists", func(t *testing.T) {
		createExtStmt := NewCreateExtensionStmt("postgis", true)

		assert.True(t, createExtStmt.IfNotExists)
		assert.Contains(t, createExtStmt.String(), "IF NOT EXISTS")
	})

	t.Run("CreateExtensionWithOptions", func(t *testing.T) {
		createExtStmt := NewCreateExtensionStmt("plpgsql", false)
		schemaOption := NewDefElem("schema", NewString("public"))
		versionOption := NewDefElem("version", NewString("1.0"))
		createExtStmt.Options = []*DefElem{schemaOption, versionOption}

		assert.Len(t, createExtStmt.Options, 2)
		assert.Equal(t, "schema", createExtStmt.Options[0].Defname)
		assert.Equal(t, "version", createExtStmt.Options[1].Defname)
	})
}

// TestTypeName tests the TypeName node.
func TestTypeName(t *testing.T) {
	t.Run("SimpleType", func(t *testing.T) {
		typeName := NewTypeName([]string{"integer"})

		assert.Equal(t, T_TypeName, typeName.NodeTag())
		assert.Equal(t, []string{"integer"}, typeName.Names)
		assert.Contains(t, typeName.String(), "integer")

		// Test interface compliance
		var _ Node = typeName
	})

	t.Run("QualifiedType", func(t *testing.T) {
		typeName := NewTypeName([]string{"public", "custom_type"})

		assert.Equal(t, []string{"public", "custom_type"}, typeName.Names)
		assert.Contains(t, typeName.String(), "custom_type") // Should show the last part
	})
}

// TestCollateClause tests the CollateClause node.
func TestCollateClause(t *testing.T) {
	t.Run("SimpleCollation", func(t *testing.T) {
		collateClause := NewCollateClause([]string{"en_US"})

		assert.Equal(t, T_CollateClause, collateClause.NodeTag())
		assert.Equal(t, []string{"en_US"}, collateClause.Collname)
		assert.Contains(t, collateClause.String(), "en_US")

		// Test interface compliance
		var _ Node = collateClause
	})

	t.Run("QualifiedCollation", func(t *testing.T) {
		collateClause := NewCollateClause([]string{"pg_catalog", "C"})

		assert.Equal(t, []string{"pg_catalog", "C"}, collateClause.Collname)
		assert.Contains(t, collateClause.String(), "C") // Should show the last part
	})
}

// TestDDLComplexExamples tests complex DDL statement examples.
func TestDDLComplexExamples(t *testing.T) {
	t.Run("CompleteTableAlter", func(t *testing.T) {
		// ALTER TABLE users 
		//   ADD COLUMN email VARCHAR(255),
		//   ADD CONSTRAINT uk_email UNIQUE (email),
		//   DROP COLUMN old_field CASCADE
		
		relation := NewRangeVar("users", nil, nil)
		
		// Add column command
		columnDef := NewString("VARCHAR(255)")
		addColumnCmd := NewAddColumnCmd("email", columnDef)
		
		// Add constraint command
		uniqueConstraint := NewUniqueConstraint("uk_email", []string{"email"})
		addConstraintCmd := NewAddConstraintCmd(uniqueConstraint)
		
		// Drop column command
		dropColumnCmd := NewDropColumnCmd("old_field", DROP_CASCADE)
		
		alterStmt := NewAlterTableStmt(relation, []*AlterTableCmd{
			addColumnCmd, addConstraintCmd, dropColumnCmd,
		})

		assert.Len(t, alterStmt.Cmds, 3)
		assert.Equal(t, AT_AddColumn, alterStmt.Cmds[0].Subtype)
		assert.Equal(t, AT_AddConstraint, alterStmt.Cmds[1].Subtype)
		assert.Equal(t, AT_DropColumn, alterStmt.Cmds[2].Subtype)
		assert.Contains(t, alterStmt.String(), "3 cmds")
	})

	t.Run("ComplexIndex", func(t *testing.T) {
		// CREATE UNIQUE INDEX CONCURRENTLY idx_users_email_lower 
		// ON users (lower(email)) WHERE active = true
		
		relation := NewRangeVar("users", nil, nil)
		lowerFunc := NewFuncExpr(870, 25, []Node{NewVar(1, 1, 25)}) // lower(email)
		idxElem := NewExpressionIndexElem(lowerFunc)
		
		indexStmt := NewUniqueIndex("idx_users_email_lower", relation, []*IndexElem{idxElem})
		indexStmt.Concurrent = true
		
		// WHERE clause (simplified)
		whereClause := NewBinaryOp(96, NewVar(1, 2, 16), NewConst(16, 1, false)) // active = true
		indexStmt.WhereClause = whereClause

		assert.True(t, indexStmt.Unique)
		assert.True(t, indexStmt.Concurrent)
		assert.NotNil(t, indexStmt.WhereClause)
		assert.Len(t, indexStmt.IndexParams, 1)
		
		elem := indexStmt.IndexParams[0]
		assert.Equal(t, "", elem.Name) // Expression index has no column name
		assert.NotNil(t, elem.Expr)
	})

	t.Run("ViewWithConstraints", func(t *testing.T) {
		// CREATE OR REPLACE VIEW active_users (user_id, username) AS
		// SELECT id, name FROM users WHERE active = true
		// WITH CASCADED CHECK OPTION
		
		view := NewRangeVar("active_users", nil, nil)
		query := NewString("SELECT id, name FROM users WHERE active = true")
		viewStmt := NewViewStmt(view, query, true)
		viewStmt.Aliases = []string{"user_id", "username"}
		viewStmt.WithCheckOption = CASCADED_CHECK_OPTION
		
		// Add options
		securityBarrierOpt := NewDefElem("security_barrier", NewBoolean(true))
		viewStmt.Options = []*DefElem{securityBarrierOpt}

		assert.True(t, viewStmt.Replace)
		assert.Equal(t, []string{"user_id", "username"}, viewStmt.Aliases)
		assert.Equal(t, CASCADED_CHECK_OPTION, viewStmt.WithCheckOption)
		assert.Len(t, viewStmt.Options, 1)
		assert.Equal(t, "security_barrier", viewStmt.Options[0].Defname)
	})
}