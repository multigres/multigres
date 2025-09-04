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
		assert.Equal(t, "RESTRICT", DropRestrict.String())
		assert.Equal(t, "CASCADE", DropCascade.String())
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
		assert.Equal(t, []string{"id"}, constraint.GetKeys())
		assert.Contains(t, constraint.String(), "PRIMARY_KEY")
		assert.Contains(t, constraint.String(), "pk_users")

		// Test interface compliance
		var _ Node = constraint
	})

	t.Run("ForeignKeyConstraint", func(t *testing.T) {
		pktable := NewRangeVar("departments", "", "")
		constraint := NewForeignKeyConstraint("fk_dept", []string{"dept_id"}, pktable, []string{"id"})

		assert.Equal(t, CONSTR_FOREIGN, constraint.Contype)
		assert.Equal(t, "fk_dept", constraint.Conname)
		assert.Equal(t, []string{"dept_id"}, constraint.GetFkAttrs())
		assert.Equal(t, pktable, constraint.Pktable)
		assert.Equal(t, []string{"id"}, constraint.GetPkAttrs())
		assert.Contains(t, constraint.String(), "FOREIGN_KEY")
	})

	t.Run("UniqueConstraint", func(t *testing.T) {
		constraint := NewUniqueConstraint("uk_email", []string{"email"})

		assert.Equal(t, CONSTR_UNIQUE, constraint.Contype)
		assert.Equal(t, "uk_email", constraint.Conname)
		assert.Equal(t, []string{"email"}, constraint.GetKeys())
		assert.False(t, constraint.NullsNotDistinct, "Default UNIQUE constraint should have NullsNotDistinct=false")
		assert.Contains(t, constraint.String(), "UNIQUE")
	})

	t.Run("UniqueConstraintNullsNotDistinct", func(t *testing.T) {
		constraint := NewUniqueConstraintNullsNotDistinct("uk_email_nnd", []string{"email"})

		assert.Equal(t, CONSTR_UNIQUE, constraint.Contype)
		assert.Equal(t, "uk_email_nnd", constraint.Conname)
		assert.Equal(t, []string{"email"}, constraint.GetKeys())
		assert.True(t, constraint.NullsNotDistinct, "NULLS NOT DISTINCT constraint should have NullsNotDistinct=true")
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
		relation := NewRangeVar("users", "", "")
		columnDef := NewString("VARCHAR(100)") // Simplified column definition
		addColumnCmd := NewAddColumnCmd("email", columnDef)

		cmdList := NewNodeList()
		cmdList.Append(addColumnCmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		assert.Equal(t, T_AlterTableStmt, alterStmt.NodeTag())
		assert.Equal(t, "AlterTableStmt", alterStmt.StatementType())
		assert.Equal(t, relation, alterStmt.Relation)
		assert.Equal(t, 1, alterStmt.Cmds.Len())
		assert.Equal(t, OBJECT_TABLE, alterStmt.Objtype)
		assert.Contains(t, alterStmt.String(), "users")
		assert.Contains(t, alterStmt.String(), "ADD COLUMN")

		// Test interface compliance
		var _ Node = alterStmt
		var _ Stmt = alterStmt

		// Test the command
		cmd := alterStmt.Cmds.Items[0].(*AlterTableCmd)
		assert.Equal(t, T_AlterTableCmd, cmd.NodeTag())
		assert.Equal(t, AT_AddColumn, cmd.Subtype)
		assert.Equal(t, "email", cmd.Name)
		assert.Equal(t, columnDef, cmd.Def)
		assert.Contains(t, cmd.String(), "ADD_COLUMN")
		assert.Contains(t, cmd.String(), "email")
	})

	t.Run("DropColumn", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		dropColumnCmd := NewDropColumnCmd("old_field", DropCascade)

		cmdList := NewNodeList()
		cmdList.Append(dropColumnCmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		cmd := alterStmt.Cmds.Items[0].(*AlterTableCmd)
		assert.Equal(t, AT_DropColumn, cmd.Subtype)
		assert.Equal(t, "old_field", cmd.Name)
		assert.Equal(t, DropCascade, cmd.Behavior)
		assert.Contains(t, cmd.String(), "DROP_COLUMN")
	})

	t.Run("AddConstraint", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		constraint := NewPrimaryKeyConstraint("pk_users", []string{"id"})
		addConstraintCmd := NewAddConstraintCmd(constraint)

		cmdList := NewNodeList()
		cmdList.Append(addConstraintCmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		cmd := alterStmt.Cmds.Items[0].(*AlterTableCmd)
		assert.Equal(t, AT_AddConstraint, cmd.Subtype)
		assert.Equal(t, "pk_users", cmd.Name)
		assert.Equal(t, constraint, cmd.Def)
		assert.Contains(t, cmd.String(), "ADD_CONSTRAINT")
	})

	t.Run("DropConstraint", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		dropConstraintCmd := NewDropConstraintCmd("old_constraint", DropRestrict)

		cmdList := NewNodeList()
		cmdList.Append(dropConstraintCmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		cmd := alterStmt.Cmds.Items[0].(*AlterTableCmd)
		assert.Equal(t, AT_DropConstraint, cmd.Subtype)
		assert.Equal(t, "old_constraint", cmd.Name)
		assert.Equal(t, DropRestrict, cmd.Behavior)
		assert.Contains(t, cmd.String(), "DROP_CONSTRAINT")
	})
}

// TestIndexStmt tests the IndexStmt node.
func TestIndexStmt(t *testing.T) {
	t.Run("BasicIndex", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		idxElem := NewIndexElem("email")
		indexParams := NewNodeList()
		indexParams.Append(idxElem)
		indexStmt := NewIndexStmt("idx_users_email", relation, indexParams)

		assert.Equal(t, T_IndexStmt, indexStmt.NodeTag())
		assert.Equal(t, "IndexStmt", indexStmt.StatementType())
		assert.Equal(t, "idx_users_email", indexStmt.Idxname)
		assert.Equal(t, relation, indexStmt.Relation)
		assert.Equal(t, "btree", indexStmt.AccessMethod)
		assert.Equal(t, 1, indexStmt.IndexParams.Len())
		assert.False(t, indexStmt.Unique)
		assert.Contains(t, indexStmt.String(), "idx_users_email")
		assert.Contains(t, indexStmt.String(), "users")

		// Test interface compliance
		var _ Node = indexStmt
		var _ Stmt = indexStmt
	})

	t.Run("UniqueIndex", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		idxElem := NewIndexElem("email")
		indexParams := NewNodeList()
		indexParams.Append(idxElem)
		indexStmt := NewUniqueIndex("uk_users_email", relation, indexParams)

		assert.True(t, indexStmt.Unique)
		assert.Contains(t, indexStmt.String(), "UNIQUE")
	})

	t.Run("CompositeIndex", func(t *testing.T) {
		relation := NewRangeVar("orders", "", "")
		idxElem1 := NewIndexElem("customer_id")
		idxElem2 := NewDescIndexElem("order_date")
		indexParams := NewNodeList()
		indexParams.Append(idxElem1)
		indexParams.Append(idxElem2)
		indexStmt := NewIndexStmt("idx_orders_customer_date", relation, indexParams)

		assert.Equal(t, 2, indexStmt.IndexParams.Len())

		// Test first element
		elem1 := indexStmt.IndexParams.Items[0].(*IndexElem)
		assert.Equal(t, "customer_id", elem1.Name)
		assert.Equal(t, SORTBY_DEFAULT, elem1.Ordering)

		// Test second element (descending)
		elem2 := indexStmt.IndexParams.Items[1].(*IndexElem)
		assert.Equal(t, "order_date", elem2.Name)
		assert.Equal(t, SORTBY_DESC, elem2.Ordering)
		assert.Contains(t, elem2.String(), "DESC")
	})

	t.Run("ExpressionIndex", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		lowerExpr := NewFuncExpr(870, 25, NewNodeList(NewVar(1, 1, 25))) // lower(email)
		idxElem := NewExpressionIndexElem(lowerExpr)
		indexParams := NewNodeList()
		indexParams.Append(idxElem)
		indexStmt := NewIndexStmt("idx_users_lower_email", relation, indexParams)

		elem := indexStmt.IndexParams.Items[0].(*IndexElem)
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
		view := NewRangeVar("user_summary", "", "")
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
		var _ Stmt = viewStmt
	})

	t.Run("CreateOrReplaceView", func(t *testing.T) {
		view := NewRangeVar("user_summary", "", "")
		query := NewString("SELECT id, name, email FROM users")
		viewStmt := NewViewStmt(view, query, true)

		assert.True(t, viewStmt.Replace)
		assert.Contains(t, viewStmt.String(), "OR REPLACE")
	})

	t.Run("ViewWithCheckOption", func(t *testing.T) {
		view := NewRangeVar("active_users", "", "")
		query := NewString("SELECT * FROM users WHERE active = true")
		viewStmt := NewViewStmt(view, query, false)
		viewStmt.WithCheckOption = LOCAL_CHECK_OPTION

		assert.Equal(t, LOCAL_CHECK_OPTION, viewStmt.WithCheckOption)
	})

	t.Run("ViewWithAliases", func(t *testing.T) {
		view := NewRangeVar("user_info", "", "")
		query := NewString("SELECT id, name FROM users")
		viewStmt := NewViewStmt(view, query, false)
		viewStmt.Aliases = &NodeList{Items: []Node{NewString("user_id"), NewString("full_name")}}

		assert.Equal(t, 2, viewStmt.Aliases.Len())
		if alias1, ok := viewStmt.Aliases.Items[0].(*String); ok {
			assert.Equal(t, "user_id", alias1.SVal)
		}
		if alias2, ok := viewStmt.Aliases.Items[1].(*String); ok {
			assert.Equal(t, "full_name", alias2.SVal)
		}
	})
}

// TestDomainStmts tests domain-related statements.
func TestDomainStmts(t *testing.T) {
	t.Run("CreateDomainStmt", func(t *testing.T) {
		domainname := NewNodeList(NewString("public"))
		domainname.Append(NewString("email_domain"))
		typeName := NewTypeName([]string{"varchar"})
		createDomainStmt := NewCreateDomainStmt(domainname, typeName)

		assert.Equal(t, T_CreateDomainStmt, createDomainStmt.NodeTag())
		assert.Equal(t, "CreateDomainStmt", createDomainStmt.StatementType())
		assert.Equal(t, domainname, createDomainStmt.Domainname)
		assert.Equal(t, typeName, createDomainStmt.TypeName)
		assert.Contains(t, createDomainStmt.String(), "email_domain")

		// Test interface compliance
		var _ Node = createDomainStmt
		var _ Stmt = createDomainStmt
	})

	t.Run("AlterDomainStmt", func(t *testing.T) {
		typeName := NewNodeList(NewString("public"))
		typeName.Append(NewString("email_domain"))
		alterDomainStmt := NewAlterDomainStmt('T', typeName) // 'T' for alter default

		assert.Equal(t, T_AlterDomainStmt, alterDomainStmt.NodeTag())
		assert.Equal(t, "AlterDomainStmt", alterDomainStmt.StatementType())
		assert.Equal(t, byte('T'), alterDomainStmt.Subtype)
		assert.Equal(t, typeName, alterDomainStmt.TypeName)
		assert.Equal(t, DropRestrict, alterDomainStmt.Behavior)
		assert.Contains(t, alterDomainStmt.String(), "email_domain")

		// Test interface compliance
		var _ Node = alterDomainStmt
		var _ Stmt = alterDomainStmt
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
		var _ Stmt = createSchemaStmt
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
		createExtStmt := NewCreateExtensionStmt("uuid-ossp", false, nil)

		assert.Equal(t, T_CreateExtensionStmt, createExtStmt.NodeTag())
		assert.Equal(t, "CreateExtensionStmt", createExtStmt.StatementType())
		assert.Equal(t, "uuid-ossp", createExtStmt.Extname)
		assert.False(t, createExtStmt.IfNotExists)
		assert.Contains(t, createExtStmt.String(), "uuid-ossp")
		assert.NotContains(t, createExtStmt.String(), "IF NOT EXISTS")

		// Test interface compliance
		var _ Node = createExtStmt
		var _ Stmt = createExtStmt
	})

	t.Run("CreateExtensionIfNotExists", func(t *testing.T) {
		createExtStmt := NewCreateExtensionStmt("postgis", true, nil)

		assert.True(t, createExtStmt.IfNotExists)
		assert.Contains(t, createExtStmt.String(), "IF NOT EXISTS")
	})

	t.Run("CreateExtensionWithOptions", func(t *testing.T) {
		schemaOption := NewDefElem("schema", NewString("public"))
		versionOption := NewDefElem("version", NewString("1.0"))
		options := NewNodeList(schemaOption)
		options.Append(versionOption)
		createExtStmt := NewCreateExtensionStmt("plpgsql", false, options)

		assert.NotNil(t, createExtStmt.Options)
		assert.Equal(t, 2, createExtStmt.Options.Len())

		// Check the first option
		if defElem, ok := createExtStmt.Options.Items[0].(*DefElem); ok {
			assert.Equal(t, "schema", defElem.Defname)
		}

		// Check the second option
		if defElem, ok := createExtStmt.Options.Items[1].(*DefElem); ok {
			assert.Equal(t, "version", defElem.Defname)
		}
	})
}

// TestTypeName tests the TypeName node.
func TestTypeName(t *testing.T) {
	t.Run("SimpleType", func(t *testing.T) {
		typeName := NewTypeName([]string{"integer"})

		assert.Equal(t, T_TypeName, typeName.NodeTag())
		assert.Equal(t, []string{"integer"}, typeName.GetNames())
		assert.Contains(t, typeName.String(), "integer")

		// Test interface compliance
		var _ Node = typeName
	})

	t.Run("QualifiedType", func(t *testing.T) {
		typeName := NewTypeName([]string{"public", "custom_type"})

		assert.Equal(t, []string{"public", "custom_type"}, typeName.GetNames())
		assert.Contains(t, typeName.String(), "custom_type") // Should show the last part
	})
}

// TestCollateClause tests the CollateClause node.
func TestCollateClause(t *testing.T) {
	t.Run("SimpleCollation", func(t *testing.T) {
		collNames := NewNodeList(NewString("en_US"))
		collateClause := NewCollateClause(collNames)

		assert.Equal(t, T_CollateClause, collateClause.NodeTag())
		assert.Equal(t, collNames, collateClause.Collname)
		assert.Contains(t, collateClause.String(), "en_US")

		// Test interface compliance
		var _ Node = collateClause
	})

	t.Run("QualifiedCollation", func(t *testing.T) {
		collNames := NewNodeList(NewString("pg_catalog"))
		collNames.Append(NewString("C"))
		collateClause := NewCollateClause(collNames)

		assert.Equal(t, collNames, collateClause.Collname)
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

		relation := NewRangeVar("users", "", "")

		// Add column command
		columnDef := NewString("VARCHAR(255)")
		addColumnCmd := NewAddColumnCmd("email", columnDef)

		// Add constraint command
		uniqueConstraint := NewUniqueConstraint("uk_email", []string{"email"})
		addConstraintCmd := NewAddConstraintCmd(uniqueConstraint)

		// Drop column command
		dropColumnCmd := NewDropColumnCmd("old_field", DropCascade)

		cmdList := NewNodeList()
		cmdList.Append(addColumnCmd)
		cmdList.Append(addConstraintCmd)
		cmdList.Append(dropColumnCmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		assert.Equal(t, 3, alterStmt.Cmds.Len())
		assert.Equal(t, AT_AddColumn, alterStmt.Cmds.Items[0].(*AlterTableCmd).Subtype)
		assert.Equal(t, AT_AddConstraint, alterStmt.Cmds.Items[1].(*AlterTableCmd).Subtype)
		assert.Equal(t, AT_DropColumn, alterStmt.Cmds.Items[2].(*AlterTableCmd).Subtype)
		assert.Contains(t, alterStmt.String(), "ALTER TABLE users ADD COLUMN 'VARCHAR(255)', ADD CONSTRAINT uk_email UNIQUE (email), DROP COLUMN old_field CASCADE")
	})

	t.Run("ComplexIndex", func(t *testing.T) {
		// CREATE UNIQUE INDEX CONCURRENTLY idx_users_email_lower
		// ON users (lower(email)) WHERE active = true

		relation := NewRangeVar("users", "", "")
		lowerFunc := NewFuncExpr(870, 25, NewNodeList(NewVar(1, 1, 25))) // lower(email)
		idxElem := NewExpressionIndexElem(lowerFunc)

		indexParams := NewNodeList()
		indexParams.Append(idxElem)
		indexStmt := NewUniqueIndex("idx_users_email_lower", relation, indexParams)
		indexStmt.Concurrent = true

		// WHERE clause (simplified)
		whereClause := NewBinaryOp(96, NewVar(1, 2, 16), NewConst(16, 1, false)) // active = true
		indexStmt.WhereClause = whereClause

		assert.True(t, indexStmt.Unique)
		assert.True(t, indexStmt.Concurrent)
		assert.NotNil(t, indexStmt.WhereClause)
		assert.Equal(t, 1, indexStmt.IndexParams.Len())

		elem := indexStmt.IndexParams.Items[0].(*IndexElem)
		assert.Equal(t, "", elem.Name) // Expression index has no column name
		assert.NotNil(t, elem.Expr)
	})

	t.Run("ViewWithConstraints", func(t *testing.T) {
		// CREATE OR REPLACE VIEW active_users (user_id, username) AS
		// SELECT id, name FROM users WHERE active = true
		// WITH CASCADED CHECK OPTION

		view := NewRangeVar("active_users", "", "")
		query := NewString("SELECT id, name FROM users WHERE active = true")
		viewStmt := NewViewStmt(view, query, true)
		viewStmt.Aliases = &NodeList{Items: []Node{NewString("user_id"), NewString("username")}}
		viewStmt.WithCheckOption = CASCADED_CHECK_OPTION

		// Add options
		securityBarrierOpt := NewDefElem("security_barrier", NewBoolean(true))
		viewStmt.Options = &NodeList{Items: []Node{securityBarrierOpt}}

		assert.True(t, viewStmt.Replace)
		assert.Equal(t, 2, viewStmt.Aliases.Len())
		if alias1, ok := viewStmt.Aliases.Items[0].(*String); ok {
			assert.Equal(t, "user_id", alias1.SVal)
		}
		if alias2, ok := viewStmt.Aliases.Items[1].(*String); ok {
			assert.Equal(t, "username", alias2.SVal)
		}
		assert.Equal(t, CASCADED_CHECK_OPTION, viewStmt.WithCheckOption)
		assert.Equal(t, 1, viewStmt.Options.Len())
		if defElem, ok := viewStmt.Options.Items[0].(*DefElem); ok {
			assert.Equal(t, "security_barrier", defElem.Defname)
		}
	})
}

// TestAlterTableStmtSqlString tests the SqlString method for AlterTableStmt
func TestAlterTableStmtSqlString(t *testing.T) {
	t.Run("AddColumn", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		columnDef := NewColumnDef("email", NewTypeName([]string{"varchar"}), 0)
		addColumnCmd := NewAddColumnCmd("", columnDef)

		cmdList := NewNodeList()
		cmdList.Append(addColumnCmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		expected := "ALTER TABLE users ADD COLUMN email VARCHAR"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("DropColumn", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		dropColumnCmd := NewDropColumnCmd("old_field", DropCascade)
		dropColumnCmd.MissingOk = true

		cmdList := NewNodeList()
		cmdList.Append(dropColumnCmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		expected := "ALTER TABLE users DROP COLUMN IF EXISTS old_field CASCADE"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("AlterColumnType", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		typeName := NewTypeName([]string{"bigint"})
		cmd := NewAlterTableCmd(AT_AlterColumnType, "id", typeName)

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		expected := "ALTER TABLE users ALTER COLUMN id TYPE BIGINT"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("SetNotNull", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		cmd := NewAlterTableCmd(AT_SetNotNull, "email", nil)

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		expected := "ALTER TABLE users ALTER COLUMN email SET NOT NULL"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("DropNotNull", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		cmd := NewAlterTableCmd(AT_DropNotNull, "email", nil)

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		expected := "ALTER TABLE users ALTER COLUMN email DROP NOT NULL"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("SetDefault", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		defaultValue := NewString("unknown")
		cmd := NewAlterTableCmd(AT_ColumnDefault, "status", defaultValue)

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		expected := "ALTER TABLE users ALTER COLUMN status SET DEFAULT 'unknown'"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("DropDefault", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		cmd := NewAlterTableCmd(AT_ColumnDefault, "status", nil)

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		expected := "ALTER TABLE users ALTER COLUMN status DROP DEFAULT"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("SetStatistics", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		statsValue := NewInteger(1000)
		cmd := NewAlterTableCmd(AT_SetStatistics, "email", statsValue)

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		expected := "ALTER TABLE users ALTER COLUMN email SET STATISTICS 1000"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("AlterIndex", func(t *testing.T) {
		relation := NewRangeVar("idx_users_email", "", "")
		cmd := NewAlterTableCmd(AT_SetTableSpace, "fast_storage", nil)

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)
		alterStmt.Objtype = OBJECT_INDEX

		expected := "ALTER INDEX idx_users_email SET TABLESPACE fast_storage"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("AlterIndexIfExists", func(t *testing.T) {
		relation := NewRangeVar("idx_users_email", "", "")
		cmd := NewAlterTableCmd(AT_SetTableSpace, "fast_storage", nil)

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)
		alterStmt.Objtype = OBJECT_INDEX
		alterStmt.MissingOk = true

		expected := "ALTER INDEX IF EXISTS idx_users_email SET TABLESPACE fast_storage"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("MultipleCommands", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")

		cmd1 := NewAlterTableCmd(AT_SetNotNull, "email", nil)
		cmd2 := NewAlterTableCmd(AT_SetStatistics, "email", NewInteger(500))

		cmdList := NewNodeList()
		cmdList.Append(cmd1)
		cmdList.Append(cmd2)
		alterStmt := NewAlterTableStmt(relation, cmdList)

		expected := "ALTER TABLE users ALTER COLUMN email SET NOT NULL, ALTER COLUMN email SET STATISTICS 500"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	// Test new object types
	t.Run("AlterSequence", func(t *testing.T) {
		relation := NewRangeVar("user_id_seq", "", "")
		cmd := NewAlterTableCmd(AT_ColumnDefault, "increment_by", NewString("5"))

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)
		alterStmt.Objtype = OBJECT_SEQUENCE

		expected := "ALTER SEQUENCE user_id_seq ALTER COLUMN increment_by SET DEFAULT '5'"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("AlterSequenceIfExists", func(t *testing.T) {
		relation := NewRangeVar("user_id_seq", "", "")
		cmd := NewAlterTableCmd(AT_ColumnDefault, "increment_by", NewString("1"))

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)
		alterStmt.Objtype = OBJECT_SEQUENCE
		alterStmt.MissingOk = true

		expected := "ALTER SEQUENCE IF EXISTS user_id_seq ALTER COLUMN increment_by SET DEFAULT '1'"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("AlterView", func(t *testing.T) {
		relation := NewRangeVar("user_view", "", "")
		columnDef := NewColumnDef("status", NewTypeName([]string{"varchar"}), 0)
		addColumnCmd := NewAddColumnCmd("", columnDef)

		cmdList := NewNodeList()
		cmdList.Append(addColumnCmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)
		alterStmt.Objtype = OBJECT_VIEW

		expected := "ALTER VIEW user_view ADD COLUMN status VARCHAR"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("AlterViewIfExists", func(t *testing.T) {
		relation := NewRangeVar("user_view", "", "")
		cmd := NewAlterTableCmd(AT_DropColumn, "old_field", nil)
		cmd.MissingOk = true

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)
		alterStmt.Objtype = OBJECT_VIEW
		alterStmt.MissingOk = true

		expected := "ALTER VIEW IF EXISTS user_view DROP COLUMN IF EXISTS old_field"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("AlterMaterializedView", func(t *testing.T) {
		relation := NewRangeVar("user_summary_mv", "", "")
		cmd := NewAlterTableCmd(AT_SetNotNull, "created_at", nil)

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)
		alterStmt.Objtype = OBJECT_MATVIEW

		expected := "ALTER MATERIALIZED VIEW user_summary_mv ALTER COLUMN created_at SET NOT NULL"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("AlterMaterializedViewIfExists", func(t *testing.T) {
		relation := NewRangeVar("user_summary_mv", "", "")
		cmd := NewAlterTableCmd(AT_DropNotNull, "updated_at", nil)

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)
		alterStmt.Objtype = OBJECT_MATVIEW
		alterStmt.MissingOk = true

		expected := "ALTER MATERIALIZED VIEW IF EXISTS user_summary_mv ALTER COLUMN updated_at DROP NOT NULL"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("AlterForeignTable", func(t *testing.T) {
		relation := NewRangeVar("remote_users", "", "")
		columnDef := NewColumnDef("external_id", NewTypeName([]string{"bigint"}), 0)
		addColumnCmd := NewAddColumnCmd("", columnDef)

		cmdList := NewNodeList()
		cmdList.Append(addColumnCmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)
		alterStmt.Objtype = OBJECT_FOREIGN_TABLE

		expected := "ALTER FOREIGN TABLE remote_users ADD COLUMN external_id BIGINT"
		assert.Equal(t, expected, alterStmt.SqlString())
	})

	t.Run("AlterForeignTableIfExists", func(t *testing.T) {
		relation := NewRangeVar("remote_users", "", "")
		cmd := NewAlterTableCmd(AT_DropColumn, "deprecated_field", nil)

		cmdList := NewNodeList()
		cmdList.Append(cmd)
		alterStmt := NewAlterTableStmt(relation, cmdList)
		alterStmt.Objtype = OBJECT_FOREIGN_TABLE
		alterStmt.MissingOk = true

		expected := "ALTER FOREIGN TABLE IF EXISTS remote_users DROP COLUMN deprecated_field"
		assert.Equal(t, expected, alterStmt.SqlString())
	})
}

// TestImportForeignSchemaStmt tests ImportForeignSchemaStmt functionality
func TestImportForeignSchemaStmt(t *testing.T) {
	t.Run("ImportForeignSchemaType", func(t *testing.T) {
		tests := []struct {
			schemaType ImportForeignSchemaType
			expected   string
		}{
			{FDW_IMPORT_SCHEMA_ALL, ""},
			{FDW_IMPORT_SCHEMA_LIMIT_TO, "LIMIT TO"},
			{FDW_IMPORT_SCHEMA_EXCEPT, "EXCEPT"},
		}

		for _, tt := range tests {
			assert.Equal(t, tt.expected, tt.schemaType.String())
		}
	})

	t.Run("ImportForeignSchemaBasic", func(t *testing.T) {
		serverName := NewString("my_server")
		remoteSchema := NewString("remote_schema")
		localSchema := NewString("local_schema")
		
		stmt := NewImportForeignSchemaStmt(
			serverName,
			remoteSchema,
			localSchema,
			FDW_IMPORT_SCHEMA_ALL,
			NewNodeList(),
			NewNodeList(),
		)

		expected := "IMPORT FOREIGN SCHEMA remote_schema FROM SERVER my_server INTO local_schema"
		assert.Equal(t, expected, stmt.SqlString())
	})

	t.Run("ImportForeignSchemaWithLimitTo", func(t *testing.T) {
		serverName := NewString("my_server")
		remoteSchema := NewString("remote_schema")
		localSchema := NewString("local_schema")
		
		// Create table list
		tableList := NewNodeList()
		table1 := NewRangeVar("users", "", "")
		table2 := NewRangeVar("orders", "", "")
		tableList.Append(table1)
		tableList.Append(table2)
		
		stmt := NewImportForeignSchemaStmt(
			serverName,
			remoteSchema,
			localSchema,
			FDW_IMPORT_SCHEMA_LIMIT_TO,
			tableList,
			NewNodeList(),
		)

		expected := "IMPORT FOREIGN SCHEMA remote_schema LIMIT TO ( users, orders ) FROM SERVER my_server INTO local_schema"
		assert.Equal(t, expected, stmt.SqlString())
	})

	t.Run("ImportForeignSchemaWithExcept", func(t *testing.T) {
		serverName := NewString("my_server")
		remoteSchema := NewString("remote_schema")
		localSchema := NewString("local_schema")
		
		// Create table list
		tableList := NewNodeList()
		table1 := NewRangeVar("temp_table", "", "")
		tableList.Append(table1)
		
		stmt := NewImportForeignSchemaStmt(
			serverName,
			remoteSchema,
			localSchema,
			FDW_IMPORT_SCHEMA_EXCEPT,
			tableList,
			NewNodeList(),
		)

		expected := "IMPORT FOREIGN SCHEMA remote_schema EXCEPT ( temp_table ) FROM SERVER my_server INTO local_schema"
		assert.Equal(t, expected, stmt.SqlString())
	})

	t.Run("ImportForeignSchemaWithOptions", func(t *testing.T) {
		serverName := NewString("my_server")
		remoteSchema := NewString("remote_schema")
		localSchema := NewString("local_schema")
		
		// Create options
		options := NewNodeList()
		opt1 := NewDefElem("schema_regexp", NewString("^public"))
		opt2 := NewDefElem("max_workers", NewInteger(4))
		options.Append(opt1)
		options.Append(opt2)
		
		stmt := NewImportForeignSchemaStmt(
			serverName,
			remoteSchema,
			localSchema,
			FDW_IMPORT_SCHEMA_ALL,
			NewNodeList(),
			options,
		)

		expected := "IMPORT FOREIGN SCHEMA remote_schema FROM SERVER my_server INTO local_schema OPTIONS ( schema_regexp = '^public', max_workers = 4 )"
		assert.Equal(t, expected, stmt.SqlString())
	})
}
