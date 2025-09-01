// Package ast provides PostgreSQL utility statement node definitions.
// Ported from postgres/src/include/nodes/parsenodes.h
package ast

import (
	"fmt"
	"strings"
)

// shouldQuoteValue determines if a value should be quoted as a string literal.
// It uses the same logic as QuoteIdentifier to determine if quoting is needed.
func shouldQuoteValue(val string) bool {
	if val == "" {
		return true
	}
	
	// Use the same logic as QuoteIdentifier to determine if quoting is needed
	return QuoteIdentifier(val) != val
}


// ==============================================================================
// TRANSACTION CONTROL STATEMENTS - PostgreSQL parsenodes.h:3653-3679
// ==============================================================================

// TransactionStmtKind represents the type of transaction statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3653
type TransactionStmtKind int

const (
	TRANS_STMT_BEGIN             TransactionStmtKind = iota // BEGIN/START
	TRANS_STMT_START                                        // START (alias for BEGIN)
	TRANS_STMT_COMMIT                                       // COMMIT
	TRANS_STMT_ROLLBACK                                     // ROLLBACK
	TRANS_STMT_SAVEPOINT                                    // SAVEPOINT
	TRANS_STMT_RELEASE                                      // RELEASE
	TRANS_STMT_ROLLBACK_TO                                  // ROLLBACK TO
	TRANS_STMT_PREPARE                                      // PREPARE TRANSACTION
	TRANS_STMT_COMMIT_PREPARED                              // COMMIT PREPARED
	TRANS_STMT_ROLLBACK_PREPARED                            // ROLLBACK PREPARED
)

func (t TransactionStmtKind) String() string {
	switch t {
	case TRANS_STMT_BEGIN:
		return "BEGIN"
	case TRANS_STMT_START:
		return "START"
	case TRANS_STMT_COMMIT:
		return "COMMIT"
	case TRANS_STMT_ROLLBACK:
		return "ROLLBACK"
	case TRANS_STMT_SAVEPOINT:
		return "SAVEPOINT"
	case TRANS_STMT_RELEASE:
		return "RELEASE"
	case TRANS_STMT_ROLLBACK_TO:
		return "ROLLBACK_TO"
	case TRANS_STMT_PREPARE:
		return "PREPARE"
	case TRANS_STMT_COMMIT_PREPARED:
		return "COMMIT_PREPARED"
	case TRANS_STMT_ROLLBACK_PREPARED:
		return "ROLLBACK_PREPARED"
	default:
		return fmt.Sprintf("TransactionStmtKind(%d)", int(t))
	}
}

// TransactionStmt represents a transaction control statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3667
type TransactionStmt struct {
	BaseNode
	Kind          TransactionStmtKind // Kind of transaction statement - postgres/src/include/nodes/parsenodes.h:3670
	Options       *NodeList           // List of DefElem nodes for BEGIN/START - postgres/src/include/nodes/parsenodes.h:3671
	SavepointName string              // Savepoint name for SAVEPOINT/RELEASE/ROLLBACK TO - postgres/src/include/nodes/parsenodes.h:3672
	Gid           string              // String identifier for two-phase commit - postgres/src/include/nodes/parsenodes.h:3673
	Chain         bool                // AND CHAIN option - postgres/src/include/nodes/parsenodes.h:3674
}

// NewTransactionStmt creates a new TransactionStmt node.
func NewTransactionStmt(kind TransactionStmtKind) *TransactionStmt {
	return &TransactionStmt{
		BaseNode: BaseNode{Tag: T_TransactionStmt},
		Kind:     kind,
	}
}

// NewBeginStmt creates a new BEGIN statement.
func NewBeginStmt() *TransactionStmt {
	return NewTransactionStmt(TRANS_STMT_BEGIN)
}

// NewCommitStmt creates a new COMMIT statement.
func NewCommitStmt() *TransactionStmt {
	return NewTransactionStmt(TRANS_STMT_COMMIT)
}

// NewRollbackStmt creates a new ROLLBACK statement.
func NewRollbackStmt() *TransactionStmt {
	return NewTransactionStmt(TRANS_STMT_ROLLBACK)
}

// NewSavepointStmt creates a new SAVEPOINT statement.
func NewSavepointStmt(name string) *TransactionStmt {
	stmt := NewTransactionStmt(TRANS_STMT_SAVEPOINT)
	stmt.SavepointName = name
	return stmt
}

// NewReleaseStmt creates a new RELEASE statement.
func NewReleaseStmt(name string) *TransactionStmt {
	stmt := NewTransactionStmt(TRANS_STMT_RELEASE)
	stmt.SavepointName = name
	return stmt
}

// NewRollbackToStmt creates a new ROLLBACK TO statement.
func NewRollbackToStmt(name string) *TransactionStmt {
	stmt := NewTransactionStmt(TRANS_STMT_ROLLBACK_TO)
	stmt.SavepointName = name
	return stmt
}

func (ts *TransactionStmt) String() string {
	var parts []string
	parts = append(parts, ts.Kind.String())

	if ts.SavepointName != "" {
		parts = append(parts, ts.SavepointName)
	}

	if ts.Gid != "" {
		parts = append(parts, fmt.Sprintf("'%s'", ts.Gid))
	}

	if ts.Chain {
		parts = append(parts, "AND CHAIN")
	}

	return fmt.Sprintf("TransactionStmt(%s)@%d", strings.Join(parts, " "), ts.Location())
}

func (ts *TransactionStmt) StatementType() string {
	return ts.Kind.String()
}

func (ts *TransactionStmt) SqlString() string {
	var parts []string

	switch ts.Kind {
	case TRANS_STMT_BEGIN:
		parts = append(parts, "BEGIN")
	case TRANS_STMT_START:
		parts = append(parts, "START", "TRANSACTION")
	case TRANS_STMT_COMMIT:
		parts = append(parts, "COMMIT")
	case TRANS_STMT_ROLLBACK:
		parts = append(parts, "ROLLBACK")
	case TRANS_STMT_SAVEPOINT:
		parts = append(parts, "SAVEPOINT")
		if ts.SavepointName != "" {
			parts = append(parts, ts.SavepointName)
		}
	case TRANS_STMT_RELEASE:
		parts = append(parts, "RELEASE")
		if ts.SavepointName != "" {
			parts = append(parts, "SAVEPOINT", ts.SavepointName)
		}
	case TRANS_STMT_ROLLBACK_TO:
		parts = append(parts, "ROLLBACK", "TO")
		if ts.SavepointName != "" {
			parts = append(parts, "SAVEPOINT", ts.SavepointName)
		}
	case TRANS_STMT_PREPARE:
		parts = append(parts, "PREPARE", "TRANSACTION")
		if ts.Gid != "" {
			parts = append(parts, fmt.Sprintf("'%s'", ts.Gid))
		}
	case TRANS_STMT_COMMIT_PREPARED:
		parts = append(parts, "COMMIT", "PREPARED")
		if ts.Gid != "" {
			parts = append(parts, fmt.Sprintf("'%s'", ts.Gid))
		}
	case TRANS_STMT_ROLLBACK_PREPARED:
		parts = append(parts, "ROLLBACK", "PREPARED")
		if ts.Gid != "" {
			parts = append(parts, fmt.Sprintf("'%s'", ts.Gid))
		}
	}

	// Add transaction options for BEGIN/START TRANSACTION
	if (ts.Kind == TRANS_STMT_BEGIN || ts.Kind == TRANS_STMT_START) && ts.Options != nil && len(ts.Options.Items) > 0 {
		var optionParts []string
		for _, item := range ts.Options.Items {
			if defElem, ok := item.(*DefElem); ok {
				optionParts = append(optionParts, formatTransactionOption(defElem))
			}
		}
		if len(optionParts) > 0 {
			parts = append(parts, strings.Join(optionParts, " "))
		}
	}

	// Add AND CHAIN / AND NO CHAIN for COMMIT/ROLLBACK
	if ts.Kind == TRANS_STMT_COMMIT || ts.Kind == TRANS_STMT_ROLLBACK {
		if ts.Chain {
			parts = append(parts, "AND", "CHAIN")
		} else if ts.Options != nil {
			// Check if we have an explicit "no chain" option
			for _, item := range ts.Options.Items {
				if defElem, ok := item.(*DefElem); ok && defElem.Defname == "chain" {
					if boolVal, ok := defElem.Arg.(*Boolean); ok && !boolVal.BoolVal {
						parts = append(parts, "AND", "NO", "CHAIN")
						break
					}
				}
			}
		}
	}

	return strings.Join(parts, " ")
}

// ==============================================================================
// SECURITY STATEMENTS - GRANT/REVOKE - PostgreSQL parsenodes.h:2491-2565
// ==============================================================================

// GrantTargetType represents the target type for GRANT/REVOKE statements.
// Ported from postgres/src/include/nodes/parsenodes.h:2472-2489
type GrantTargetType int

const (
	ACL_TARGET_OBJECT        GrantTargetType = iota // Grant on specific objects
	ACL_TARGET_ALL_IN_SCHEMA                        // Grant on all objects in schema
	ACL_TARGET_DEFAULTS                             // Alter default privileges
)

func (g GrantTargetType) String() string {
	switch g {
	case ACL_TARGET_OBJECT:
		return "OBJECT"
	case ACL_TARGET_ALL_IN_SCHEMA:
		return "ALL_IN_SCHEMA"
	case ACL_TARGET_DEFAULTS:
		return "DEFAULTS"
	default:
		return fmt.Sprintf("GrantTargetType(%d)", int(g))
	}
}

// GrantStmt represents a GRANT or REVOKE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2491
type GrantStmt struct {
	BaseNode
	IsGrant     bool            // True = GRANT, false = REVOKE - postgres/src/include/nodes/parsenodes.h:2494
	Targtype    GrantTargetType // Type of target - postgres/src/include/nodes/parsenodes.h:2495
	Objtype     ObjectType      // Kind of object being operated on - postgres/src/include/nodes/parsenodes.h:2496
	Objects     *NodeList       // List of RangeVar nodes, or list of String nodes - postgres/src/include/nodes/parsenodes.h:2497
	Privileges  *NodeList       // List of AccessPriv nodes - postgres/src/include/nodes/parsenodes.h:2498
	Grantees    *NodeList       // List of RoleSpec nodes - postgres/src/include/nodes/parsenodes.h:2499
	GrantOption bool            // Grant or revoke grant option - postgres/src/include/nodes/parsenodes.h:2500
	Grantor     *RoleSpec       // Set by GRANTED BY (when not NULL) - postgres/src/include/nodes/parsenodes.h:2501
	Behavior    DropBehavior    // Drop behavior - postgres/src/include/nodes/parsenodes.h:2502
}

// AccessPriv represents a privilege in a GRANT/REVOKE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2540
type AccessPriv struct {
	BaseNode
	PrivName string    // String name of privilege - postgres/src/include/nodes/parsenodes.h:2510
	Cols     *NodeList // List of column names (or NIL) - postgres/src/include/nodes/parsenodes.h:2511
}

// NewGrantStmt creates a new GRANT statement.
func NewGrantStmt(objtype ObjectType, objects *NodeList, privileges *NodeList, grantees *NodeList) *GrantStmt {
	return &GrantStmt{
		BaseNode:   BaseNode{Tag: T_GrantStmt},
		IsGrant:    true,
		Targtype:   ACL_TARGET_OBJECT,
		Objtype:    objtype,
		Objects:    objects,
		Privileges: privileges,
		Grantees:   grantees,
		Behavior:   DropRestrict,
	}
}

// NewRevokeStmt creates a new REVOKE statement.
func NewRevokeStmt(objtype ObjectType, objects *NodeList, privileges *NodeList, grantees *NodeList) *GrantStmt {
	return &GrantStmt{
		BaseNode:   BaseNode{Tag: T_GrantStmt},
		IsGrant:    false,
		Targtype:   ACL_TARGET_OBJECT,
		Objtype:    objtype,
		Objects:    objects,
		Privileges: privileges,
		Grantees:   grantees,
		Behavior:   DropRestrict,
	}
}

// NewAccessPriv creates a new AccessPriv node.
func NewAccessPriv(privName string, cols *NodeList) *AccessPriv {
	return &AccessPriv{
		BaseNode: BaseNode{Tag: T_AccessPriv},
		PrivName: privName,
		Cols:     cols,
	}
}

func (gs *GrantStmt) String() string {
	action := "REVOKE"
	if gs.IsGrant {
		action = "GRANT"
	}
	objectCount := 0
	if gs.Objects != nil {
		objectCount = len(gs.Objects.Items)
	}
	return fmt.Sprintf("GrantStmt(%s %s on %d objects)@%d", action, gs.Objtype, objectCount, gs.Location())
}

func (gs *GrantStmt) StatementType() string {
	if gs.IsGrant {
		return "GRANT"
	}
	return "REVOKE"
}

func (ap *AccessPriv) String() string {
	if ap.Cols != nil && len(ap.Cols.Items) > 0 {
		return fmt.Sprintf("AccessPriv(%s on %d cols)@%d", ap.PrivName, len(ap.Cols.Items), ap.Location())
	}
	return fmt.Sprintf("AccessPriv(%s)@%d", ap.PrivName, ap.Location())
}

// GrantRoleStmt represents a GRANT/REVOKE role statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2556
type GrantRoleStmt struct {
	BaseNode
	GrantedRoles *NodeList    // List of roles to be granted/revoked - postgres/src/include/nodes/parsenodes.h:2559
	GranteeRoles *NodeList    // List of member roles to add/delete - postgres/src/include/nodes/parsenodes.h:2560
	IsGrant      bool         // True = GRANT, false = REVOKE - postgres/src/include/nodes/parsenodes.h:2561
	Opt          *NodeList    // Options e.g. WITH GRANT OPTION - postgres/src/include/nodes/parsenodes.h:2562
	Grantor      *RoleSpec    // Set by GRANTED BY (when not NULL) - postgres/src/include/nodes/parsenodes.h:2563
	Behavior     DropBehavior // Drop behavior for REVOKE - postgres/src/include/nodes/parsenodes.h:2564
}

// NewGrantRoleStmt creates a new GRANT role statement.
func NewGrantRoleStmt(grantedRoles, granteeRoles *NodeList) *GrantRoleStmt {
	return &GrantRoleStmt{
		BaseNode:     BaseNode{Tag: T_GrantRoleStmt},
		GrantedRoles: grantedRoles,
		GranteeRoles: granteeRoles,
		IsGrant:      true,
		Behavior:     DropRestrict,
	}
}

// NewRevokeRoleStmt creates a new REVOKE role statement.
func NewRevokeRoleStmt(grantedRoles, granteeRoles *NodeList) *GrantRoleStmt {
	return &GrantRoleStmt{
		BaseNode:     BaseNode{Tag: T_GrantRoleStmt},
		GrantedRoles: grantedRoles,
		GranteeRoles: granteeRoles,
		IsGrant:      false,
		Behavior:     DropRestrict,
	}
}

// NewGrantRoleStmtWithOptions creates a new GRANT role statement with options.
func NewGrantRoleStmtWithOptions(grantedRoles, granteeRoles *NodeList, opt *NodeList) *GrantRoleStmt {
	return &GrantRoleStmt{
		BaseNode:     BaseNode{Tag: T_GrantRoleStmt},
		GrantedRoles: grantedRoles,
		GranteeRoles: granteeRoles,
		IsGrant:      true,
		Opt:          opt,
		Behavior:     DropRestrict,
	}
}

func (grs *GrantRoleStmt) String() string {
	action := "REVOKE"
	if grs.IsGrant {
		action = "GRANT"
	}
	grantedCount := 0
	if grs.GrantedRoles != nil {
		grantedCount = len(grs.GrantedRoles.Items)
	}
	granteeCount := 0
	if grs.GranteeRoles != nil {
		granteeCount = len(grs.GranteeRoles.Items)
	}
	return fmt.Sprintf("GrantRoleStmt(%s %d roles to %d grantees)@%d", action, grantedCount, granteeCount, grs.Location())
}

func (grs *GrantRoleStmt) StatementType() string {
	if grs.IsGrant {
		return "GRANT_ROLE"
	}
	return "REVOKE_ROLE"
}

// ==============================================================================
// ROLE MANAGEMENT STATEMENTS - PostgreSQL parsenodes.h:3074-3103
// ==============================================================================

// RoleStatementType represents the type of role statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3074-3079
type RoleStatementType int

const (
	ROLESTMT_ROLE  RoleStatementType = iota // CREATE/ALTER/DROP ROLE
	ROLESTMT_USER                           // CREATE/ALTER/DROP USER
	ROLESTMT_GROUP                          // CREATE/ALTER/DROP GROUP
)

func (r RoleStatementType) String() string {
	switch r {
	case ROLESTMT_ROLE:
		return "ROLE"
	case ROLESTMT_USER:
		return "USER"
	case ROLESTMT_GROUP:
		return "GROUP"
	default:
		return fmt.Sprintf("RoleStatementType(%d)", int(r))
	}
}

// CreateRoleStmt represents a CREATE ROLE/USER/GROUP statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3081
type CreateRoleStmt struct {
	BaseNode
	StmtType RoleStatementType // Role type: ROLE, USER, or GROUP - postgres/src/include/nodes/parsenodes.h:3084
	Role     string            // Role name - postgres/src/include/nodes/parsenodes.h:3085
	Options  *NodeList         // List of DefElem nodes - postgres/src/include/nodes/parsenodes.h:3086
}

// NewCreateRoleStmt creates a new CREATE ROLE statement.
func NewCreateRoleStmt(stmtType RoleStatementType, role string, options *NodeList) *CreateRoleStmt {
	return &CreateRoleStmt{
		BaseNode: BaseNode{Tag: T_CreateRoleStmt},
		StmtType: stmtType,
		Role:     role,
		Options:  options,
	}
}

func (crs *CreateRoleStmt) String() string {
	optionCount := 0
	if crs.Options != nil {
		optionCount = len(crs.Options.Items)
	}
	return fmt.Sprintf("CreateRoleStmt(%s %s, %d options)@%d", crs.StmtType, crs.Role, optionCount, crs.Location())
}

func (crs *CreateRoleStmt) StatementType() string {
	return fmt.Sprintf("CREATE_%s", crs.StmtType)
}

func (crs *CreateRoleStmt) SqlString() string {
	var parts []string

	parts = append(parts, "CREATE")
	parts = append(parts, crs.StmtType.String())
	parts = append(parts, crs.Role)

	// Add options if present
	if crs.Options != nil && len(crs.Options.Items) > 0 {
		var optionParts []string
		for _, item := range crs.Options.Items {
			if defElem, ok := item.(*DefElem); ok {
				optionParts = append(optionParts, formatRoleOption(defElem))
			}
		}
		if len(optionParts) > 0 {
			parts = append(parts, "WITH")
			parts = append(parts, strings.Join(optionParts, " "))
		}
	}

	return strings.Join(parts, " ")
}

// formatRoleOption formats a DefElem as a role option without the = sign
func formatRoleOption(d *DefElem) string {
	// Handle special role options that need transformation from internal names to SQL syntax
	switch strings.ToLower(d.Defname) {
	case "canlogin":
		if boolVal, ok := d.Arg.(*Boolean); ok {
			if boolVal.BoolVal {
				return "LOGIN"
			} else {
				return "NOLOGIN"
			}
		}
	case "inherit":
		if boolVal, ok := d.Arg.(*Boolean); ok {
			if boolVal.BoolVal {
				return "INHERIT"
			} else {
				return "NOINHERIT"
			}
		}
	case "createrole":
		if boolVal, ok := d.Arg.(*Boolean); ok {
			if boolVal.BoolVal {
				return "CREATEROLE"
			} else {
				return "NOCREATEROLE"
			}
		}
	case "createdb":
		if boolVal, ok := d.Arg.(*Boolean); ok {
			if boolVal.BoolVal {
				return "CREATEDB"
			} else {
				return "NOCREATEDB"
			}
		}
	case "isreplication":
		if boolVal, ok := d.Arg.(*Boolean); ok {
			if boolVal.BoolVal {
				return "REPLICATION"
			} else {
				return "NOREPLICATION"
			}
		}
	case "issuperuser", "superuser":
		if boolVal, ok := d.Arg.(*Boolean); ok {
			if boolVal.BoolVal {
				return "SUPERUSER"
			} else {
				return "NOSUPERUSER"
			}
		}
	case "connectionlimit":
		if d.Arg != nil {
			return fmt.Sprintf("CONNECTION LIMIT %s", d.Arg.SqlString())
		}
	case "validuntil":
		if d.Arg != nil {
			return fmt.Sprintf("VALID UNTIL %s", d.Arg.SqlString())
		}
	case "addroleto":
		if d.Arg != nil {
			return fmt.Sprintf("IN ROLE %s", d.Arg.SqlString())
		}
	case "rolemembers":
		if d.Arg != nil {
			return fmt.Sprintf("ROLE %s", d.Arg.SqlString())
		}
	case "adminmembers":
		if d.Arg != nil {
			return fmt.Sprintf("ADMIN %s", d.Arg.SqlString())
		}
	case "password":
		if d.Arg != nil {
			return fmt.Sprintf("PASSWORD %s", d.Arg.SqlString())
		}
	}

	// Default formatting for other options
	if d.Arg != nil {
		argStr := d.Arg.SqlString()
		return fmt.Sprintf("%s %s", strings.ToUpper(d.Defname), argStr)
	}
	return strings.ToUpper(d.Defname)
}

// formatTransactionOption formats a DefElem as a transaction option
func formatTransactionOption(d *DefElem) string {
	// Handle special transaction options that need transformation from internal names to SQL syntax
	switch strings.ToLower(d.Defname) {
	case "transaction_isolation":
		if d.Arg != nil {
			// Special handling for isolation levels - don't quote them
			if strNode, ok := d.Arg.(*String); ok {
				return fmt.Sprintf("ISOLATION LEVEL %s", strings.ToUpper(strNode.SVal))
			}
			return fmt.Sprintf("ISOLATION LEVEL %s", d.Arg.SqlString())
		}
	case "transaction_read_only":
		if boolVal, ok := d.Arg.(*Boolean); ok {
			if boolVal.BoolVal {
				return "READ ONLY"
			} else {
				return "READ WRITE"
			}
		}
	case "transaction_deferrable":
		if boolVal, ok := d.Arg.(*Boolean); ok {
			if boolVal.BoolVal {
				return "DEFERRABLE"
			} else {
				return "NOT DEFERRABLE"
			}
		}
	}

	// Default formatting for other transaction options
	if d.Arg != nil {
		return fmt.Sprintf("%s %s", strings.ToUpper(d.Defname), d.Arg.SqlString())
	}
	return strings.ToUpper(d.Defname)
}

// AlterRoleStmt represents an ALTER ROLE/USER/GROUP statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3089
type AlterRoleStmt struct {
	BaseNode
	Role    *RoleSpec // Role to alter - postgres/src/include/nodes/parsenodes.h:3092
	Options *NodeList // List of DefElem nodes - postgres/src/include/nodes/parsenodes.h:3093
	Action  int       // +1 = add members, -1 = drop members - postgres/src/include/nodes/parsenodes.h:3094
}

// NewAlterRoleStmt creates a new ALTER ROLE statement.
func NewAlterRoleStmt(role *RoleSpec, options *NodeList) *AlterRoleStmt {
	return &AlterRoleStmt{
		BaseNode: BaseNode{Tag: T_AlterRoleStmt},
		Role:     role,
		Options:  options,
		Action:   0,
	}
}

func (ars *AlterRoleStmt) String() string {
	roleName := ""
	if ars.Role != nil {
		roleName = ars.Role.Rolename
	}
	optionCount := 0
	if ars.Options != nil {
		optionCount = len(ars.Options.Items)
	}
	return fmt.Sprintf("AlterRoleStmt(%s, %d options)@%d", roleName, optionCount, ars.Location())
}

func (ars *AlterRoleStmt) StatementType() string {
	return "ALTER_ROLE"
}

func (ars *AlterRoleStmt) SqlString() string {
	var parts []string

	// Check if this is an ALTER GROUP statement by examining the options for rolemembers
	isAlterGroup := false
	if ars.Options != nil {
		for _, item := range ars.Options.Items {
			if defElem, ok := item.(*DefElem); ok && defElem.Defname == "rolemembers" {
				isAlterGroup = true
				break
			}
		}
	}

	if isAlterGroup {
		// ALTER GROUP ADD/DROP USER statement
		parts = append(parts, "ALTER", "GROUP")
		if ars.Role != nil {
			parts = append(parts, ars.Role.Rolename)
		}

		// Add ADD/DROP USER action
		if ars.Action == 1 { // ADD
			parts = append(parts, "ADD", "USER")
		} else { // DROP
			parts = append(parts, "DROP", "USER")
		}

		// Add role list from options
		if ars.Options != nil && len(ars.Options.Items) > 0 {
			var roleNames []string
			for _, item := range ars.Options.Items {
				if defElem, ok := item.(*DefElem); ok && defElem.Defname == "rolemembers" {
					if nodeList, ok := defElem.Arg.(*NodeList); ok {
						for _, roleItem := range nodeList.Items {
							if roleSpec, ok := roleItem.(*RoleSpec); ok {
								roleNames = append(roleNames, roleSpec.Rolename)
							}
						}
					}
				}
			}
			if len(roleNames) > 0 {
				parts = append(parts, strings.Join(roleNames, ", "))
			}
		}
	} else {
		// Regular ALTER ROLE/USER
		parts = append(parts, "ALTER", "ROLE")
		if ars.Role != nil {
			parts = append(parts, ars.Role.Rolename)
		}

		// Add options if present
		if ars.Options != nil && len(ars.Options.Items) > 0 {
			var optionParts []string
			for _, item := range ars.Options.Items {
				if defElem, ok := item.(*DefElem); ok {
					optionParts = append(optionParts, formatRoleOption(defElem))
				}
			}
			if len(optionParts) > 0 {
				parts = append(parts, "WITH")
				parts = append(parts, strings.Join(optionParts, " "))
			}
		}
	}

	return strings.Join(parts, " ")
}

// AlterRoleSetStmt represents an ALTER ROLE/USER SET/RESET statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3097
type AlterRoleSetStmt struct {
	BaseNode
	Role     *RoleSpec        // Role to modify - postgres/src/include/nodes/parsenodes.h:3100
	Database string           // Database name, or empty string - postgres/src/include/nodes/parsenodes.h:3101
	Setstmt  *VariableSetStmt // SET or RESET subcommand - postgres/src/include/nodes/parsenodes.h:3102
}

// NewAlterRoleSetStmt creates a new ALTER ROLE SET statement.
func NewAlterRoleSetStmt(role *RoleSpec, database string, setstmt *VariableSetStmt) *AlterRoleSetStmt {
	return &AlterRoleSetStmt{
		BaseNode: BaseNode{Tag: T_AlterRoleSetStmt},
		Role:     role,
		Database: database,
		Setstmt:  setstmt,
	}
}

func (arss *AlterRoleSetStmt) String() string {
	roleName := "ALL"
	if arss.Role != nil {
		roleName = arss.Role.Rolename
	}
	dbInfo := ""
	if arss.Database != "" {
		dbInfo = fmt.Sprintf(" IN DATABASE %s", arss.Database)
	}
	return fmt.Sprintf("AlterRoleSetStmt(%s%s)@%d", roleName, dbInfo, arss.Location())
}

func (arss *AlterRoleSetStmt) StatementType() string {
	return "ALTER_ROLE_SET"
}

func (arss *AlterRoleSetStmt) SqlString() string {
	var parts []string

	parts = append(parts, "ALTER", "ROLE")

	if arss.Role != nil {
		parts = append(parts, QuoteIdentifier(arss.Role.Rolename))
	} else {
		parts = append(parts, "ALL")
	}

	if arss.Database != "" {
		parts = append(parts, "IN", "DATABASE", arss.Database)
	}

	if arss.Setstmt != nil {
		// Format the SET/RESET clause specifically for ALTER ROLE
		if arss.Setstmt.Kind == VAR_SET_VALUE {
			parts = append(parts, "SET", arss.Setstmt.Name)
			if arss.Setstmt.Args != nil && arss.Setstmt.Args.Len() > 0 {
				parts = append(parts, "TO")
				argParts := []string{}
				for _, arg := range arss.Setstmt.Args.Items {
					// Format arguments using keyword-aware quoting
					if strNode, ok := arg.(*String); ok {
						val := strNode.SVal
						// For SET values, prefer unquoted identifiers when possible
						if !shouldQuoteValue(val) {
							argParts = append(argParts, val)
						} else {
							argParts = append(argParts, QuoteStringLiteral(val))
						}
					} else {
						argParts = append(argParts, arg.SqlString())
					}
				}
				parts = append(parts, strings.Join(argParts, ", "))
			}
		} else if arss.Setstmt.Kind == VAR_SET_DEFAULT {
			parts = append(parts, "SET", arss.Setstmt.Name, "TO", "DEFAULT")
		} else if arss.Setstmt.Kind == VAR_SET_CURRENT {
			parts = append(parts, "SET", arss.Setstmt.Name, "FROM", "CURRENT")
		} else if arss.Setstmt.Kind == VAR_SET_MULTI {
			// For multi-valued SET like search_path
			parts = append(parts, "SET", arss.Setstmt.Name)
			if arss.Setstmt.Args != nil && arss.Setstmt.Args.Len() > 0 {
				parts = append(parts, "TO")
				argParts := []string{}
				for _, arg := range arss.Setstmt.Args.Items {
					// Format arguments using keyword-aware quoting
					if strNode, ok := arg.(*String); ok {
						val := strNode.SVal
						// For SET values, prefer unquoted identifiers when possible
						if !shouldQuoteValue(val) {
							argParts = append(argParts, val)
						} else {
							argParts = append(argParts, QuoteStringLiteral(val))
						}
					} else {
						argParts = append(argParts, arg.SqlString())
					}
				}
				parts = append(parts, strings.Join(argParts, ", "))
			}
		} else if arss.Setstmt.Kind == VAR_RESET {
			parts = append(parts, "RESET", arss.Setstmt.Name)
		} else if arss.Setstmt.Kind == VAR_RESET_ALL {
			parts = append(parts, "RESET", "ALL")
		}
	}

	return strings.Join(parts, " ")
}

// DropRoleStmt represents a DROP ROLE/USER/GROUP statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3105
type DropRoleStmt struct {
	BaseNode
	Roles     *NodeList // List of roles to remove - postgres/src/include/nodes/parsenodes.h:3108
	MissingOk bool      // Skip error if a role is missing? - postgres/src/include/nodes/parsenodes.h:3109
}

// NewDropRoleStmt creates a new DROP ROLE statement.
func NewDropRoleStmt(roles *NodeList, missingOk bool) *DropRoleStmt {
	return &DropRoleStmt{
		BaseNode:  BaseNode{Tag: T_DropRoleStmt},
		Roles:     roles,
		MissingOk: missingOk,
	}
}

func (drs *DropRoleStmt) String() string {
	ifExists := ""
	if drs.MissingOk {
		ifExists = "IF EXISTS "
	}
	roleCount := 0
	if drs.Roles != nil {
		roleCount = len(drs.Roles.Items)
	}
	return fmt.Sprintf("DropRoleStmt(%s%d roles)@%d", ifExists, roleCount, drs.Location())
}

func (drs *DropRoleStmt) StatementType() string {
	return "DROP_ROLE"
}

func (drs *DropRoleStmt) SqlString() string {
	var parts []string

	parts = append(parts, "DROP", "ROLE")

	if drs.MissingOk {
		parts = append(parts, "IF", "EXISTS")
	}

	// Add role names
	if drs.Roles != nil && len(drs.Roles.Items) > 0 {
		var roleNames []string
		for _, item := range drs.Roles.Items {
			if roleSpec, ok := item.(*RoleSpec); ok {
				roleNames = append(roleNames, roleSpec.Rolename)
			}
		}
		if len(roleNames) > 0 {
			parts = append(parts, strings.Join(roleNames, ", "))
		}
	}

	return strings.Join(parts, " ")
}

// ==============================================================================
// CONFIGURATION STATEMENTS - SET/SHOW/RESET - PostgreSQL parsenodes.h:2608-2631
// ==============================================================================

// VariableSetKind represents the kind of variable setting operation.
// Ported from postgres/src/include/nodes/parsenodes.h:2608-2616
type VariableSetKind int

const (
	VAR_SET_VALUE   VariableSetKind = iota // SET variable = value
	VAR_SET_DEFAULT                        // SET variable TO DEFAULT
	VAR_SET_CURRENT                        // SET variable FROM CURRENT
	VAR_SET_MULTI                          // SET TRANSACTION CHARACTERISTICS
	VAR_RESET                              // RESET variable
	VAR_RESET_ALL                          // RESET ALL
)

func (v VariableSetKind) String() string {
	switch v {
	case VAR_SET_VALUE:
		return "SET_VALUE"
	case VAR_SET_DEFAULT:
		return "SET_DEFAULT"
	case VAR_SET_CURRENT:
		return "SET_CURRENT"
	case VAR_SET_MULTI:
		return "SET_MULTI"
	case VAR_RESET:
		return "RESET"
	case VAR_RESET_ALL:
		return "RESET_ALL"
	default:
		return fmt.Sprintf("VariableSetKind(%d)", int(v))
	}
}

// VariableSetStmt represents a SET/RESET statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2618
type VariableSetStmt struct {
	BaseNode
	Kind    VariableSetKind // What kind of SET command - postgres/src/include/nodes/parsenodes.h:2621
	Name    string          // Variable name - postgres/src/include/nodes/parsenodes.h:2622
	Args    *NodeList       // List of A_Const nodes - postgres/src/include/nodes/parsenodes.h:2623
	IsLocal bool            // SET LOCAL? - postgres/src/include/nodes/parsenodes.h:2624
}

// NewVariableSetStmt creates a new SET statement.
func NewVariableSetStmt(kind VariableSetKind, name string, args *NodeList, isLocal bool) *VariableSetStmt {
	return &VariableSetStmt{
		BaseNode: BaseNode{Tag: T_VariableSetStmt},
		Kind:     kind,
		Name:     name,
		Args:     args,
		IsLocal:  isLocal,
	}
}

// NewSetStmt creates a new SET variable statement.
func NewSetStmt(name string, args *NodeList) *VariableSetStmt {
	return NewVariableSetStmt(VAR_SET_VALUE, name, args, false)
}

// NewResetStmt creates a new RESET statement.
func NewResetStmt(name string) *VariableSetStmt {
	return NewVariableSetStmt(VAR_RESET, name, nil, false)
}

// SqlString returns the SQL representation of the SET statement
func (v *VariableSetStmt) SqlString() string {
	var parts []string

	// Add SET
	parts = append(parts, "SET")

	// Add LOCAL if applicable
	if v.IsLocal {
		parts = append(parts, "LOCAL")
	}

	// Handle different kinds of SET statements
	switch v.Kind {
	case VAR_SET_VALUE:
		// Handle specific PostgreSQL SET variants
		switch v.Name {
		case "timezone":
			parts = append(parts, "TIME", "ZONE")
		case "catalog":
			parts = append(parts, "CATALOG")
		case "search_path":
			// For single schema, use SET SCHEMA syntax (more idiomatic)
			// For multiple schemas, use SET search_path = syntax
			if v.Args != nil && v.Args.Len() == 1 {
				parts = append(parts, "SCHEMA")
			} else {
				parts = append(parts, "search_path")
			}
		case "client_encoding":
			parts = append(parts, "NAMES")
		case "role":
			parts = append(parts, "ROLE")
		case "session_authorization":
			parts = append(parts, "SESSION", "AUTHORIZATION")
		case "xmloption":
			parts = append(parts, "XML", "OPTION")
		case "transaction_snapshot":
			parts = append(parts, "TRANSACTION", "SNAPSHOT")
		default:
			// Generic variable name
			parts = append(parts, v.Name)
		}

		// Add values (syntax depends on the specific SET variant)
		if v.Args != nil && v.Args.Len() > 0 {
			// For special cases that use specific syntax
			if v.Name == "xmloption" {
				// XML OPTION uses the value directly (DOCUMENT/CONTENT)
				if str, ok := v.Args.Items[0].(*String); ok {
					parts = append(parts, strings.ToUpper(str.SVal))
				}
			} else if v.Name == "timezone" || v.Name == "catalog" ||
				v.Name == "client_encoding" || v.Name == "role" ||
				v.Name == "session_authorization" || v.Name == "transaction_snapshot" ||
				(v.Name == "search_path" && v.Args != nil && v.Args.Len() == 1) {
				// PostgreSQL-specific forms don't use = (e.g., SET TIME ZONE 'UTC', SET SCHEMA 'public')
				var values []string
				for _, arg := range v.Args.Items {
					if str, ok := arg.(*String); ok {
						if needsQuoting(str.SVal) {
							values = append(values, "'"+strings.ReplaceAll(str.SVal, "'", "''")+"'")
						} else {
							values = append(values, str.SVal)
						}
					} else if integer, ok := arg.(*Integer); ok {
						values = append(values, fmt.Sprintf("%d", integer.IVal))
					} else {
						values = append(values, arg.SqlString())
					}
				}
				if len(values) > 0 {
					parts = append(parts, strings.Join(values, ", "))
				}
			} else {
				// Generic variable: add = and values
				parts = append(parts, "=")
				var values []string
				for _, arg := range v.Args.Items {
					if str, ok := arg.(*String); ok {
						if needsQuoting(str.SVal) {
							values = append(values, "'"+strings.ReplaceAll(str.SVal, "'", "''")+"'")
						} else {
							values = append(values, str.SVal)
						}
					} else if integer, ok := arg.(*Integer); ok {
						values = append(values, fmt.Sprintf("%d", integer.IVal))
					} else {
						values = append(values, arg.SqlString())
					}
				}
				if len(values) > 0 {
					parts = append(parts, strings.Join(values, ", "))
				}
			}
		} else if v.Name == "client_encoding" {
			// SET NAMES without arguments is valid
		}

	case VAR_SET_DEFAULT:
		// Handle SET var = DEFAULT or SET SESSION AUTHORIZATION DEFAULT
		if v.Name == "session_authorization" {
			parts = append(parts, "SESSION", "AUTHORIZATION", "DEFAULT")
		} else {
			parts = append(parts, v.Name, "=", "DEFAULT")
		}

	case VAR_SET_CURRENT:
		// Handle SET var FROM CURRENT
		parts = append(parts, v.Name, "FROM", "CURRENT")

	case VAR_RESET:
		// Handle RESET (this would be a different statement type normally)
		parts[0] = "RESET" // Replace SET with RESET
		parts = append(parts, v.Name)

	case VAR_RESET_ALL:
		// Handle RESET ALL
		parts[0] = "RESET" // Replace SET with RESET
		parts = append(parts, "ALL")
	}

	return strings.Join(parts, " ")
}

// needsQuoting determines if a string value needs to be quoted
func needsQuoting(value string) bool {
	// Don't quote certain special values that are keywords
	upper := strings.ToUpper(value)
	switch upper {
	case "TRUE", "FALSE", "ON", "OFF", "DEFAULT":
		return false
	}

	// Quote if contains spaces, special characters, or non-ASCII
	if strings.ContainsAny(value, " '\"`\\,;()[]{}") {
		return true
	}

	// Check if it's a simple number (integer or float)
	if _, err := fmt.Sscanf(value, "%f", new(float64)); err == nil {
		// It's a number, don't quote
		return false
	}

	// For anything else (including identifiers), quote it
	// This is safer and matches PostgreSQL behavior for SET values
	return true
}

func (vss *VariableSetStmt) String() string {
	scope := ""
	if vss.IsLocal {
		scope = "LOCAL "
	}
	return fmt.Sprintf("VariableSetStmt(%s%s %s)@%d", scope, vss.Kind, vss.Name, vss.Location())
}

func (vss *VariableSetStmt) StatementType() string {
	if vss.Kind == VAR_RESET || vss.Kind == VAR_RESET_ALL {
		return "RESET"
	}
	return "SET"
}

// VariableShowStmt represents a SHOW statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2631
type VariableShowStmt struct {
	BaseNode
	Name string // Variable name, or "all" - postgres/src/include/nodes/parsenodes.h:2634
}

// NewVariableShowStmt creates a new SHOW statement.
func NewVariableShowStmt(name string) *VariableShowStmt {
	return &VariableShowStmt{
		BaseNode: BaseNode{Tag: T_VariableShowStmt},
		Name:     name,
	}
}

func (vss *VariableShowStmt) String() string {
	return fmt.Sprintf("VariableShowStmt(%s)@%d", vss.Name, vss.Location())
}

func (vss *VariableShowStmt) StatementType() string {
	return "SHOW"
}

// ==============================================================================
// QUERY ANALYSIS STATEMENTS - EXPLAIN/PREPARE/EXECUTE - PostgreSQL parsenodes.h:3868-4070
// ==============================================================================

// ExplainStmt represents an EXPLAIN statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3868
type ExplainStmt struct {
	BaseNode
	Query   Node       // The query to explain - postgres/src/include/nodes/parsenodes.h:3871
	Options []*DefElem // List of DefElem nodes - postgres/src/include/nodes/parsenodes.h:3872
}

// NewExplainStmt creates a new EXPLAIN statement.
func NewExplainStmt(query Node, options []*DefElem) *ExplainStmt {
	return &ExplainStmt{
		BaseNode: BaseNode{Tag: T_ExplainStmt},
		Query:    query,
		Options:  options,
	}
}

func (es *ExplainStmt) String() string {
	return fmt.Sprintf("ExplainStmt(%d options)@%d", len(es.Options), es.Location())
}

func (es *ExplainStmt) StatementType() string {
	return "EXPLAIN"
}

// PrepareStmt represents a PREPARE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:4030
type PrepareStmt struct {
	BaseNode
	Name     string      // Statement name - postgres/src/include/nodes/parsenodes.h:4033
	Argtypes []*TypeName // List of TypeName nodes (specified arg types) - postgres/src/include/nodes/parsenodes.h:4034
	Query    Node        // Statement to prepare - postgres/src/include/nodes/parsenodes.h:4035
}

// NewPrepareStmt creates a new PREPARE statement.
func NewPrepareStmt(name string, argtypes []*TypeName, query Node) *PrepareStmt {
	return &PrepareStmt{
		BaseNode: BaseNode{Tag: T_PrepareStmt},
		Name:     name,
		Argtypes: argtypes,
		Query:    query,
	}
}

func (ps *PrepareStmt) String() string {
	return fmt.Sprintf("PrepareStmt(%s, %d argtypes)@%d", ps.Name, len(ps.Argtypes), ps.Location())
}

func (ps *PrepareStmt) StatementType() string {
	return "PREPARE"
}

// ExecuteStmt represents an EXECUTE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:4044
type ExecuteStmt struct {
	BaseNode
	Name   string    // Statement name - postgres/src/include/nodes/parsenodes.h:4047
	Params *NodeList // List of parameter expressions - postgres/src/include/nodes/parsenodes.h:4048
}

// NewExecuteStmt creates a new EXECUTE statement.
func NewExecuteStmt(name string, params *NodeList) *ExecuteStmt {
	return &ExecuteStmt{
		BaseNode: BaseNode{Tag: T_ExecuteStmt},
		Name:     name,
		Params:   params,
	}
}

func (es *ExecuteStmt) String() string {
	paramCount := 0
	if es.Params != nil {
		paramCount = len(es.Params.Items)
	}
	return fmt.Sprintf("ExecuteStmt(%s, %d params)@%d", es.Name, paramCount, es.Location())
}

func (es *ExecuteStmt) StatementType() string {
	return "EXECUTE"
}

// DeallocateStmt represents a DEALLOCATE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:4056
type DeallocateStmt struct {
	BaseNode
	Name  string // Statement name, or NULL for all - postgres/src/include/nodes/parsenodes.h:4060
	IsAll bool   // True if DEALLOCATE ALL - postgres/src/include/nodes/parsenodes.h:4067
}

// NewDeallocateStmt creates a new DEALLOCATE statement.
func NewDeallocateStmt(name string) *DeallocateStmt {
	return &DeallocateStmt{
		BaseNode: BaseNode{Tag: T_DeallocateStmt},
		Name:     name,
		IsAll:    false,
	}
}

// NewDeallocateAllStmt creates a new DEALLOCATE ALL statement.
func NewDeallocateAllStmt() *DeallocateStmt {
	return &DeallocateStmt{
		BaseNode: BaseNode{Tag: T_DeallocateStmt},
		Name:     "", // Empty name means ALL
		IsAll:    true,
	}
}

func (ds *DeallocateStmt) String() string {
	name := ds.Name
	if name == "" {
		name = "ALL"
	}
	return fmt.Sprintf("DeallocateStmt(%s)@%d", name, ds.Location())
}

func (ds *DeallocateStmt) StatementType() string {
	return "DEALLOCATE"
}

// ==============================================================================
// DATA TRANSFER STATEMENTS - COPY - PostgreSQL parsenodes.h:2586-2599
// ==============================================================================

// CopyStmt represents a COPY statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2586
type CopyStmt struct {
	BaseNode
	Relation    *RangeVar // Relation to copy - postgres/src/include/nodes/parsenodes.h:2589
	Query       Node      // Query to copy (SELECT/INSERT/UPDATE/DELETE) - postgres/src/include/nodes/parsenodes.h:2590
	Attlist     *NodeList // List of column names (or NIL for all columns) - postgres/src/include/nodes/parsenodes.h:2591
	IsFrom      bool      // TO or FROM - postgres/src/include/nodes/parsenodes.h:2592
	IsProgram   bool      // Is 'filename' a program to popen? - postgres/src/include/nodes/parsenodes.h:2593
	Filename    string    // Filename, or NULL for STDIN/STDOUT - postgres/src/include/nodes/parsenodes.h:2594
	Options     *NodeList // List of DefElem nodes - postgres/src/include/nodes/parsenodes.h:2595
	WhereClause Node      // WHERE condition (for COPY FROM WHERE) - postgres/src/include/nodes/parsenodes.h:2596
}

// NewCopyStmt creates a new COPY statement.
func NewCopyStmt(relation *RangeVar, query Node, isFrom bool) *CopyStmt {
	return &CopyStmt{
		BaseNode: BaseNode{Tag: T_CopyStmt},
		Relation: relation,
		Query:    query,
		IsFrom:   isFrom,
	}
}

// NewCopyFromStmt creates a new COPY FROM statement.
func NewCopyFromStmt(relation *RangeVar, filename string) *CopyStmt {
	return &CopyStmt{
		BaseNode: BaseNode{Tag: T_CopyStmt},
		Relation: relation,
		IsFrom:   true,
		Filename: filename,
	}
}

// NewCopyToStmt creates a new COPY TO statement.
func NewCopyToStmt(relation *RangeVar, filename string) *CopyStmt {
	return &CopyStmt{
		BaseNode: BaseNode{Tag: T_CopyStmt},
		Relation: relation,
		IsFrom:   false,
		Filename: filename,
	}
}

func (cs *CopyStmt) String() string {
	direction := "TO"
	if cs.IsFrom {
		direction = "FROM"
	}

	target := ""
	if cs.Relation != nil {
		target = cs.Relation.RelName
	} else if cs.Query != nil {
		target = "query"
	}

	return fmt.Sprintf("CopyStmt(%s %s %s)@%d", target, direction, cs.Filename, cs.Location())
}

func (cs *CopyStmt) StatementType() string {
	return "COPY"
}

// SqlString returns the SQL representation of the COPY statement
func (cs *CopyStmt) SqlString() string {
	var parts []string

	// Start with COPY
	parts = append(parts, "COPY")

	if cs.Relation != nil {
		// COPY table_name
		if cs.Relation.SchemaName != "" {
			parts = append(parts, cs.Relation.SchemaName+"."+cs.Relation.RelName)
		} else {
			parts = append(parts, cs.Relation.RelName)
		}

		// Add column list if specified
		if cs.Attlist != nil && cs.Attlist.Len() > 0 {
			var columns []string
			for _, item := range cs.Attlist.Items {
				if str, ok := item.(*String); ok {
					columns = append(columns, str.SVal)
				}
			}
			if len(columns) > 0 {
				parts = append(parts, "("+strings.Join(columns, ", ")+")")
			}
		}
	} else if cs.Query != nil {
		// COPY (query)
		parts = append(parts, "("+cs.Query.SqlString()+")")
	}

	// Add direction (FROM/TO)
	if cs.IsFrom {
		parts = append(parts, "FROM")
	} else {
		parts = append(parts, "TO")
	}

	// Add PROGRAM if specified
	if cs.IsProgram {
		parts = append(parts, "PROGRAM")
	}

	// Add filename or STDIN/STDOUT
	if cs.Filename == "" {
		if cs.IsFrom {
			parts = append(parts, "STDIN")
		} else {
			parts = append(parts, "STDOUT")
		}
	} else {
		parts = append(parts, "'"+cs.Filename+"'")
	}

	// Add options if any - always use modern parenthesized syntax
	if cs.Options != nil && cs.Options.Len() > 0 {
		var optionParts []string
		for _, item := range cs.Options.Items {
			if option, ok := item.(*DefElem); ok {
				optStr := formatCopyOption(option)
				if optStr != "" {
					optionParts = append(optionParts, optStr)
				}
			}
		}
		if len(optionParts) > 0 {
			parts = append(parts, "("+strings.Join(optionParts, ", ")+")")
		}
	}

	return strings.Join(parts, " ")
}

// formatCopyOption formats a single COPY option for the canonical parenthesized syntax
func formatCopyOption(option *DefElem) string {
	if option.Defname == "" {
		return ""
	}

	optionName := option.Defname

	// Handle different types of option arguments
	if option.Arg == nil {
		// Boolean option with no explicit value (defaults to true)
		return optionName
	}

	switch arg := option.Arg.(type) {
	case *String:
		if arg.SVal == "true" || arg.SVal == "on" || arg.SVal == "1" {
			return optionName + " true"
		} else if arg.SVal == "false" || arg.SVal == "off" || arg.SVal == "0" {
			return optionName + " false"
		} else if arg.SVal == "default" {
			return optionName + " default"
		} else {
			// String value - quote it
			return optionName + " '" + arg.SVal + "'"
		}
	case *Integer:
		return optionName + " " + fmt.Sprintf("%d", arg.IVal)
	case *Float:
		return optionName + " " + arg.FVal
	case *A_Star:
		return optionName + " *"
	case *NodeList:
		// Handle lists like force_quote (col1, col2)
		var listItems []string
		for _, item := range arg.Items {
			if str, ok := item.(*String); ok {
				listItems = append(listItems, str.SVal)
			}
		}
		if len(listItems) > 0 {
			return optionName + " (" + strings.Join(listItems, ", ") + ")"
		}
		return optionName
	default:
		// Fallback for other types
		if stringer, ok := arg.(interface{ SqlString() string }); ok {
			return optionName + " " + stringer.SqlString()
		}
		return optionName
	}
}

// ==============================================================================
// MAINTENANCE STATEMENTS - VACUUM/ANALYZE/REINDEX/CLUSTER - PostgreSQL parsenodes.h:3822-3982
// ==============================================================================

// VacuumStmt represents a VACUUM or ANALYZE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3837
type VacuumStmt struct {
	BaseNode
	Options     []*DefElem        // List of DefElem nodes - postgres/src/include/nodes/parsenodes.h:3840
	Rels        []*VacuumRelation // List of VacuumRelation, or NIL for all - postgres/src/include/nodes/parsenodes.h:3841
	IsVacuumcmd bool              // True for VACUUM, false for ANALYZE - postgres/src/include/nodes/parsenodes.h:3842
}

// VacuumRelation represents a relation in a VACUUM statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3845-3851
type VacuumRelation struct {
	BaseNode
	Relation *RangeVar // Relation to vacuum - postgres/src/include/nodes/parsenodes.h:3848
	Oid      uint32    // OID of relation, for RangeVar-less VacuumStmt - postgres/src/include/nodes/parsenodes.h:3849
	VaCols   []string  // List of column names, or NIL for all - postgres/src/include/nodes/parsenodes.h:3850
}

// NewVacuumStmt creates a new VACUUM statement.
func NewVacuumStmt(options []*DefElem, rels []*VacuumRelation) *VacuumStmt {
	return &VacuumStmt{
		BaseNode:    BaseNode{Tag: T_VacuumStmt},
		Options:     options,
		Rels:        rels,
		IsVacuumcmd: true,
	}
}

// NewAnalyzeStmt creates a new ANALYZE statement.
func NewAnalyzeStmt(options []*DefElem, rels []*VacuumRelation) *VacuumStmt {
	return &VacuumStmt{
		BaseNode:    BaseNode{Tag: T_VacuumStmt},
		Options:     options,
		Rels:        rels,
		IsVacuumcmd: false,
	}
}

// NewVacuumRelation creates a new VacuumRelation node.
func NewVacuumRelation(relation *RangeVar, vaCols []string) *VacuumRelation {
	return &VacuumRelation{
		BaseNode: BaseNode{Tag: T_VacuumRelation},
		Relation: relation,
		VaCols:   vaCols,
	}
}

func (vs *VacuumStmt) String() string {
	action := "ANALYZE"
	if vs.IsVacuumcmd {
		action = "VACUUM"
	}
	return fmt.Sprintf("VacuumStmt(%s, %d rels, %d options)@%d", action, len(vs.Rels), len(vs.Options), vs.Location())
}

func (vs *VacuumStmt) StatementType() string {
	if vs.IsVacuumcmd {
		return "VACUUM"
	}
	return "ANALYZE"
}

func (vr *VacuumRelation) String() string {
	relName := ""
	if vr.Relation != nil {
		relName = vr.Relation.RelName
	}
	return fmt.Sprintf("VacuumRelation(%s, %d cols)@%d", relName, len(vr.VaCols), vr.Location())
}

// ReindexObjectType represents the type of object to reindex.
// Ported from postgres/src/include/nodes/parsenodes.h:3965-3972
type ReindexObjectType int

const (
	REINDEX_OBJECT_INDEX    ReindexObjectType = iota // REINDEX INDEX
	REINDEX_OBJECT_TABLE                             // REINDEX TABLE
	REINDEX_OBJECT_SCHEMA                            // REINDEX SCHEMA
	REINDEX_OBJECT_SYSTEM                            // REINDEX SYSTEM
	REINDEX_OBJECT_DATABASE                          // REINDEX DATABASE
)

func (r ReindexObjectType) String() string {
	switch r {
	case REINDEX_OBJECT_INDEX:
		return "INDEX"
	case REINDEX_OBJECT_TABLE:
		return "TABLE"
	case REINDEX_OBJECT_SCHEMA:
		return "SCHEMA"
	case REINDEX_OBJECT_SYSTEM:
		return "SYSTEM"
	case REINDEX_OBJECT_DATABASE:
		return "DATABASE"
	default:
		return fmt.Sprintf("ReindexObjectType(%d)", int(r))
	}
}

// ReindexStmt represents a REINDEX statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3974
type ReindexStmt struct {
	BaseNode
	Kind     ReindexObjectType // Object type to reindex - postgres/src/include/nodes/parsenodes.h:3977
	Relation *RangeVar         // Table or index to reindex - postgres/src/include/nodes/parsenodes.h:3978
	Name     string            // Name of database to reindex - postgres/src/include/nodes/parsenodes.h:3979
	Params   []*DefElem        // List of DefElem nodes - postgres/src/include/nodes/parsenodes.h:3980
}

// NewReindexStmt creates a new REINDEX statement.
func NewReindexStmt(kind ReindexObjectType, relation *RangeVar, name string, params []*DefElem) *ReindexStmt {
	return &ReindexStmt{
		BaseNode: BaseNode{Tag: T_ReindexStmt},
		Kind:     kind,
		Relation: relation,
		Name:     name,
		Params:   params,
	}
}

// NewReindexIndexStmt creates a new REINDEX INDEX statement.
func NewReindexIndexStmt(relation *RangeVar) *ReindexStmt {
	return NewReindexStmt(REINDEX_OBJECT_INDEX, relation, "", nil)
}

// NewReindexTableStmt creates a new REINDEX TABLE statement.
func NewReindexTableStmt(relation *RangeVar) *ReindexStmt {
	return NewReindexStmt(REINDEX_OBJECT_TABLE, relation, "", nil)
}

// NewReindexDatabaseStmt creates a new REINDEX DATABASE statement.
func NewReindexDatabaseStmt(name string) *ReindexStmt {
	return NewReindexStmt(REINDEX_OBJECT_DATABASE, nil, name, nil)
}

func (rs *ReindexStmt) String() string {
	target := rs.Name
	if rs.Relation != nil {
		target = rs.Relation.RelName
	}
	return fmt.Sprintf("ReindexStmt(%s %s)@%d", rs.Kind, target, rs.Location())
}

func (rs *ReindexStmt) StatementType() string {
	return "REINDEX"
}

// ClusterStmt represents a CLUSTER statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3822-3828
type ClusterStmt struct {
	BaseNode
	Relation  *RangeVar  // Relation to cluster - postgres/src/include/nodes/parsenodes.h:3825
	Indexname string     // Index name or NULL - postgres/src/include/nodes/parsenodes.h:3826
	Params    []*DefElem // List of DefElem nodes - postgres/src/include/nodes/parsenodes.h:3827
}

// NewClusterStmt creates a new CLUSTER statement.
func NewClusterStmt(relation *RangeVar, indexname string, params []*DefElem) *ClusterStmt {
	return &ClusterStmt{
		BaseNode:  BaseNode{Tag: T_ClusterStmt},
		Relation:  relation,
		Indexname: indexname,
		Params:    params,
	}
}

func (cs *ClusterStmt) String() string {
	relName := ""
	if cs.Relation != nil {
		relName = cs.Relation.RelName
	}
	return fmt.Sprintf("ClusterStmt(%s ON %s)@%d", relName, cs.Indexname, cs.Location())
}

func (cs *ClusterStmt) StatementType() string {
	return "CLUSTER"
}

// ==============================================================================
// ADMINISTRATIVE STATEMENTS - PostgreSQL parsenodes.h:3914-3948
// ==============================================================================

// CheckPointStmt represents a CHECKPOINT statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3914-3917
type CheckPointStmt struct {
	BaseNode
}

// NewCheckPointStmt creates a new CHECKPOINT statement.
func NewCheckPointStmt() *CheckPointStmt {
	return &CheckPointStmt{
		BaseNode: BaseNode{Tag: T_CheckPointStmt},
	}
}

func (cps *CheckPointStmt) String() string {
	return fmt.Sprintf("CheckPointStmt@%d", cps.Location())
}

func (cps *CheckPointStmt) StatementType() string {
	return "CHECKPOINT"
}

// DiscardMode represents the mode for DISCARD statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3924-3930
type DiscardMode int

const (
	DISCARD_ALL       DiscardMode = iota // DISCARD ALL
	DISCARD_PLANS                        // DISCARD PLANS
	DISCARD_SEQUENCES                    // DISCARD SEQUENCES
	DISCARD_TEMP                         // DISCARD TEMP
)

func (d DiscardMode) String() string {
	switch d {
	case DISCARD_ALL:
		return "ALL"
	case DISCARD_PLANS:
		return "PLANS"
	case DISCARD_SEQUENCES:
		return "SEQUENCES"
	case DISCARD_TEMP:
		return "TEMP"
	default:
		return fmt.Sprintf("DiscardMode(%d)", int(d))
	}
}

// DiscardStmt represents a DISCARD statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3932-3936
type DiscardStmt struct {
	BaseNode
	Target DiscardMode // What to discard - postgres/src/include/nodes/parsenodes.h:3935
}

// NewDiscardStmt creates a new DISCARD statement.
func NewDiscardStmt(target DiscardMode) *DiscardStmt {
	return &DiscardStmt{
		BaseNode: BaseNode{Tag: T_DiscardStmt},
		Target:   target,
	}
}

func (ds *DiscardStmt) String() string {
	return fmt.Sprintf("DiscardStmt(%s)@%d", ds.Target, ds.Location())
}

func (ds *DiscardStmt) StatementType() string {
	return "DISCARD"
}

// LoadStmt represents a LOAD statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3755-3759
type LoadStmt struct {
	BaseNode
	Filename string // File to load - postgres/src/include/nodes/parsenodes.h:3758
}

// NewLoadStmt creates a new LOAD statement.
func NewLoadStmt(filename string) *LoadStmt {
	return &LoadStmt{
		BaseNode: BaseNode{Tag: T_LoadStmt},
		Filename: filename,
	}
}

func (ls *LoadStmt) String() string {
	return fmt.Sprintf("LoadStmt(%s)@%d", ls.Filename, ls.Location())
}

func (ls *LoadStmt) StatementType() string {
	return "LOAD"
}

// NotifyStmt represents a NOTIFY statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3622-3627
type NotifyStmt struct {
	BaseNode
	Conditionname string // Condition name - postgres/src/include/nodes/parsenodes.h:3625
	Payload       string // Optional payload string - postgres/src/include/nodes/parsenodes.h:3626
}

// NewNotifyStmt creates a new NOTIFY statement.
func NewNotifyStmt(conditionname, payload string) *NotifyStmt {
	return &NotifyStmt{
		BaseNode:      BaseNode{Tag: T_NotifyStmt},
		Conditionname: conditionname,
		Payload:       payload,
	}
}

func (ns *NotifyStmt) String() string {
	if ns.Payload != "" {
		return fmt.Sprintf("NotifyStmt(%s, '%s')@%d", ns.Conditionname, ns.Payload, ns.Location())
	}
	return fmt.Sprintf("NotifyStmt(%s)@%d", ns.Conditionname, ns.Location())
}

func (ns *NotifyStmt) StatementType() string {
	return "NOTIFY"
}

// ListenStmt represents a LISTEN statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3633-3637
type ListenStmt struct {
	BaseNode
	Conditionname string // Condition name to listen for - postgres/src/include/nodes/parsenodes.h:3636
}

// NewListenStmt creates a new LISTEN statement.
func NewListenStmt(conditionname string) *ListenStmt {
	return &ListenStmt{
		BaseNode:      BaseNode{Tag: T_ListenStmt},
		Conditionname: conditionname,
	}
}

func (ls *ListenStmt) String() string {
	return fmt.Sprintf("ListenStmt(%s)@%d", ls.Conditionname, ls.Location())
}

func (ls *ListenStmt) StatementType() string {
	return "LISTEN"
}

// UnlistenStmt represents an UNLISTEN statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3643
type UnlistenStmt struct {
	BaseNode
	Conditionname string // Condition name to stop listening for, or "*" for all - postgres/src/include/nodes/parsenodes.h:3650
}

// NewUnlistenStmt creates a new UNLISTEN statement.
func NewUnlistenStmt(conditionname string) *UnlistenStmt {
	return &UnlistenStmt{
		BaseNode:      BaseNode{Tag: T_UnlistenStmt},
		Conditionname: conditionname,
	}
}

// NewUnlistenAllStmt creates a new UNLISTEN * statement.
func NewUnlistenAllStmt() *UnlistenStmt {
	return &UnlistenStmt{
		BaseNode:      BaseNode{Tag: T_UnlistenStmt},
		Conditionname: "*",
	}
}

func (us *UnlistenStmt) String() string {
	return fmt.Sprintf("UnlistenStmt(%s)@%d", us.Conditionname, us.Location())
}

func (us *UnlistenStmt) StatementType() string {
	return "UNLISTEN"
}
