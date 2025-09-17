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

// SqlString returns the SQL representation of the GRANT/REVOKE statement
func (gs *GrantStmt) SqlString() string {
	var parts []string

	// Start with GRANT or REVOKE
	if gs.IsGrant {
		parts = append(parts, "GRANT")
	} else {
		parts = append(parts, "REVOKE")
	}

	// Add GRANT OPTION FOR if this is a grant option revoke
	if !gs.IsGrant && gs.GrantOption {
		parts = append(parts, "GRANT", "OPTION", "FOR")
	}

	// Add privileges
	if gs.Privileges == nil || len(gs.Privileges.Items) == 0 {
		parts = append(parts, "ALL")
	} else {
		var privParts []string
		for _, item := range gs.Privileges.Items {
			if priv, ok := item.(*AccessPriv); ok {
				privStr := strings.ToUpper(priv.PrivName)
				// Handle the case where privilege name is empty but has columns (ALL (cols))
				if privStr == "" && priv.Cols != nil && len(priv.Cols.Items) > 0 {
					privStr = "ALL"
				}
				if priv.Cols != nil && len(priv.Cols.Items) > 0 {
					var colNames []string
					for _, col := range priv.Cols.Items {
						if colStr, ok := col.(*String); ok {
							colNames = append(colNames, colStr.SVal)
						}
					}
					if len(colNames) > 0 {
						privStr += " (" + strings.Join(colNames, ", ") + ")"
					}
				}
				privParts = append(privParts, privStr)
			}
		}
		parts = append(parts, strings.Join(privParts, ", "))
	}

	// Add ON
	parts = append(parts, "ON")

	// Add target type specific keywords
	if gs.Targtype == ACL_TARGET_ALL_IN_SCHEMA {
		parts = append(parts, "ALL")
		switch gs.Objtype {
		case OBJECT_TABLE:
			parts = append(parts, "TABLES")
		case OBJECT_SEQUENCE:
			parts = append(parts, "SEQUENCES")
		case OBJECT_FUNCTION:
			parts = append(parts, "FUNCTIONS")
		case OBJECT_PROCEDURE:
			parts = append(parts, "PROCEDURES")
		case OBJECT_ROUTINE:
			parts = append(parts, "ROUTINES")
		}
		parts = append(parts, "IN", "SCHEMA")
	} else {
		// Add explicit object type if not table
		switch gs.Objtype {
		case OBJECT_SEQUENCE:
			parts = append(parts, "SEQUENCE")
		case OBJECT_DATABASE:
			parts = append(parts, "DATABASE")
		case OBJECT_SCHEMA:
			parts = append(parts, "SCHEMA")
		case OBJECT_FUNCTION:
			parts = append(parts, "FUNCTION")
		case OBJECT_PROCEDURE:
			parts = append(parts, "PROCEDURE")
		case OBJECT_TYPE:
			parts = append(parts, "TYPE")
		case OBJECT_LARGEOBJECT:
			parts = append(parts, "LARGE", "OBJECT")
		case OBJECT_TABLE:
			// TABLE is optional, but we can add it for clarity in certain contexts
			// parts = append(parts, "TABLE")
		}
	}

	// Add object names
	if gs.Objects != nil && len(gs.Objects.Items) > 0 {
		var objNames []string
		for _, obj := range gs.Objects.Items {
			if rangeVar, ok := obj.(*RangeVar); ok {
				name := rangeVar.RelName
				if rangeVar.SchemaName != "" {
					name = rangeVar.SchemaName + "." + name
				}
				objNames = append(objNames, name)
			} else if str, ok := obj.(*String); ok {
				objNames = append(objNames, str.SVal)
			} else if integer, ok := obj.(*Integer); ok {
				// For large objects and other numeric identifiers
				objNames = append(objNames, fmt.Sprintf("%d", integer.IVal))
			} else if objWithArgs, ok := obj.(*ObjectWithArgs); ok {
				// For functions, use the SqlString method
				objNames = append(objNames, objWithArgs.SqlString())
			} else if nodeList, ok := obj.(*NodeList); ok {
				// For types and other complex objects, extract names from the NodeList
				for _, item := range nodeList.Items {
					if str, ok := item.(*String); ok {
						objNames = append(objNames, str.SVal)
					}
				}
			}
		}
		parts = append(parts, strings.Join(objNames, ", "))
	}

	// Add TO/FROM
	if gs.IsGrant {
		parts = append(parts, "TO")
	} else {
		parts = append(parts, "FROM")
	}

	// Add grantees
	if gs.Grantees != nil && len(gs.Grantees.Items) > 0 {
		var granteeNames []string
		for _, grantee := range gs.Grantees.Items {
			if roleSpec, ok := grantee.(*RoleSpec); ok {
				granteeNames = append(granteeNames, roleSpec.SqlString())
			}
		}
		parts = append(parts, strings.Join(granteeNames, ", "))
	}

	// Add WITH GRANT OPTION for GRANT statements
	if gs.IsGrant && gs.GrantOption {
		parts = append(parts, "WITH", "GRANT", "OPTION")
	}

	// Add GRANTED BY
	if gs.Grantor != nil {
		parts = append(parts, "GRANTED", "BY", gs.Grantor.SqlString())
	}

	// Add CASCADE/RESTRICT for REVOKE statements
	if !gs.IsGrant && gs.Behavior != DropRestrict {
		switch gs.Behavior {
		case DropCascade:
			parts = append(parts, "CASCADE")
		}
	}

	return strings.Join(parts, " ")
}

// SqlString returns the SQL representation of the GRANT/REVOKE role statement
func (grs *GrantRoleStmt) SqlString() string {
	var parts []string

	// Start with GRANT or REVOKE
	if grs.IsGrant {
		parts = append(parts, "GRANT")
	} else {
		parts = append(parts, "REVOKE")
	}

	// Add role names
	if grs.GrantedRoles != nil && len(grs.GrantedRoles.Items) > 0 {
		var roleNames []string
		for _, role := range grs.GrantedRoles.Items {
			if roleSpec, ok := role.(*RoleSpec); ok {
				roleNames = append(roleNames, roleSpec.Rolename)
			} else if accessPriv, ok := role.(*AccessPriv); ok {
				roleNames = append(roleNames, accessPriv.PrivName)
			}
		}
		parts = append(parts, strings.Join(roleNames, ", "))
	}

	// Add TO/FROM
	if grs.IsGrant {
		parts = append(parts, "TO")
	} else {
		parts = append(parts, "FROM")
	}

	// Add grantee roles
	if grs.GranteeRoles != nil && len(grs.GranteeRoles.Items) > 0 {
		var granteeNames []string
		for _, grantee := range grs.GranteeRoles.Items {
			if roleSpec, ok := grantee.(*RoleSpec); ok {
				granteeNames = append(granteeNames, roleSpec.Rolename)
			}
		}
		parts = append(parts, strings.Join(granteeNames, ", "))
	}

	// Add options for both GRANT and REVOKE role statements
	if grs.Opt != nil && len(grs.Opt.Items) > 0 {
		var optParts []string
		for _, opt := range grs.Opt.Items {
			if defElem, ok := opt.(*DefElem); ok {
				optStr := strings.ToUpper(defElem.Defname)
				if defElem.Arg != nil {
					if boolVal, ok := defElem.Arg.(*Boolean); ok {
						// For admin, set, and inherit options, use "OPTION" suffix for both GRANT and REVOKE
						if defElem.Defname == "admin" || defElem.Defname == "set" || defElem.Defname == "inherit" {
							optStr += " OPTION"
						} else if boolVal.BoolVal {
							optStr += " TRUE"
						} else {
							optStr += " FALSE"
						}
					} else {
						optStr += " " + defElem.Arg.SqlString()
					}
				}
				optParts = append(optParts, optStr)
			}
		}
		if len(optParts) > 0 {
			if grs.IsGrant {
				parts = append(parts, "WITH", strings.Join(optParts, ", "))
			} else {
				// For REVOKE, options come before the role names
				// Insert after REVOKE but before role names
				parts = []string{"REVOKE", strings.Join(optParts, ", "), "FOR"}
				// Add role names
				if grs.GrantedRoles != nil && len(grs.GrantedRoles.Items) > 0 {
					var roleNames []string
					for _, role := range grs.GrantedRoles.Items {
						if roleSpec, ok := role.(*RoleSpec); ok {
							roleNames = append(roleNames, roleSpec.Rolename)
						} else if accessPriv, ok := role.(*AccessPriv); ok {
							roleNames = append(roleNames, accessPriv.PrivName)
						}
					}
					parts = append(parts, strings.Join(roleNames, ", "))
				}
				// Add FROM
				parts = append(parts, "FROM")
				// Add grantee roles
				if grs.GranteeRoles != nil && len(grs.GranteeRoles.Items) > 0 {
					var granteeNames []string
					for _, grantee := range grs.GranteeRoles.Items {
						if roleSpec, ok := grantee.(*RoleSpec); ok {
							granteeNames = append(granteeNames, roleSpec.Rolename)
						}
					}
					parts = append(parts, strings.Join(granteeNames, ", "))
				}
				// Add GRANTED BY
				if grs.Grantor != nil {
					parts = append(parts, "GRANTED", "BY", grs.Grantor.SqlString())
				}
				// Add CASCADE/RESTRICT for REVOKE statements
				if grs.Behavior != DropRestrict {
					switch grs.Behavior {
					case DropCascade:
						parts = append(parts, "CASCADE")
					}
				}
				return strings.Join(parts, " ")
			}
		}
	}

	// Add GRANTED BY
	if grs.Grantor != nil {
		parts = append(parts, "GRANTED", "BY", grs.Grantor.SqlString())
	}

	// Add CASCADE/RESTRICT for REVOKE statements
	if !grs.IsGrant && grs.Behavior != DropRestrict {
		switch grs.Behavior {
		case DropCascade:
			parts = append(parts, "CASCADE")
		}
	}

	return strings.Join(parts, " ")
}

// ==============================================================================
// ALTER DEFAULT PRIVILEGES STATEMENT - PostgreSQL parsenodes.h:2571-2576
// ==============================================================================

// AlterDefaultPrivilegesStmt represents ALTER DEFAULT PRIVILEGES statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2571
type AlterDefaultPrivilegesStmt struct {
	BaseNode
	Options *NodeList  // List of DefElem - postgres/src/include/nodes/parsenodes.h:2574
	Action  *GrantStmt // GRANT/REVOKE action (with objects=NIL) - postgres/src/include/nodes/parsenodes.h:2575
}

// NewAlterDefaultPrivilegesStmt creates a new ALTER DEFAULT PRIVILEGES statement.
func NewAlterDefaultPrivilegesStmt(options *NodeList, action *GrantStmt) *AlterDefaultPrivilegesStmt {
	return &AlterDefaultPrivilegesStmt{
		BaseNode: BaseNode{Tag: T_AlterDefaultPrivilegesStmt},
		Options:  options,
		Action:   action,
	}
}

func (adps *AlterDefaultPrivilegesStmt) String() string {
	optionsCount := 0
	if adps.Options != nil {
		optionsCount = len(adps.Options.Items)
	}
	action := "REVOKE"
	if adps.Action != nil && adps.Action.IsGrant {
		action = "GRANT"
	}
	return fmt.Sprintf("AlterDefaultPrivilegesStmt(%s with %d options)@%d", action, optionsCount, adps.Location())
}

func (adps *AlterDefaultPrivilegesStmt) StatementType() string {
	return "ALTER_DEFAULT_PRIVILEGES"
}

// SqlString returns the SQL representation of the ALTER DEFAULT PRIVILEGES statement
func (adps *AlterDefaultPrivilegesStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER", "DEFAULT", "PRIVILEGES")

	// Add options (FOR ROLE first, then IN SCHEMA for consistent ordering)
	if adps.Options != nil && len(adps.Options.Items) > 0 {
		// First pass: collect role options
		for _, opt := range adps.Options.Items {
			if defElem, ok := opt.(*DefElem); ok && defElem.Defname == "roles" {
				parts = append(parts, "FOR", "ROLE")
				if defElem.Arg != nil {
					if nodeList, ok := defElem.Arg.(*NodeList); ok {
						var roleNames []string
						for _, role := range nodeList.Items {
							if roleSpec, ok := role.(*RoleSpec); ok {
								roleNames = append(roleNames, roleSpec.SqlString())
							} else if str, ok := role.(*String); ok {
								roleNames = append(roleNames, str.SVal)
							}
						}
						parts = append(parts, strings.Join(roleNames, ", "))
					}
				}
				break // Only one role option expected
			}
		}

		// Second pass: collect schema options
		for _, opt := range adps.Options.Items {
			if defElem, ok := opt.(*DefElem); ok && defElem.Defname == "schemas" {
				parts = append(parts, "IN", "SCHEMA")
				if defElem.Arg != nil {
					if nodeList, ok := defElem.Arg.(*NodeList); ok {
						var schemaNames []string
						for _, schema := range nodeList.Items {
							if str, ok := schema.(*String); ok {
								schemaNames = append(schemaNames, str.SVal)
							}
						}
						parts = append(parts, strings.Join(schemaNames, ", "))
					}
				}
				break // Only one schema option expected
			}
		}
	}

	// Add the action (GRANT/REVOKE statement)
	if adps.Action != nil {
		// Construct the action part manually since it's a special form
		if adps.Action.IsGrant {
			parts = append(parts, "GRANT")
		} else {
			parts = append(parts, "REVOKE")
		}

		// Add GRANT OPTION FOR if this is a grant option revoke
		if !adps.Action.IsGrant && adps.Action.GrantOption {
			parts = append(parts, "GRANT", "OPTION", "FOR")
		}

		// Add privileges
		if adps.Action.Privileges == nil || len(adps.Action.Privileges.Items) == 0 {
			parts = append(parts, "ALL", "PRIVILEGES")
		} else {
			var privParts []string
			for _, item := range adps.Action.Privileges.Items {
				if priv, ok := item.(*AccessPriv); ok {
					privStr := strings.ToUpper(priv.PrivName)
					if priv.Cols != nil && len(priv.Cols.Items) > 0 {
						var colNames []string
						for _, col := range priv.Cols.Items {
							if str, ok := col.(*String); ok {
								colNames = append(colNames, str.SVal)
							}
						}
						privStr += " (" + strings.Join(colNames, ", ") + ")"
					}
					privParts = append(privParts, privStr)
				}
			}
			parts = append(parts, strings.Join(privParts, ", "))
		}

		// Add ON
		parts = append(parts, "ON")

		// Add target type - for default privileges, this is always the object type
		switch adps.Action.Objtype {
		case OBJECT_TABLE:
			parts = append(parts, "TABLES")
		case OBJECT_SEQUENCE:
			parts = append(parts, "SEQUENCES")
		case OBJECT_FUNCTION:
			parts = append(parts, "FUNCTIONS")
		case OBJECT_TYPE:
			parts = append(parts, "TYPES")
		case OBJECT_SCHEMA:
			parts = append(parts, "SCHEMAS")
		}

		// Add TO/FROM
		if adps.Action.IsGrant {
			parts = append(parts, "TO")
		} else {
			parts = append(parts, "FROM")
		}

		// Add grantees
		if adps.Action.Grantees != nil && len(adps.Action.Grantees.Items) > 0 {
			var granteeNames []string
			for _, grantee := range adps.Action.Grantees.Items {
				if roleSpec, ok := grantee.(*RoleSpec); ok {
					granteeNames = append(granteeNames, roleSpec.SqlString())
				}
			}
			parts = append(parts, strings.Join(granteeNames, ", "))
		}

		// Add WITH GRANT OPTION for GRANT statements
		if adps.Action.IsGrant && adps.Action.GrantOption {
			parts = append(parts, "WITH", "GRANT", "OPTION")
		}

		// Add GRANTED BY
		if adps.Action.Grantor != nil {
			parts = append(parts, "GRANTED", "BY", adps.Action.Grantor.SqlString())
		}

		// Add CASCADE/RESTRICT for REVOKE statements
		if !adps.Action.IsGrant && adps.Action.Behavior != DropRestrict {
			switch adps.Action.Behavior {
			case DropCascade:
				parts = append(parts, "CASCADE")
			}
		}
	}

	return strings.Join(parts, " ")
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
	case "bypassrls":
		if boolVal, ok := d.Arg.(*Boolean); ok {
			if boolVal.BoolVal {
				return "BYPASSRLS"
			} else {
				return "NOBYPASSRLS"
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
		} else {
			return "PASSWORD NULL"
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
			parts = append(parts, ars.Role.SqlString())
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
			parts = append(parts, ars.Role.SqlString())
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
		case "transaction_isolation":
			parts = append(parts, "TRANSACTION", "ISOLATION", "LEVEL")
		default:
			// Generic variable name - handle dotted identifiers properly
			parts = append(parts, QuoteQualifiedIdentifier(v.Name))
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
				v.Name == "transaction_isolation" ||
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
					} else if typeCast, ok := arg.(*TypeCast); ok && v.Name == "timezone" {
						// Special handling for INTERVAL expressions in SET TIME ZONE
						// Convert from CAST('1' AS INTERVAL hour) back to INTERVAL '1' HOUR
						if typeCast.TypeName != nil && typeCast.TypeName.Names != nil && typeCast.TypeName.Names.Len() > 0 {
							if firstItem, ok := typeCast.TypeName.Names.Items[0].(*String); ok && firstItem.SVal == "interval" {
								argStr := ""
								if typeCast.Arg != nil {
									argStr = typeCast.Arg.SqlString()
								}

								// Get the interval unit from the type modifiers
								intervalUnit := ""
								if typeCast.TypeName.Typmods != nil && typeCast.TypeName.Typmods.Len() > 0 {
									if firstMod, ok := typeCast.TypeName.Typmods.Items[0].(*Integer); ok {
										intervalUnit = intervalMaskToString(firstMod.IVal)
									}
								}

								if intervalUnit != "" && intervalUnit != "FULL_RANGE" {
									// Format as INTERVAL 'value' UNIT
									values = append(values, fmt.Sprintf("INTERVAL %s %s", argStr, strings.ToUpper(intervalUnit)))
								} else if intervalUnit == "FULL_RANGE" {
									// Handle INTERVAL(precision) 'value' format for full range with precision
									if typeCast.TypeName.Typmods != nil && typeCast.TypeName.Typmods.Len() == 2 {
										if precision, ok := typeCast.TypeName.Typmods.Items[1].(*Integer); ok {
											values = append(values, fmt.Sprintf("INTERVAL(%d) %s", precision.IVal, argStr))
										} else {
											values = append(values, arg.SqlString())
										}
									} else {
										// INTERVAL 'value' format for full range without precision
										values = append(values, fmt.Sprintf("INTERVAL %s", argStr))
									}
								} else {
									// Fallback to regular CAST syntax
									values = append(values, arg.SqlString())
								}
							} else {
								values = append(values, arg.SqlString())
							}
						} else {
							values = append(values, arg.SqlString())
						}
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

	case VAR_SET_MULTI:
		// Handle SET TRANSACTION and similar multi-value statements
		if v.Name == "TRANSACTION" {
			parts = append(parts, "TRANSACTION")
			// Add transaction mode items
			if v.Args != nil {
				for _, arg := range v.Args.Items {
					if defElem, ok := arg.(*DefElem); ok {
						if defElem.Defname == "transaction_isolation" {
							parts = append(parts, "ISOLATION", "LEVEL")
							if str, ok := defElem.Arg.(*String); ok {
								parts = append(parts, strings.ToUpper(str.SVal))
							}
						} else if defElem.Defname == "transaction_read_only" {
							if boolVal, ok := defElem.Arg.(*Boolean); ok {
								if boolVal.BoolVal {
									parts = append(parts, "READ", "ONLY")
								} else {
									parts = append(parts, "READ", "WRITE")
								}
							}
						} else if defElem.Defname == "transaction_deferrable" {
							if boolVal, ok := defElem.Arg.(*Boolean); ok {
								if boolVal.BoolVal {
									parts = append(parts, "DEFERRABLE")
								} else {
									parts = append(parts, "NOT", "DEFERRABLE")
								}
							}
						}
					}
				}
			}
		} else if v.Name == "SESSION CHARACTERISTICS AS TRANSACTION" {
			parts = append(parts, "SESSION", "CHARACTERISTICS", "AS", "TRANSACTION")
			// Add transaction mode items
			if v.Args != nil {
				for _, arg := range v.Args.Items {
					if defElem, ok := arg.(*DefElem); ok {
						if defElem.Defname == "transaction_isolation" {
							parts = append(parts, "ISOLATION", "LEVEL")
							if str, ok := defElem.Arg.(*String); ok {
								parts = append(parts, strings.ToUpper(str.SVal))
							}
						} else if defElem.Defname == "transaction_read_only" {
							if boolVal, ok := defElem.Arg.(*Boolean); ok {
								if boolVal.BoolVal {
									parts = append(parts, "READ", "ONLY")
								} else {
									parts = append(parts, "READ", "WRITE")
								}
							}
						} else if defElem.Defname == "transaction_deferrable" {
							if boolVal, ok := defElem.Arg.(*Boolean); ok {
								if boolVal.BoolVal {
									parts = append(parts, "DEFERRABLE")
								} else {
									parts = append(parts, "NOT", "DEFERRABLE")
								}
							}
						}
					}
				}
			}
		} else {
			// For other multi-value statements, use generic handling
			parts = append(parts, v.Name)
			if v.Args != nil && v.Args.Len() > 0 {
				var values []string
				for _, arg := range v.Args.Items {
					values = append(values, arg.SqlString())
				}
				parts = append(parts, strings.Join(values, ", "))
			}
		}
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
	var parsed float64
	var remainder string
	n, err := fmt.Sscanf(value, "%f%s", &parsed, &remainder)
	if err == nil && n == 1 {
		// It's a pure number with no trailing characters, don't quote
		return false
	}

	// If it starts with a number but has additional characters (like "256MB"), quote it
	if err == nil && n == 2 {
		// Has trailing characters, needs quoting
		return true
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

// SqlString returns the SQL representation of the SHOW statement
func (vss *VariableShowStmt) SqlString() string {
	return "SHOW " + QuoteQualifiedIdentifier(vss.Name)
}

// ==============================================================================
// SYSTEM CONFIGURATION STATEMENTS - ALTER SYSTEM - PostgreSQL parsenodes.h:3812-3816
// ==============================================================================

// AlterSystemStmt represents an ALTER SYSTEM statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3812
type AlterSystemStmt struct {
	BaseNode
	Setstmt *VariableSetStmt // SET subcommand - postgres/src/include/nodes/parsenodes.h:3815
}

// NewAlterSystemStmt creates a new ALTER SYSTEM statement.
func NewAlterSystemStmt(setstmt *VariableSetStmt) *AlterSystemStmt {
	return &AlterSystemStmt{
		BaseNode: BaseNode{Tag: T_AlterSystemStmt},
		Setstmt:  setstmt,
	}
}

func (ass *AlterSystemStmt) String() string {
	var setInfo string
	if ass.Setstmt != nil {
		setInfo = ass.Setstmt.Name
	}
	return fmt.Sprintf("AlterSystemStmt(%s)@%d", setInfo, ass.Location())
}

func (ass *AlterSystemStmt) StatementType() string {
	return "ALTER_SYSTEM"
}

// SqlString returns the SQL representation of the ALTER SYSTEM statement
func (ass *AlterSystemStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER", "SYSTEM")

	if ass.Setstmt != nil {
		// Format the SET/RESET clause specifically for ALTER SYSTEM
		switch ass.Setstmt.Kind {
		case VAR_SET_VALUE:
			parts = append(parts, "SET", ass.Setstmt.Name)
			if ass.Setstmt.Args != nil && ass.Setstmt.Args.Len() > 0 {
				parts = append(parts, "=")
				var values []string
				for _, arg := range ass.Setstmt.Args.Items {
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
		case VAR_SET_DEFAULT:
			parts = append(parts, "SET", ass.Setstmt.Name, "=", "DEFAULT")
		case VAR_RESET:
			parts = append(parts, "RESET", ass.Setstmt.Name)
		case VAR_RESET_ALL:
			parts = append(parts, "RESET", "ALL")
		}
	}

	return strings.Join(parts, " ")
}

// ==============================================================================
// QUERY ANALYSIS STATEMENTS - EXPLAIN/PREPARE/EXECUTE - PostgreSQL parsenodes.h:3868-4070
// ==============================================================================

// ExplainStmt represents an EXPLAIN statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3868
type ExplainStmt struct {
	BaseNode
	Query   Node      // The query to explain - postgres/src/include/nodes/parsenodes.h:3871
	Options *NodeList // List of DefElem nodes - postgres/src/include/nodes/parsenodes.h:3872
}

// NewExplainStmt creates a new EXPLAIN statement.
func NewExplainStmt(query Node, options *NodeList) *ExplainStmt {
	return &ExplainStmt{
		BaseNode: BaseNode{Tag: T_ExplainStmt},
		Query:    query,
		Options:  options,
	}
}

func (es *ExplainStmt) String() string {
	optionCount := 0
	if es.Options != nil {
		optionCount = es.Options.Len()
	}
	return fmt.Sprintf("ExplainStmt(%d options)@%d", optionCount, es.Location())
}

func (es *ExplainStmt) StatementType() string {
	return "EXPLAIN"
}

// SqlString returns the SQL representation of the EXPLAIN statement
func (es *ExplainStmt) SqlString() string {
	var parts []string
	parts = append(parts, "EXPLAIN")

	// Add options
	if es.Options != nil && es.Options.Len() > 0 {
		// Try to use simple syntax for common cases
		if canUseSimpleSyntax(es.Options) {
			for _, item := range es.Options.Items {
				if option, ok := item.(*DefElem); ok {
					parts = append(parts, strings.ToUpper(option.Defname))
				}
			}
		} else {
			// Use parentheses format for complex options
			var optionParts []string
			for _, item := range es.Options.Items {
				if option, ok := item.(*DefElem); ok {
					optStr := formatExplainOption(option)
					if optStr != "" {
						optionParts = append(optionParts, optStr)
					}
				}
			}
			if len(optionParts) > 0 {
				parts = append(parts, "("+strings.Join(optionParts, ", ")+")")
			}
		}
	}

	// Add the query
	if es.Query != nil {
		parts = append(parts, es.Query.SqlString())
	}

	return strings.Join(parts, " ")
}

// canUseSimpleSyntax determines if we can use EXPLAIN ANALYZE/VERBOSE syntax
// instead of EXPLAIN (option_name value) syntax
func canUseSimpleSyntax(options *NodeList) bool {
	if options == nil || options.Len() == 0 {
		return false
	}

	// Check if all options are simple boolean options with no explicit values
	for _, item := range options.Items {
		if option, ok := item.(*DefElem); ok {
			// Only ANALYZE and VERBOSE can use simple syntax
			if option.Defname != "analyze" && option.Defname != "verbose" {
				return false
			}
			// Option must have nil argument (no explicit value)
			if option.Arg != nil {
				return false
			}
		} else {
			return false
		}
	}

	return true
}

// formatExplainOption formats a single EXPLAIN option
func formatExplainOption(option *DefElem) string {
	if option.Defname == "" {
		return ""
	}

	optionName := strings.ToUpper(option.Defname)

	// Handle different types of option arguments
	if option.Arg == nil {
		// Boolean option with no explicit value (defaults to true)
		return optionName + " true"
	}

	switch arg := option.Arg.(type) {
	case *String:
		if arg.SVal == "true" || arg.SVal == "on" || arg.SVal == "1" {
			return optionName + " true"
		} else if arg.SVal == "false" || arg.SVal == "off" || arg.SVal == "0" {
			return optionName + " false"
		} else {
			// String value - don't quote for EXPLAIN options like FORMAT JSON
			return optionName + " " + strings.ToUpper(arg.SVal)
		}
	case *Boolean:
		if arg.BoolVal {
			return optionName + " true"
		} else {
			return optionName + " false"
		}
	case *Integer:
		return optionName + " " + fmt.Sprintf("%d", arg.IVal)
	default:
		// Fallback for other types
		if stringer, ok := arg.(interface{ SqlString() string }); ok {
			return optionName + " " + stringer.SqlString()
		}
		return optionName + " true"
	}
}

// PrepareStmt represents a PREPARE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:4030
type PrepareStmt struct {
	BaseNode
	Name     string    // Statement name - postgres/src/include/nodes/parsenodes.h:4033
	Argtypes *NodeList // List of TypeName nodes (specified arg types) - postgres/src/include/nodes/parsenodes.h:4034
	Query    Node      // Statement to prepare - postgres/src/include/nodes/parsenodes.h:4035
}

// NewPrepareStmt creates a new PREPARE statement.
func NewPrepareStmt(name string, argtypes *NodeList, query Node) *PrepareStmt {
	return &PrepareStmt{
		BaseNode: BaseNode{Tag: T_PrepareStmt},
		Name:     name,
		Argtypes: argtypes,
		Query:    query,
	}
}

func (ps *PrepareStmt) String() string {
	argCount := 0
	if ps.Argtypes != nil {
		argCount = ps.Argtypes.Len()
	}
	return fmt.Sprintf("PrepareStmt(%s, %d argtypes)@%d", ps.Name, argCount, ps.Location())
}

func (ps *PrepareStmt) StatementType() string {
	return "PREPARE"
}

// SqlString returns the SQL representation of the PREPARE statement
func (ps *PrepareStmt) SqlString() string {
	var parts []string
	parts = append(parts, "PREPARE", ps.Name)

	// Add argument types if present
	if ps.Argtypes != nil && ps.Argtypes.Len() > 0 {
		parts = append(parts, "(")
		var typeNames []string
		for _, argtype := range ps.Argtypes.Items {
			if typeName, ok := argtype.(*TypeName); ok {
				typeNames = append(typeNames, typeName.SqlString())
			}
		}
		parts = append(parts, strings.Join(typeNames, ", "))
		parts = append(parts, ")")
	}

	parts = append(parts, "AS")

	// Add the query
	if ps.Query != nil {
		if sqlNode, ok := ps.Query.(interface{ SqlString() string }); ok {
			parts = append(parts, sqlNode.SqlString())
		} else {
			parts = append(parts, "<query>")
		}
	}

	return strings.Join(parts, " ")
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

// SqlString returns the SQL representation of the EXECUTE statement
func (es *ExecuteStmt) SqlString() string {
	var parts []string
	parts = append(parts, "EXECUTE", es.Name)

	// Add parameters if present
	if es.Params != nil && es.Params.Len() > 0 {
		parts = append(parts, "(")
		var paramValues []string
		for _, param := range es.Params.Items {
			paramValues = append(paramValues, param.SqlString())
		}
		parts = append(parts, strings.Join(paramValues, ", "))
		parts = append(parts, ")")
	}

	return strings.Join(parts, " ")
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

// SqlString returns the SQL representation of the DEALLOCATE statement
func (ds *DeallocateStmt) SqlString() string {
	var parts []string
	parts = append(parts, "DEALLOCATE")

	if ds.IsAll || ds.Name == "" {
		parts = append(parts, "ALL")
	} else {
		parts = append(parts, ds.Name)
	}

	return strings.Join(parts, " ")
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
					columns = append(columns, QuoteIdentifier(str.SVal))
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
			// String value - properly escape and quote it
			return optionName + " " + QuoteStringLiteral(arg.SVal)
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
	Options     *NodeList // List of DefElem nodes - postgres/src/include/nodes/parsenodes.h:3840
	Rels        *NodeList // List of VacuumRelation, or NIL for all - postgres/src/include/nodes/parsenodes.h:3841
	IsVacuumcmd bool      // True for VACUUM, false for ANALYZE - postgres/src/include/nodes/parsenodes.h:3842
}

// VacuumRelation represents a relation in a VACUUM statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3845-3851
type VacuumRelation struct {
	BaseNode
	Relation *RangeVar // Relation to vacuum - postgres/src/include/nodes/parsenodes.h:3848
	Oid      Oid       // OID of relation, for RangeVar-less VacuumStmt - postgres/src/include/nodes/parsenodes.h:3849
	VaCols   *NodeList // List of column names, or NIL for all - postgres/src/include/nodes/parsenodes.h:3850
}

// NewVacuumStmt creates a new VACUUM statement.
func NewVacuumStmt(options *NodeList, rels *NodeList) *VacuumStmt {
	return &VacuumStmt{
		BaseNode:    BaseNode{Tag: T_VacuumStmt},
		Options:     options,
		Rels:        rels,
		IsVacuumcmd: true,
	}
}

// NewAnalyzeStmt creates a new ANALYZE statement.
func NewAnalyzeStmt(options *NodeList, rels *NodeList) *VacuumStmt {
	return &VacuumStmt{
		BaseNode:    BaseNode{Tag: T_VacuumStmt},
		Options:     options,
		Rels:        rels,
		IsVacuumcmd: false,
	}
}

// NewVacuumRelation creates a new VacuumRelation node.
func NewVacuumRelation(relation *RangeVar, vaCols *NodeList) *VacuumRelation {
	return &VacuumRelation{
		BaseNode: BaseNode{Tag: T_VacuumRelation},
		Relation: relation,
		VaCols:   vaCols,
		Oid:      InvalidOid,
	}
}

func (vs *VacuumStmt) String() string {
	action := "ANALYZE"
	if vs.IsVacuumcmd {
		action = "VACUUM"
	}
	relCount := 0
	if vs.Rels != nil {
		relCount = vs.Rels.Len()
	}
	optionCount := 0
	if vs.Options != nil {
		optionCount = vs.Options.Len()
	}
	return fmt.Sprintf("VacuumStmt(%s, %d rels, %d options)@%d", action, relCount, optionCount, vs.Location())
}

func (vs *VacuumStmt) StatementType() string {
	if vs.IsVacuumcmd {
		return "VACUUM"
	}
	return "ANALYZE"
}

// SqlString returns the SQL representation of the VACUUM/ANALYZE statement
func (vs *VacuumStmt) SqlString() string {
	var parts []string

	// Start with VACUUM or ANALYZE
	if vs.IsVacuumcmd {
		parts = append(parts, "VACUUM")
	} else {
		parts = append(parts, "ANALYZE")
	}

	// Add options in parentheses if present (modern syntax)
	if vs.Options != nil && vs.Options.Len() > 0 {
		var optionParts []string
		for _, item := range vs.Options.Items {
			if option, ok := item.(*DefElem); ok {
				optStr := formatVacuumOption(option)
				if optStr != "" {
					optionParts = append(optionParts, optStr)
				}
			}
		}
		if len(optionParts) > 0 {
			parts = append(parts, "("+strings.Join(optionParts, ", ")+")")
		}
	}

	// Add relations if present
	if vs.Rels != nil && vs.Rels.Len() > 0 {
		var relParts []string
		for _, item := range vs.Rels.Items {
			if rel, ok := item.(*VacuumRelation); ok {
				if rel.Relation != nil {
					relName := rel.Relation.RelName
					if rel.Relation.SchemaName != "" {
						relName = rel.Relation.SchemaName + "." + relName
					}

					// Add column list if present
					if rel.VaCols != nil && rel.VaCols.Len() > 0 {
						var colNames []string
						for _, item := range rel.VaCols.Items {
							if str, ok := item.(*String); ok {
								colNames = append(colNames, QuoteIdentifier(str.SVal))
							}
						}
						if len(colNames) > 0 {
							relName += " (" + strings.Join(colNames, ", ") + ")"
						}
					}

					relParts = append(relParts, relName)
				}
			}
		}
		if len(relParts) > 0 {
			parts = append(parts, strings.Join(relParts, ", "))
		}
	}

	return strings.Join(parts, " ")
}

// isSimpleVacuumOption determines if a VACUUM option can be written without a value
func isSimpleVacuumOption(optionName string) bool {
	switch strings.ToLower(optionName) {
	case "full", "verbose", "analyze", "freeze", "disable_page_skipping":
		return true
	default:
		return false
	}
}

// formatVacuumOption formats a single VACUUM option
func formatVacuumOption(option *DefElem) string {
	if option.Defname == "" {
		return ""
	}

	optionName := strings.ToUpper(option.Defname)

	// Handle different types of option arguments
	if option.Arg == nil {
		// For simple boolean options like FULL, VERBOSE, ANALYZE, FREEZE, just return the name
		if isSimpleVacuumOption(option.Defname) {
			return optionName
		}
		// Other options with no explicit value (defaults to true)
		return optionName + " true"
	}

	switch arg := option.Arg.(type) {
	case *String:
		if arg.SVal == "true" || arg.SVal == "on" || arg.SVal == "1" {
			// For simple boolean options that are true, just return the name
			if isSimpleVacuumOption(option.Defname) {
				return optionName
			}
			return optionName + " true"
		} else if arg.SVal == "false" || arg.SVal == "off" || arg.SVal == "0" {
			return optionName + " false"
		} else {
			// String value - quote it for VACUUM options
			return optionName + " '" + arg.SVal + "'"
		}
	case *Boolean:
		if arg.BoolVal {
			// For simple boolean options that are true, just return the name
			if isSimpleVacuumOption(option.Defname) {
				return optionName
			}
			return optionName + " true"
		} else {
			return optionName + " false"
		}
	case *Integer:
		return optionName + " " + fmt.Sprintf("%d", arg.IVal)
	default:
		// Fallback for other types
		if stringer, ok := arg.(interface{ SqlString() string }); ok {
			return optionName + " " + stringer.SqlString()
		}
		return optionName + " true"
	}
}

func (vr *VacuumRelation) String() string {
	relName := ""
	if vr.Relation != nil {
		relName = vr.Relation.RelName
	}
	colCount := 0
	if vr.VaCols != nil {
		colCount = vr.VaCols.Len()
	}
	return fmt.Sprintf("VacuumRelation(%s, %d cols)@%d", relName, colCount, vr.Location())
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
	Params   *NodeList         // List of DefElem nodes - postgres/src/include/nodes/parsenodes.h:3980
}

// NewReindexStmt creates a new REINDEX statement.
func NewReindexStmt(kind ReindexObjectType, relation *RangeVar, name string, params *NodeList) *ReindexStmt {
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

func (rs *ReindexStmt) SqlString() string {
	var parts []string
	parts = append(parts, "REINDEX")

	// Add options if present
	hasConcurrently := false
	if rs.Params != nil && rs.Params.Len() > 0 {
		var options []string
		for _, param := range rs.Params.Items {
			if defElem, ok := param.(*DefElem); ok {
				if defElem.Defname == "concurrently" {
					hasConcurrently = true
				} else {
					options = append(options, defElem.Defname)
				}
			}
		}
		if len(options) > 0 {
			parts = append(parts, "("+strings.Join(options, ", ")+")")
		}
	}

	// Add object type and target
	switch rs.Kind {
	case REINDEX_OBJECT_INDEX:
		parts = append(parts, "INDEX")
	case REINDEX_OBJECT_TABLE:
		parts = append(parts, "TABLE")
	case REINDEX_OBJECT_SCHEMA:
		parts = append(parts, "SCHEMA")
	case REINDEX_OBJECT_SYSTEM:
		parts = append(parts, "SYSTEM")
	case REINDEX_OBJECT_DATABASE:
		parts = append(parts, "DATABASE")
	}

	// Add CONCURRENTLY if specified
	if hasConcurrently {
		parts = append(parts, "CONCURRENTLY")
	}

	// Add target name
	if rs.Relation != nil {
		parts = append(parts, rs.Relation.RelName)
	} else if rs.Name != "" {
		parts = append(parts, rs.Name)
	}

	return strings.Join(parts, " ")
}

// ClusterStmt represents a CLUSTER statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3822-3828
type ClusterStmt struct {
	BaseNode
	Relation  *RangeVar // Relation to cluster - postgres/src/include/nodes/parsenodes.h:3825
	Indexname string    // Index name or NULL - postgres/src/include/nodes/parsenodes.h:3826
	Params    *NodeList // List of DefElem nodes - postgres/src/include/nodes/parsenodes.h:3827
}

// NewClusterStmt creates a new CLUSTER statement.
func NewClusterStmt(relation *RangeVar, indexname string, params *NodeList) *ClusterStmt {
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

func (cs *ClusterStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CLUSTER")

	// Add options if present
	if cs.Params != nil && cs.Params.Len() > 0 {
		var options []string
		for _, param := range cs.Params.Items {
			if defElem, ok := param.(*DefElem); ok {
				options = append(options, defElem.Defname)
			}
		}
		if len(options) > 0 {
			parts = append(parts, "("+strings.Join(options, ", ")+")")
		}
	}

	// Add table name if specified
	if cs.Relation != nil {
		parts = append(parts, cs.Relation.SqlString())

		// Add index specification if present
		if cs.Indexname != "" {
			parts = append(parts, "USING", cs.Indexname)
		}
	}

	return strings.Join(parts, " ")
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

func (cps *CheckPointStmt) SqlString() string {
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

func (ds *DiscardStmt) SqlString() string {
	return "DISCARD " + ds.Target.String()
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

// SqlString returns the SQL representation of the LOAD statement
func (ls *LoadStmt) SqlString() string {
	return fmt.Sprintf("LOAD '%s'", ls.Filename)
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

// SqlString returns the SQL representation of the NOTIFY statement
func (ns *NotifyStmt) SqlString() string {
	if ns.Payload != "" {
		return fmt.Sprintf("NOTIFY %s, '%s'", ns.Conditionname, ns.Payload)
	}
	return fmt.Sprintf("NOTIFY %s", ns.Conditionname)
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

// SqlString returns the SQL representation of the LISTEN statement
func (ls *ListenStmt) SqlString() string {
	return fmt.Sprintf("LISTEN %s", ls.Conditionname)
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

// SqlString returns the SQL representation of the UNLISTEN statement
func (us *UnlistenStmt) SqlString() string {
	if us.Conditionname == "*" {
		return "UNLISTEN *"
	}
	return fmt.Sprintf("UNLISTEN %s", us.Conditionname)
}

// ==============================================================================
// CONSTRAINTS SET STATEMENT - Phase 3J PostgreSQL Extensions
// ==============================================================================

// ConstraintsSetStmt represents SET CONSTRAINTS statement
// Ported from postgres/src/include/nodes/parsenodes.h
type ConstraintsSetStmt struct {
	BaseNode
	Constraints *NodeList `json:"constraints"` // List of names as RangeVars
	Deferred    bool      `json:"deferred"`    // true=DEFERRED, false=IMMEDIATE
}

func (n *ConstraintsSetStmt) node() {}
func (n *ConstraintsSetStmt) stmt() {}

// StatementType returns the statement type
func (n *ConstraintsSetStmt) StatementType() string {
	return "SET_CONSTRAINTS"
}

// NewConstraintsSetStmt creates a new ConstraintsSetStmt node
func NewConstraintsSetStmt(constraints *NodeList, deferred bool) *ConstraintsSetStmt {
	return &ConstraintsSetStmt{
		BaseNode:    BaseNode{Tag: T_ConstraintsSetStmt},
		Constraints: constraints,
		Deferred:    deferred,
	}
}

// String returns a string representation of the ConstraintsSetStmt
func (n *ConstraintsSetStmt) String() string {
	parts := []string{"SET CONSTRAINTS"}

	if n.Constraints == nil || len(n.Constraints.Items) == 0 {
		parts = append(parts, "ALL")
	} else {
		constraintNames := make([]string, 0)
		for _, constraint := range n.Constraints.Items {
			if rangeVar, ok := constraint.(*RangeVar); ok {
				constraintNames = append(constraintNames, rangeVar.RelName)
			} else if nodeList, ok := constraint.(*NodeList); ok && len(nodeList.Items) > 0 {
				// Handle qualified name (schema.constraint)
				nameStr := ""
				for i, item := range nodeList.Items {
					if i > 0 {
						nameStr += "."
					}
					if strNode, ok := item.(*String); ok {
						nameStr += strNode.SVal
					} else {
						nameStr += item.String()
					}
				}
				constraintNames = append(constraintNames, nameStr)
			} else {
				constraintNames = append(constraintNames, constraint.String())
			}
		}
		parts = append(parts, strings.Join(constraintNames, ", "))
	}

	if n.Deferred {
		parts = append(parts, "DEFERRED")
	} else {
		parts = append(parts, "IMMEDIATE")
	}

	return strings.Join(parts, " ")
}

// SqlString returns the SQL representation of the SET CONSTRAINTS statement
func (n *ConstraintsSetStmt) SqlString() string {
	return n.String()
}
