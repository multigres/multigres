// Package ast provides PostgreSQL DDL statement node definitions.
// Ported from postgres/src/include/nodes/parsenodes.h
package ast

import (
	"fmt"
	"strings"
)

// ==============================================================================
// DDL FRAMEWORK - PostgreSQL parsenodes.h DDL implementation
// Ported from postgres/src/include/nodes/parsenodes.h
// ==============================================================================

// Supporting enums and types for DDL statements

// ObjectType represents the type of database object - ported from postgres/src/include/nodes/parsenodes.h:2256-2310
type ObjectType int

const (
	OBJECT_ACCESS_METHOD ObjectType = iota
	OBJECT_AGGREGATE
	OBJECT_AMOP
	OBJECT_AMPROC
	OBJECT_ATTRIBUTE // type's attribute, when distinct from column
	OBJECT_CAST
	OBJECT_COLUMN
	OBJECT_COLLATION
	OBJECT_CONVERSION
	OBJECT_DATABASE
	OBJECT_DEFAULT
	OBJECT_DEFACL
	OBJECT_DOMAIN
	OBJECT_DOMCONSTRAINT
	OBJECT_EVENT_TRIGGER
	OBJECT_EXTENSION
	OBJECT_FDW
	OBJECT_FOREIGN_SERVER
	OBJECT_FOREIGN_TABLE
	OBJECT_FUNCTION
	OBJECT_INDEX
	OBJECT_LANGUAGE
	OBJECT_LARGEOBJECT
	OBJECT_MATVIEW
	OBJECT_OPCLASS
	OBJECT_OPERATOR
	OBJECT_OPFAMILY
	OBJECT_PARAMETER_ACL
	OBJECT_POLICY
	OBJECT_PROCEDURE
	OBJECT_PUBLICATION
	OBJECT_PUBLICATION_NAMESPACE
	OBJECT_PUBLICATION_REL
	OBJECT_ROLE
	OBJECT_ROUTINE
	OBJECT_RULE
	OBJECT_SCHEMA
	OBJECT_SEQUENCE
	OBJECT_SUBSCRIPTION
	OBJECT_STATISTIC_EXT
	OBJECT_TABCONSTRAINT
	OBJECT_TABLE
	OBJECT_TABLESPACE
	OBJECT_TRANSFORM
	OBJECT_TRIGGER
	OBJECT_TSCONFIGURATION
	OBJECT_TSDICTIONARY
	OBJECT_TSPARSER
	OBJECT_TSTEMPLATE
	OBJECT_TYPE
	OBJECT_USER_MAPPING
	OBJECT_VIEW
)

func (o ObjectType) String() string {
	switch o {
	case OBJECT_ACCESS_METHOD:
		return "ACCESS_METHOD"
	case OBJECT_AGGREGATE:
		return "AGGREGATE"
	case OBJECT_COLUMN:
		return "COLUMN"
	case OBJECT_DATABASE:
		return "DATABASE"
	case OBJECT_DOMAIN:
		return "DOMAIN"
	case OBJECT_EXTENSION:
		return "EXTENSION"
	case OBJECT_FUNCTION:
		return "FUNCTION"
	case OBJECT_INDEX:
		return "INDEX"
	case OBJECT_SCHEMA:
		return "SCHEMA"
	case OBJECT_SEQUENCE:
		return "SEQUENCE"
	case OBJECT_TABLE:
		return "TABLE"
	case OBJECT_VIEW:
		return "VIEW"
	default:
		return fmt.Sprintf("ObjectType(%d)", int(o))
	}
}

// DropBehavior represents CASCADE/RESTRICT behavior - ported from postgres/src/include/nodes/parsenodes.h:2329-2333
type DropBehavior int

const (
	DropRestrict DropBehavior = iota // drop fails if any dependent objects
	DropCascade                      // remove dependent objects too
)

func (d DropBehavior) String() string {
	switch d {
	case DropRestrict:
		return "RESTRICT"
	case DropCascade:
		return "CASCADE"
	default:
		return fmt.Sprintf("DropBehavior(%d)", int(d))
	}
}

// ConstrType represents types of constraints - ported from postgres/src/include/nodes/parsenodes.h:2697-2714
type ConstrType int

const (
	CONSTR_NULL ConstrType = iota // not standard SQL, but expected
	CONSTR_NOTNULL
	CONSTR_DEFAULT
	CONSTR_IDENTITY
	CONSTR_GENERATED
	CONSTR_CHECK
	CONSTR_PRIMARY
	CONSTR_UNIQUE
	CONSTR_EXCLUSION
	CONSTR_FOREIGN
	CONSTR_ATTR_DEFERRABLE // attributes for previous constraint node
	CONSTR_ATTR_NOT_DEFERRABLE
	CONSTR_ATTR_DEFERRED
	CONSTR_ATTR_IMMEDIATE
)

func (c ConstrType) String() string {
	switch c {
	case CONSTR_NULL:
		return "NULL"
	case CONSTR_NOTNULL:
		return "NOT_NULL"
	case CONSTR_DEFAULT:
		return "DEFAULT"
	case CONSTR_CHECK:
		return "CHECK"
	case CONSTR_PRIMARY:
		return "PRIMARY_KEY"
	case CONSTR_UNIQUE:
		return "UNIQUE"
	case CONSTR_FOREIGN:
		return "FOREIGN_KEY"
	default:
		return fmt.Sprintf("ConstrType(%d)", int(c))
	}
}

// ViewCheckOption represents WITH CHECK OPTION - ported from postgres/src/include/nodes/parsenodes.h:3773-3777
type ViewCheckOption int

const (
	NO_CHECK_OPTION ViewCheckOption = iota
	LOCAL_CHECK_OPTION
	CASCADED_CHECK_OPTION
)

func (v ViewCheckOption) String() string {
	switch v {
	case NO_CHECK_OPTION:
		return "NO_CHECK"
	case LOCAL_CHECK_OPTION:
		return "LOCAL"
	case CASCADED_CHECK_OPTION:
		return "CASCADED"
	default:
		return fmt.Sprintf("ViewCheckOption(%d)", int(v))
	}
}

// AlterTableType represents types of ALTER TABLE operations - ported from postgres/src/include/nodes/parsenodes.h:2348-2417
type AlterTableType int

const (
	AT_AddColumn                 AlterTableType = iota // add column
	AT_AddColumnToView                                 // implicitly via CREATE OR REPLACE VIEW
	AT_ColumnDefault                                   // alter column default
	AT_CookedColumnDefault                             // add a pre-cooked column default
	AT_DropNotNull                                     // alter column drop not null
	AT_SetNotNull                                      // alter column set not null
	AT_DropExpression                                  // alter column drop expression
	AT_CheckNotNull                                    // check column is already not null
	AT_SetStatistics                                   // alter column set statistics
	AT_SetOptions                                      // alter column set (options)
	AT_ResetOptions                                    // alter column reset (options)
	AT_SetStorage                                      // alter column set storage
	AT_SetCompression                                  // alter column set compression
	AT_DropColumn                                      // drop column
	AT_AddIndex                                        // add index
	AT_ReAddIndex                                      // internal to commands/tablecmds.c
	AT_AddConstraint                                   // add constraint
	AT_AddConstraintRecurse                            // internal to commands/tablecmds.c
	AT_ReAddConstraint                                 // internal to commands/tablecmds.c
	AT_AlterConstraint                                 // alter constraint
	AT_ValidateConstraint                              // validate constraint
	AT_DropConstraint                                  // drop constraint
	AT_ReAddComment                                    // internal to commands/tablecmds.c
	AT_AlterColumnType                                 // alter column type
	AT_AlterColumnGenericOptions                       // alter column OPTIONS (...)
	AT_ChangeOwner                                     // change owner
	AT_ClusterOn                                       // CLUSTER ON
	AT_DropCluster                                     // SET WITHOUT CLUSTER
	AT_SetLogged                                       // SET LOGGED
	AT_SetUnLogged                                     // SET UNLOGGED
	AT_DropOids                                        // SET WITHOUT OIDS
	AT_SetAccessMethod                                 // SET ACCESS METHOD
	AT_SetTableSpace                                   // SET TABLESPACE
	AT_SetRelOptions                                   // SET (...)
	AT_ResetRelOptions                                 // RESET (...)
	AT_ReplaceRelOptions                               // replace reloption list in its entirety
	AT_EnableTrig                                      // ENABLE TRIGGER name
	AT_EnableAlwaysTrig                                // ENABLE ALWAYS TRIGGER name
	AT_EnableReplicaTrig                               // ENABLE REPLICA TRIGGER name
	AT_DisableTrig                                     // DISABLE TRIGGER name
	AT_EnableTrigAll                                   // ENABLE TRIGGER ALL
	AT_DisableTrigAll                                  // DISABLE TRIGGER ALL
	AT_EnableTrigUser                                  // ENABLE TRIGGER USER
	AT_DisableTrigUser                                 // DISABLE TRIGGER USER
	AT_EnableRule                                      // ENABLE RULE name
	AT_EnableAlwaysRule                                // ENABLE ALWAYS RULE name
	AT_EnableReplicaRule                               // ENABLE REPLICA RULE name
	AT_DisableRule                                     // DISABLE RULE name
	AT_AddInherit                                      // INHERIT parent
	AT_DropInherit                                     // NO INHERIT parent
	AT_AddOf                                           // OF <type_name>
	AT_DropOf                                          // NOT OF
	AT_ReplicaIdentity                                 // REPLICA IDENTITY
	AT_EnableRowSecurity                               // ENABLE ROW SECURITY
	AT_DisableRowSecurity                              // DISABLE ROW SECURITY
	AT_ForceRowSecurity                                // FORCE ROW SECURITY
	AT_NoForceRowSecurity                              // NO FORCE ROW SECURITY
	AT_GenericOptions                                  // OPTIONS (...)
	AT_AttachPartition                                 // ATTACH PARTITION
	AT_DetachPartition                                 // DETACH PARTITION
	AT_DetachPartitionFinalize                         // DETACH PARTITION ... FINALIZE
	AT_AddIdentity                                     // ADD IDENTITY
	AT_SetIdentity                                     // SET identity column options
	AT_DropIdentity                                    // DROP IDENTITY
	AT_ReAddStatistics                                 // internal to commands/tablecmds.c
)

func (a AlterTableType) String() string {
	switch a {
	case AT_AddColumn:
		return "ADD_COLUMN"
	case AT_DropColumn:
		return "DROP_COLUMN"
	case AT_ColumnDefault:
		return "COLUMN_DEFAULT"
	case AT_DropNotNull:
		return "DROP_NOT_NULL"
	case AT_SetNotNull:
		return "SET_NOT_NULL"
	case AT_AddConstraint:
		return "ADD_CONSTRAINT"
	case AT_DropConstraint:
		return "DROP_CONSTRAINT"
	case AT_AlterColumnType:
		return "ALTER_COLUMN_TYPE"
	case AT_ChangeOwner:
		return "CHANGE_OWNER"
	case AT_SetTableSpace:
		return "SET_TABLESPACE"
	default:
		return fmt.Sprintf("AlterTableType(%d)", int(a))
	}
}

// DefElemAction represents actions for DefElem - ported from postgres/src/include/nodes/parsenodes.h:803-809
type DefElemAction int

const (
	DEFELEM_UNSPEC DefElemAction = iota // no action given
	DEFELEM_SET                         // SET
	DEFELEM_ADD                         // ADD
	DEFELEM_DROP                        // DROP
)

func (d DefElemAction) String() string {
	switch d {
	case DEFELEM_UNSPEC:
		return "UNSPEC"
	case DEFELEM_SET:
		return "SET"
	case DEFELEM_ADD:
		return "ADD"
	case DEFELEM_DROP:
		return "DROP"
	default:
		return fmt.Sprintf("DefElemAction(%d)", int(d))
	}
}

// SortByDir represents sort direction - ported from postgres/src/include/nodes/parsenodes.h:57-62
type SortByDir int

const (
	SORTBY_DEFAULT SortByDir = iota
	SORTBY_ASC
	SORTBY_DESC
	SORTBY_USING // not used in indexes
)

func (s SortByDir) String() string {
	switch s {
	case SORTBY_DEFAULT:
		return "DEFAULT"
	case SORTBY_ASC:
		return "ASC"
	case SORTBY_DESC:
		return "DESC"
	case SORTBY_USING:
		return "USING"
	default:
		return fmt.Sprintf("SortByDir(%d)", int(s))
	}
}

// SortByNulls represents null ordering - ported from postgres/src/include/nodes/parsenodes.h:64-69
type SortByNulls int

const (
	SORTBY_NULLS_DEFAULT SortByNulls = iota
	SORTBY_NULLS_FIRST
	SORTBY_NULLS_LAST
)

func (s SortByNulls) String() string {
	switch s {
	case SORTBY_NULLS_DEFAULT:
		return "DEFAULT"
	case SORTBY_NULLS_FIRST:
		return "NULLS_FIRST"
	case SORTBY_NULLS_LAST:
		return "NULLS_LAST"
	default:
		return fmt.Sprintf("SortByNulls(%d)", int(s))
	}
}

// RoleSpecType represents types of role specifications - ported from postgres/src/include/nodes/parsenodes.h:383-389
type RoleSpecType int

const (
	ROLESPEC_CSTRING      RoleSpecType = iota // role name is stored as a C string
	ROLESPEC_CURRENT_ROLE                     // role spec is CURRENT_ROLE
	ROLESPEC_CURRENT_USER                     // role spec is CURRENT_USER
	ROLESPEC_SESSION_USER                     // role spec is SESSION_USER
	ROLESPEC_PUBLIC                           // role name is "public"
)

func (r RoleSpecType) String() string {
	switch r {
	case ROLESPEC_CSTRING:
		return "CSTRING"
	case ROLESPEC_CURRENT_ROLE:
		return "CURRENT_ROLE"
	case ROLESPEC_CURRENT_USER:
		return "CURRENT_USER"
	case ROLESPEC_SESSION_USER:
		return "SESSION_USER"
	case ROLESPEC_PUBLIC:
		return "PUBLIC"
	default:
		return fmt.Sprintf("RoleSpecType(%d)", int(r))
	}
}

// RoleSpec represents a role specification.
// Ported from postgres/src/include/nodes/parsenodes.h:401
type RoleSpec struct {
	BaseNode
	Roletype RoleSpecType // Type of this rolespec - postgres/src/include/nodes/parsenodes.h:403
	Rolename string       // filled only for ROLESPEC_CSTRING - postgres/src/include/nodes/parsenodes.h:404
}

// NewRoleSpec creates a new RoleSpec node.
func NewRoleSpec(roletype RoleSpecType, rolename string) *RoleSpec {
	return &RoleSpec{
		BaseNode: BaseNode{Tag: T_RoleSpec},
		Roletype: roletype,
		Rolename: rolename,
	}
}

func (r *RoleSpec) String() string {
	if r.Roletype == ROLESPEC_CSTRING {
		return fmt.Sprintf("RoleSpec(%s)@%d", r.Rolename, r.Location())
	}
	return fmt.Sprintf("RoleSpec(%s)@%d", r.Roletype, r.Location())
}

// ==============================================================================
// CORE DDL SUPPORTING STRUCTURES
// ==============================================================================

// TypeName represents a type name specification.
// This is a placeholder implementation - full TypeName from parsenodes.h will be implemented later
type TypeName struct {
	BaseNode
	Names       []string // qualified name (list of String)
	TypeOid     Oid      // type's OID (filled in by transformTypeName)
	Setof       bool     // is a set?
	PctType     bool     // %TYPE specified?
	Typmods     *NodeList // type modifier expression(s)
	Typemod     int32    // prespecified type modifier
	ArrayBounds *NodeList // array bounds
}

// NewTypeName creates a new TypeName node.
func NewTypeName(names []string) *TypeName {
	return &TypeName{
		BaseNode: BaseNode{Tag: T_TypeName},
		Names:    names,
	}
}

func (t *TypeName) String() string {
	typeName := ""
	if len(t.Names) > 0 {
		typeName = t.Names[len(t.Names)-1]
	}
	return fmt.Sprintf("TypeName(%s)@%d", typeName, t.Location())
}

// SqlString returns the SQL representation of the TypeName
func (t *TypeName) SqlString() string {
	if len(t.Names) == 0 {
		return ""
	}
	
	// Join qualified names with dots (e.g., "schema.type")
	return strings.Join(t.Names, ".")
}

// CollateClause represents a COLLATE clause.
// This is a placeholder implementation - full CollateClause from parsenodes.h will be implemented later
type CollateClause struct {
	BaseNode
	Arg      Node      // input expression
	Collname *NodeList // possibly-qualified collation name
}

// NewCollateClause creates a new CollateClause node.
func NewCollateClause(collname *NodeList) *CollateClause {
	return &CollateClause{
		BaseNode: BaseNode{Tag: T_CollateClause, Loc: -1},
		Collname: collname,
	}
}

func (c *CollateClause) String() string {
	collName := ""
	if c.Collname != nil && c.Collname.Len() > 0 {
		// Get the last element (unqualified name)
		if lastNode := c.Collname.Items[c.Collname.Len()-1]; lastNode != nil {
			if strNode, ok := lastNode.(*String); ok {
				collName = strNode.SVal
			}
		}
	}
	return fmt.Sprintf("CollateClause(%s)@%d", collName, c.Location())
}

// ==============================================================================

// DefElem represents a generic name/value pair for options.
// Ported from postgres/src/include/nodes/parsenodes.h:811
type DefElem struct {
	BaseNode
	Defnamespace string        // NULL if unqualified name - postgres/src/include/nodes/parsenodes.h:813
	Defname      string        // postgres/src/include/nodes/parsenodes.h:814
	Arg          Node          // typically Integer, Float, String, or TypeName - postgres/src/include/nodes/parsenodes.h:815
	Defaction    DefElemAction // unspecified action, or SET/ADD/DROP - postgres/src/include/nodes/parsenodes.h:816
}

// NewDefElem creates a new DefElem node.
func NewDefElem(defname string, arg Node) *DefElem {
	return &DefElem{
		BaseNode:  BaseNode{Tag: T_DefElem},
		Defname:   defname,
		Arg:       arg,
		Defaction: DEFELEM_UNSPEC,
	}
}

func (d *DefElem) String() string {
	action := ""
	if d.Defaction != DEFELEM_UNSPEC {
		action = fmt.Sprintf(" %s", d.Defaction)
	}
	return fmt.Sprintf("DefElem(%s%s)@%d", d.Defname, action, d.Location())
}

// Constraint represents a constraint definition.
// Ported from postgres/src/include/nodes/parsenodes.h:2728
type Constraint struct {
	BaseNode
	Contype            ConstrType // see above - postgres/src/include/nodes/parsenodes.h:2731
	Conname            string     // Constraint name, or NULL if unnamed - postgres/src/include/nodes/parsenodes.h:2732
	Deferrable         bool       // DEFERRABLE? - postgres/src/include/nodes/parsenodes.h:2733
	Initdeferred       bool       // INITIALLY DEFERRED? - postgres/src/include/nodes/parsenodes.h:2734
	SkipValidation     bool       // skip validation of existing rows? - postgres/src/include/nodes/parsenodes.h:2735
	InitiallyValid     bool       // mark the new constraint as valid? - postgres/src/include/nodes/parsenodes.h:2736
	IsNoInherit        bool       // is constraint non-inheritable? - postgres/src/include/nodes/parsenodes.h:2737
	RawExpr            Node       // CHECK or DEFAULT expression, as untransformed parse tree - postgres/src/include/nodes/parsenodes.h:2738
	CookedExpr         string     // CHECK or DEFAULT expression, as nodeToString representation - postgres/src/include/nodes/parsenodes.h:2740
	GeneratedWhen      byte       // ALWAYS or BY DEFAULT - postgres/src/include/nodes/parsenodes.h:2742
	Inhcount           int        // initial inheritance count to apply - postgres/src/include/nodes/parsenodes.h:2743
	NullsNotDistinct   bool       // null treatment for UNIQUE constraints - postgres/src/include/nodes/parsenodes.h:2744
	Keys               []string   // String nodes naming referenced key column(s) - postgres/src/include/nodes/parsenodes.h:2746
	Including          []string   // String nodes naming referenced nonkey column(s) - postgres/src/include/nodes/parsenodes.h:2747
	Exclusions         *NodeList  // list of (IndexElem, operator name) pairs - postgres/src/include/nodes/parsenodes.h:2748
	Options            *NodeList  // options from WITH clause - postgres/src/include/nodes/parsenodes.h:2749
	Indexname          string     // existing index to use; otherwise NULL - postgres/src/include/nodes/parsenodes.h:2750
	Indexspace         string     // index tablespace; NULL for default - postgres/src/include/nodes/parsenodes.h:2751
	ResetDefaultTblspc bool       // reset default_tablespace prior to creating the index - postgres/src/include/nodes/parsenodes.h:2752
	AccessMethod       string     // access method to use for the index - postgres/src/include/nodes/parsenodes.h:2753
	WhereClause        Node       // partial index predicate - postgres/src/include/nodes/parsenodes.h:2754
	Pktable            *RangeVar  // Primary key table for FOREIGN KEY - postgres/src/include/nodes/parsenodes.h:2755
	FkAttrs            []string   // Attributes of foreign key - postgres/src/include/nodes/parsenodes.h:2756
	PkAttrs            []string   // Corresponding attrs in PK table - postgres/src/include/nodes/parsenodes.h:2757
	FkMatchtype        byte       // FULL, PARTIAL, SIMPLE - postgres/src/include/nodes/parsenodes.h:2758
	FkUpdAction        byte       // ON UPDATE action - postgres/src/include/nodes/parsenodes.h:2759
	FkDelAction        byte       // ON DELETE action - postgres/src/include/nodes/parsenodes.h:2760
	FkDelSetCols       []string   // ON DELETE SET NULL/DEFAULT (column_list) - postgres/src/include/nodes/parsenodes.h:2761
	OldConpfeqop       []Oid      // pg_constraint.conpfeqop of my former self - postgres/src/include/nodes/parsenodes.h:2762
	OldPktableOid      Oid        // pg_class.oid of my former self - postgres/src/include/nodes/parsenodes.h:2763
}

// NewConstraint creates a new Constraint node.
func NewConstraint(contype ConstrType) *Constraint {
	return &Constraint{
		BaseNode: BaseNode{Tag: T_Constraint},
		Contype:  contype,
	}
}

func (c *Constraint) String() string {
	name := c.Conname
	if name == "" {
		name = "unnamed"
	}
	return fmt.Sprintf("Constraint(%s %s)@%d", c.Contype, name, c.Location())
}

// ==============================================================================
// ALTER TABLE STATEMENTS
// ==============================================================================

// AlterTableStmt represents an ALTER TABLE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2339
type AlterTableStmt struct {
	BaseNode
	Relation  *RangeVar        // table to work on - postgres/src/include/nodes/parsenodes.h:2341
	Cmds      []*AlterTableCmd // list of subcommands - postgres/src/include/nodes/parsenodes.h:2342
	Objtype   ObjectType       // type of object - postgres/src/include/nodes/parsenodes.h:2344
	MissingOk bool             // skip error if table missing - postgres/src/include/nodes/parsenodes.h:2345
}

// NewAlterTableStmt creates a new AlterTableStmt node.
func NewAlterTableStmt(relation *RangeVar, cmds []*AlterTableCmd) *AlterTableStmt {
	return &AlterTableStmt{
		BaseNode: BaseNode{Tag: T_AlterTableStmt},
		Relation: relation,
		Cmds:     cmds,
		Objtype:  OBJECT_TABLE,
	}
}

func (a *AlterTableStmt) StatementType() string {
	return "AlterTableStmt"
}

func (a *AlterTableStmt) String() string {
	return fmt.Sprintf("AlterTableStmt(%s, %d cmds)@%d", a.Relation.RelName, len(a.Cmds), a.Location())
}

// AlterTableCmd represents one subcommand of an ALTER TABLE.
// Ported from postgres/src/include/nodes/parsenodes.h:2426
type AlterTableCmd struct {
	BaseNode
	Subtype   AlterTableType // Type of table alteration to apply - postgres/src/include/nodes/parsenodes.h:2429
	Name      string         // column, constraint, or trigger to act on - postgres/src/include/nodes/parsenodes.h:2430
	Num       int16          // attribute number for columns referenced by number - postgres/src/include/nodes/parsenodes.h:2432
	Newowner  *RoleSpec      // postgres/src/include/nodes/parsenodes.h:2434
	Def       Node           // definition of new column, index, constraint, or parent table - postgres/src/include/nodes/parsenodes.h:2435
	Behavior  DropBehavior   // RESTRICT or CASCADE for DROP cases - postgres/src/include/nodes/parsenodes.h:2437
	MissingOk bool           // skip error if missing? - postgres/src/include/nodes/parsenodes.h:2438
	Recurse   bool           // exec-time recursion - postgres/src/include/nodes/parsenodes.h:2439
}

// NewAlterTableCmd creates a new AlterTableCmd node.
func NewAlterTableCmd(subtype AlterTableType, name string, def Node) *AlterTableCmd {
	return &AlterTableCmd{
		BaseNode: BaseNode{Tag: T_AlterTableCmd},
		Subtype:  subtype,
		Name:     name,
		Def:      def,
		Behavior: DropRestrict,
	}
}

func (a *AlterTableCmd) String() string {
	return fmt.Sprintf("AlterTableCmd(%s %s)@%d", a.Subtype, a.Name, a.Location())
}

// ==============================================================================
// INDEX STATEMENTS
// ==============================================================================

// IndexStmt represents a CREATE INDEX statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3348
type IndexStmt struct {
	BaseNode
	Idxname                  string       // name of new index, or NULL for default - postgres/src/include/nodes/parsenodes.h:3350
	Relation                 *RangeVar    // relation to build index on - postgres/src/include/nodes/parsenodes.h:3351
	AccessMethod             string       // name of access method (eg. btree) - postgres/src/include/nodes/parsenodes.h:3352
	TableSpace               string       // tablespace, or NULL for default - postgres/src/include/nodes/parsenodes.h:3353
	IndexParams              []*IndexElem // columns to index: a list of IndexElem - postgres/src/include/nodes/parsenodes.h:3354
	IndexIncludingParams     []*IndexElem // additional columns to index - postgres/src/include/nodes/parsenodes.h:3355
	Options                  []*DefElem   // WITH clause options: a list of DefElem - postgres/src/include/nodes/parsenodes.h:3357
	WhereClause              Node         // qualification (partial-index predicate) - postgres/src/include/nodes/parsenodes.h:3358
	ExcludeOpNames           *NodeList    // exclusion operator names, or NIL if none - postgres/src/include/nodes/parsenodes.h:3359
	Idxcomment               string       // comment to apply to index, or NULL - postgres/src/include/nodes/parsenodes.h:3360
	IndexOid                 Oid          // OID of an existing index, if any - postgres/src/include/nodes/parsenodes.h:3361
	OldNumber                uint32       // relfilenumber of existing storage, if any - postgres/src/include/nodes/parsenodes.h:3362
	OldCreateSubid           uint32       // rd_createSubid of existing storage, if any - postgres/src/include/nodes/parsenodes.h:3363
	OldFirstRelfilenodeSubid uint32       // rd_firstRelfilenodeSubid of existing storage - postgres/src/include/nodes/parsenodes.h:3364
	Unique                   bool         // is index unique? - postgres/src/include/nodes/parsenodes.h:3365
	NullsNotDistinct       bool         // null treatment in unique index - postgres/src/include/nodes/parsenodes.h:3366
	Primary                  bool         // is index a primary key? - postgres/src/include/nodes/parsenodes.h:3367
	Isconstraint             bool         // is it for a pkey/unique constraint? - postgres/src/include/nodes/parsenodes.h:3368
	Deferrable               bool         // is the constraint DEFERRABLE? - postgres/src/include/nodes/parsenodes.h:3369
	Initdeferred             bool         // is the constraint INITIALLY DEFERRED? - postgres/src/include/nodes/parsenodes.h:3370
	Transformed              bool         // true when transformIndexStmt is finished - postgres/src/include/nodes/parsenodes.h:3371
	Concurrent               bool         // should this be a concurrent index build? - postgres/src/include/nodes/parsenodes.h:3372
	IfNotExists              bool         // just do nothing if index already exists? - postgres/src/include/nodes/parsenodes.h:3373
	ResetDefaultTblspc       bool         // reset default_tablespace prior to creating the index - postgres/src/include/nodes/parsenodes.h:3374
}

// NewIndexStmt creates a new IndexStmt node.
func NewIndexStmt(idxname string, relation *RangeVar, indexParams []*IndexElem) *IndexStmt {
	return &IndexStmt{
		BaseNode:     BaseNode{Tag: T_IndexStmt},
		Idxname:      idxname,
		Relation:     relation,
		IndexParams:  indexParams,
		AccessMethod: "btree", // default access method
	}
}

func (i *IndexStmt) StatementType() string {
	return "IndexStmt"
}

func (i *IndexStmt) String() string {
	unique := ""
	if i.Unique {
		unique = " UNIQUE"
	}
	return fmt.Sprintf("IndexStmt(%s%s on %s)@%d", i.Idxname, unique, i.Relation.RelName, i.Location())
}

// IndexElem represents one index element (column or expression).
// Ported from postgres/src/include/nodes/parsenodes.h:780
type IndexElem struct {
	BaseNode
	Name          string      // name of attribute to index, or NULL - postgres/src/include/nodes/parsenodes.h:782
	Expr          Node        // expression to index, or NULL - postgres/src/include/nodes/parsenodes.h:783
	Indexcolname  string      // name for index column; NULL = default - postgres/src/include/nodes/parsenodes.h:784
	Collation     []string    // name of collation; NIL = default - postgres/src/include/nodes/parsenodes.h:785
	Opclass       []string    // name of desired opclass; NIL = default - postgres/src/include/nodes/parsenodes.h:786
	Opclassopts   []*DefElem  // opclass-specific options, or NIL - postgres/src/include/nodes/parsenodes.h:787
	Ordering      SortByDir   // ASC/DESC/default - postgres/src/include/nodes/parsenodes.h:788
	NullsOrdering SortByNulls // FIRST/LAST/default - postgres/src/include/nodes/parsenodes.h:789
}

// NewIndexElem creates a new IndexElem node.
func NewIndexElem(name string) *IndexElem {
	return &IndexElem{
		BaseNode:      BaseNode{Tag: T_IndexElem},
		Name:          name,
		Ordering:      SORTBY_DEFAULT,
		NullsOrdering: SORTBY_NULLS_DEFAULT,
	}
}

func (i *IndexElem) String() string {
	target := i.Name
	if target == "" && i.Expr != nil {
		target = "expr"
	}
	order := ""
	if i.Ordering != SORTBY_DEFAULT {
		order = fmt.Sprintf(" %s", i.Ordering)
	}
	return fmt.Sprintf("IndexElem(%s%s)@%d", target, order, i.Location())
}

func (i *IndexElem) SqlString() string {
	if i.Name != "" {
		return i.Name
	}
	if i.Expr != nil {
		return i.Expr.SqlString()
	}
	return ""
}

// ==============================================================================
// VIEW STATEMENTS
// ==============================================================================

// ViewStmt represents a CREATE VIEW statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3740
type ViewStmt struct {
	BaseNode
	View            *RangeVar       // the view to be created - postgres/src/include/nodes/parsenodes.h:3742
	Aliases         []string        // target column names - postgres/src/include/nodes/parsenodes.h:3743
	Query           Node            // the SELECT query (as a raw parse tree) - postgres/src/include/nodes/parsenodes.h:3744
	Replace         bool            // replace an existing view? - postgres/src/include/nodes/parsenodes.h:3745
	Options         []*DefElem      // options from WITH clause - postgres/src/include/nodes/parsenodes.h:3746
	WithCheckOption ViewCheckOption // WITH CHECK OPTION - postgres/src/include/nodes/parsenodes.h:3747
}

// NewViewStmt creates a new ViewStmt node.
func NewViewStmt(view *RangeVar, query Node, replace bool) *ViewStmt {
	return &ViewStmt{
		BaseNode:        BaseNode{Tag: T_ViewStmt},
		View:            view,
		Query:           query,
		Replace:         replace,
		WithCheckOption: NO_CHECK_OPTION,
	}
}

func (v *ViewStmt) StatementType() string {
	return "ViewStmt"
}

func (v *ViewStmt) String() string {
	replace := ""
	if v.Replace {
		replace = " OR REPLACE"
	}
	return fmt.Sprintf("ViewStmt(%s%s)@%d", v.View.RelName, replace, v.Location())
}

// ==============================================================================
// DOMAIN STATEMENTS
// ==============================================================================

// AlterDomainStmt represents an ALTER DOMAIN statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2461
type AlterDomainStmt struct {
	BaseNode
	Subtype   byte         // T/N/O/C/X for alter type - postgres/src/include/nodes/parsenodes.h:2463
	TypeName  []string     // domain to work on - postgres/src/include/nodes/parsenodes.h:2464
	Name      string       // column or constraint name to act on - postgres/src/include/nodes/parsenodes.h:2465
	Def       Node         // definition of default or constraint - postgres/src/include/nodes/parsenodes.h:2466
	Behavior  DropBehavior // RESTRICT or CASCADE for DROP cases - postgres/src/include/nodes/parsenodes.h:2467
	MissingOk bool         // skip error if missing? - postgres/src/include/nodes/parsenodes.h:2468
}

// NewAlterDomainStmt creates a new AlterDomainStmt node.
func NewAlterDomainStmt(subtype byte, typeName []string) *AlterDomainStmt {
	return &AlterDomainStmt{
		BaseNode: BaseNode{Tag: T_AlterDomainStmt},
		Subtype:  subtype,
		TypeName: typeName,
		Behavior: DropRestrict,
	}
}

func (a *AlterDomainStmt) StatementType() string {
	return "AlterDomainStmt"
}

func (a *AlterDomainStmt) String() string {
	domainName := ""
	if len(a.TypeName) > 0 {
		domainName = a.TypeName[len(a.TypeName)-1] // last part is the domain name
	}
	return fmt.Sprintf("AlterDomainStmt(%s)@%d", domainName, a.Location())
}

// CreateDomainStmt represents a CREATE DOMAIN statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3156
type CreateDomainStmt struct {
	BaseNode
	Domainname  []string       // qualified name (list of String) - postgres/src/include/nodes/parsenodes.h:3158
	TypeName    *TypeName      // the base type - postgres/src/include/nodes/parsenodes.h:3159
	CollClause  *CollateClause // untransformed COLLATE spec, if any - postgres/src/include/nodes/parsenodes.h:3160
	Constraints []*Constraint  // constraints (list of Constraint nodes) - postgres/src/include/nodes/parsenodes.h:3161
}

// NewCreateDomainStmt creates a new CreateDomainStmt node.
func NewCreateDomainStmt(domainname []string, typeName *TypeName) *CreateDomainStmt {
	return &CreateDomainStmt{
		BaseNode:   BaseNode{Tag: T_CreateDomainStmt},
		Domainname: domainname,
		TypeName:   typeName,
	}
}

func (c *CreateDomainStmt) StatementType() string {
	return "CreateDomainStmt"
}

func (c *CreateDomainStmt) String() string {
	domainName := ""
	if len(c.Domainname) > 0 {
		domainName = c.Domainname[len(c.Domainname)-1]
	}
	return fmt.Sprintf("CreateDomainStmt(%s)@%d", domainName, c.Location())
}

// ==============================================================================
// SCHEMA STATEMENTS
// ==============================================================================

// CreateSchemaStmt represents a CREATE SCHEMA statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2320
type CreateSchemaStmt struct {
	BaseNode
	Schemaname  string    // the name of the schema to create - postgres/src/include/nodes/parsenodes.h:2322
	Authrole    *RoleSpec // the owner of the created schema - postgres/src/include/nodes/parsenodes.h:2323
	SchemaElts  *NodeList // schema components (list of parsenodes) - postgres/src/include/nodes/parsenodes.h:2324
	IfNotExists bool      // just do nothing if schema already exists? - postgres/src/include/nodes/parsenodes.h:2325
}

// NewCreateSchemaStmt creates a new CreateSchemaStmt node.
func NewCreateSchemaStmt(schemaname string, ifNotExists bool) *CreateSchemaStmt {
	return &CreateSchemaStmt{
		BaseNode:    BaseNode{Tag: T_CreateSchemaStmt},
		Schemaname:  schemaname,
		IfNotExists: ifNotExists,
	}
}

func (c *CreateSchemaStmt) StatementType() string {
	return "CreateSchemaStmt"
}

func (c *CreateSchemaStmt) String() string {
	ifNotExists := ""
	if c.IfNotExists {
		ifNotExists = " IF NOT EXISTS"
	}
	return fmt.Sprintf("CreateSchemaStmt(%s%s)@%d", c.Schemaname, ifNotExists, c.Location())
}

// ==============================================================================
// EXTENSION STATEMENTS
// ==============================================================================

// CreateExtensionStmt represents a CREATE EXTENSION statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2819
type CreateExtensionStmt struct {
	BaseNode
	Extname     string     // extension name - postgres/src/include/nodes/parsenodes.h:2821
	IfNotExists bool       // just do nothing if it already exists? - postgres/src/include/nodes/parsenodes.h:2822
	Options     []*DefElem // List of DefElem nodes - postgres/src/include/nodes/parsenodes.h:2823
}

// NewCreateExtensionStmt creates a new CreateExtensionStmt node.
func NewCreateExtensionStmt(extname string, ifNotExists bool) *CreateExtensionStmt {
	return &CreateExtensionStmt{
		BaseNode:    BaseNode{Tag: T_CreateExtensionStmt},
		Extname:     extname,
		IfNotExists: ifNotExists,
	}
}

func (c *CreateExtensionStmt) StatementType() string {
	return "CreateExtensionStmt"
}

func (c *CreateExtensionStmt) String() string {
	ifNotExists := ""
	if c.IfNotExists {
		ifNotExists = " IF NOT EXISTS"
	}
	return fmt.Sprintf("CreateExtensionStmt(%s%s)@%d", c.Extname, ifNotExists, c.Location())
}

// ==============================================================================
// DDL CONVENIENCE CONSTRUCTORS
// ==============================================================================

// NewPrimaryKeyConstraint creates a PRIMARY KEY constraint.
func NewPrimaryKeyConstraint(conname string, keys []string) *Constraint {
	constraint := NewConstraint(CONSTR_PRIMARY)
	constraint.Conname = conname
	constraint.Keys = keys
	return constraint
}

// NewForeignKeyConstraint creates a FOREIGN KEY constraint.
func NewForeignKeyConstraint(conname string, fkAttrs []string, pktable *RangeVar, pkAttrs []string) *Constraint {
	constraint := NewConstraint(CONSTR_FOREIGN)
	constraint.Conname = conname
	constraint.FkAttrs = fkAttrs
	constraint.Pktable = pktable
	constraint.PkAttrs = pkAttrs
	return constraint
}

// NewUniqueConstraint creates a UNIQUE constraint.
func NewUniqueConstraint(conname string, keys []string) *Constraint {
	constraint := NewConstraint(CONSTR_UNIQUE)
	constraint.Conname = conname
	constraint.Keys = keys
	return constraint
}

// NewUniqueConstraintNullsNotDistinct creates a UNIQUE constraint with NULLS NOT DISTINCT.
func NewUniqueConstraintNullsNotDistinct(conname string, keys []string) *Constraint {
	constraint := NewConstraint(CONSTR_UNIQUE)
	constraint.Conname = conname
	constraint.Keys = keys
	constraint.NullsNotDistinct = true
	return constraint
}

// NewCheckConstraint creates a CHECK constraint.
func NewCheckConstraint(conname string, rawExpr Node) *Constraint {
	constraint := NewConstraint(CONSTR_CHECK)
	constraint.Conname = conname
	constraint.RawExpr = rawExpr
	return constraint
}

// NewNotNullConstraint creates a NOT NULL constraint.
func NewNotNullConstraint(conname string) *Constraint {
	constraint := NewConstraint(CONSTR_NOTNULL)
	constraint.Conname = conname
	return constraint
}

// NewAddColumnCmd creates an ALTER TABLE ADD COLUMN command.
func NewAddColumnCmd(columnName string, columnDef Node) *AlterTableCmd {
	return NewAlterTableCmd(AT_AddColumn, columnName, columnDef)
}

// NewDropColumnCmd creates an ALTER TABLE DROP COLUMN command.
func NewDropColumnCmd(columnName string, behavior DropBehavior) *AlterTableCmd {
	cmd := NewAlterTableCmd(AT_DropColumn, columnName, nil)
	cmd.Behavior = behavior
	return cmd
}

// NewAddConstraintCmd creates an ALTER TABLE ADD CONSTRAINT command.
func NewAddConstraintCmd(constraint *Constraint) *AlterTableCmd {
	return NewAlterTableCmd(AT_AddConstraint, constraint.Conname, constraint)
}

// NewDropConstraintCmd creates an ALTER TABLE DROP CONSTRAINT command.
func NewDropConstraintCmd(constraintName string, behavior DropBehavior) *AlterTableCmd {
	cmd := NewAlterTableCmd(AT_DropConstraint, constraintName, nil)
	cmd.Behavior = behavior
	return cmd
}

// NewUniqueIndex creates a unique index statement.
func NewUniqueIndex(idxname string, relation *RangeVar, indexParams []*IndexElem) *IndexStmt {
	idx := NewIndexStmt(idxname, relation, indexParams)
	idx.Unique = true
	return idx
}

// NewBtreeIndexElem creates a B-tree index element with default ordering.
func NewBtreeIndexElem(columnName string) *IndexElem {
	return NewIndexElem(columnName)
}

// NewDescIndexElem creates a descending index element.
func NewDescIndexElem(columnName string) *IndexElem {
	elem := NewIndexElem(columnName)
	elem.Ordering = SORTBY_DESC
	return elem
}

// NewExpressionIndexElem creates an index element on an expression.
func NewExpressionIndexElem(expr Node) *IndexElem {
	elem := &IndexElem{
		BaseNode:      BaseNode{Tag: T_IndexElem},
		Expr:          expr,
		Ordering:      SORTBY_DEFAULT,
		NullsOrdering: SORTBY_NULLS_DEFAULT,
	}
	return elem
}
