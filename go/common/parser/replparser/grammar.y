%{
// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//  Portions Copyright (c) 2026, Supabase, Inc
//
//  Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//  Portions Copyright (c) 1994, The Regents of the University of California
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
//
// Ported from postgres/src/backend/replication/repl_gram.y (PG 17.6),
// dropping physical-replication commands (BASE_BACKUP, TIMELINE_HISTORY,
// UPLOAD_MANIFEST, physical START_REPLICATION, K_PHYSICAL, opt_physical,
// opt_timeline, opt_slot).

package replparser

import (
	"fmt"

	"github.com/multigres/multigres/go/common/parser/ast"
)

%}

%union {
	str     string
	boolval bool
	uintval uint32
	recptr  ast.XLogRecPtr
	node    ast.Node
	defelt  *ast.DefElem
	list    []*ast.DefElem
}

/* Non-keyword tokens — repl_gram.y:54-57 */
%token <str>     SCONST IDENT
%token <uintval> UCONST
%token <recptr>  RECPTR

/* Keyword tokens — repl_gram.y:59-80 (physical-only tokens dropped) */
%token K_IDENTIFY_SYSTEM
%token K_READ_REPLICATION_SLOT
%token K_SHOW
%token K_START_REPLICATION
%token K_CREATE_REPLICATION_SLOT
%token K_DROP_REPLICATION_SLOT
%token K_ALTER_REPLICATION_SLOT
%token K_WAIT
%token K_TIMELINE
%token K_LOGICAL
%token K_SLOT
%token K_RESERVE_WAL
%token K_TEMPORARY
%token K_TWO_PHASE
%token K_EXPORT_SNAPSHOT
%token K_NOEXPORT_SNAPSHOT
%token K_USE_SNAPSHOT

%type <node>    command
%type <node>    start_logical_replication
%type <node>    create_replication_slot drop_replication_slot
%type <node>    alter_replication_slot identify_system read_replication_slot
%type <node>    show
%type <list>    generic_option_list
%type <defelt>  generic_option
%type <list>    plugin_options plugin_opt_list
%type <defelt>  plugin_opt_elem
%type <node>    plugin_opt_arg
%type <str>     var_name ident_or_keyword
%type <boolval> opt_temporary
%type <list>    create_slot_options create_slot_legacy_opt_list
%type <defelt>  create_slot_legacy_opt

%%

/* repl_gram.y:100-104 */
firstcmd: command opt_semicolon
            {
                if l, ok := replYylex.(interface{ setResult(ast.Stmt) }); ok {
                    if s, ok := $1.(ast.Stmt); ok {
                        l.setResult(s)
                    }
                }
            }
        ;

/* repl_gram.y:106-108 */
opt_semicolon:	';'
            | /* EMPTY */
            ;

/* repl_gram.y:110-122 (physical-only alternatives dropped) */
command:
            identify_system
            | start_logical_replication
            | create_replication_slot
            | drop_replication_slot
            | alter_replication_slot
            | read_replication_slot
            | show
            ;

/* repl_gram.y:127-132 */
identify_system:
            K_IDENTIFY_SYSTEM
                {
                    $$ = ast.NewIdentifySystemCmd()
                }
            ;

/* repl_gram.y:137-144 */
read_replication_slot:
            K_READ_REPLICATION_SLOT var_name
                {
                    $$ = ast.NewReadReplicationSlotCmd($2)
                }
            ;

/* repl_gram.y:149-160 */
show:
            K_SHOW var_name
                {
                    $$ = ast.NewVariableShowStmt($2)
                }
            ;

var_name:	IDENT	{ $$ = $1 }
            | var_name '.' IDENT
                { $$ = fmt.Sprintf("%s.%s", $1, $3) }
        ;

/* repl_gram.y:179-203 — LOGICAL alternative only */
create_replication_slot:
            K_CREATE_REPLICATION_SLOT IDENT opt_temporary K_LOGICAL IDENT create_slot_options
                {
                    cmd := ast.NewCreateReplicationSlotCmd($2, ast.ReplicationKindLogical, $5, $3)
                    cmd.Options = $6
                    $$ = cmd
                }
            ;

/* repl_gram.y:205-208 */
create_slot_options:
            '(' generic_option_list ')'			{ $$ = $2 }
            | create_slot_legacy_opt_list		{ $$ = $1 }
            ;

/* repl_gram.y:210-215 */
create_slot_legacy_opt_list:
            create_slot_legacy_opt_list create_slot_legacy_opt
                { $$ = append($1, $2) }
            | /* EMPTY */
                { $$ = nil }
            ;

/* repl_gram.y:217-243 */
create_slot_legacy_opt:
            K_EXPORT_SNAPSHOT
                {
                    $$ = ast.NewDefElem("snapshot", ast.NewString("export"))
                }
            | K_NOEXPORT_SNAPSHOT
                {
                    $$ = ast.NewDefElem("snapshot", ast.NewString("nothing"))
                }
            | K_USE_SNAPSHOT
                {
                    $$ = ast.NewDefElem("snapshot", ast.NewString("use"))
                }
            | K_RESERVE_WAL
                {
                    $$ = ast.NewDefElem("reserve_wal", ast.NewBoolean(true))
                }
            | K_TWO_PHASE
                {
                    $$ = ast.NewDefElem("two_phase", ast.NewBoolean(true))
                }
            ;

/* repl_gram.y:246-263 */
drop_replication_slot:
            K_DROP_REPLICATION_SLOT IDENT
                {
                    $$ = ast.NewDropReplicationSlotCmd($2, false)
                }
            | K_DROP_REPLICATION_SLOT IDENT K_WAIT
                {
                    $$ = ast.NewDropReplicationSlotCmd($2, true)
                }
            ;

/* repl_gram.y:266-275 */
alter_replication_slot:
            K_ALTER_REPLICATION_SLOT IDENT '(' generic_option_list ')'
                {
                    $$ = ast.NewAlterReplicationSlotCmd($2, $4)
                }
            ;

/* repl_gram.y:294-306 — LOGICAL only */
start_logical_replication:
            K_START_REPLICATION K_SLOT IDENT K_LOGICAL RECPTR plugin_options
                {
                    $$ = ast.NewStartReplicationCmd(ast.ReplicationKindLogical, $3, 0, $5, $6)
                }
            ;

/* repl_gram.y:341-344 */
opt_temporary:
            K_TEMPORARY						{ $$ = true }
            | /* EMPTY */					{ $$ = false }
            ;

/* repl_gram.y:366-369 */
plugin_options:
            '(' plugin_opt_list ')'			{ $$ = $2 }
            | /* EMPTY */					{ $$ = nil }
        ;

/* repl_gram.y:371-380 */
plugin_opt_list:
            plugin_opt_elem
                {
                    $$ = []*ast.DefElem{$1}
                }
            | plugin_opt_list ',' plugin_opt_elem
                {
                    $$ = append($1, $3)
                }
        ;

/* repl_gram.y:382-387 */
plugin_opt_elem:
            IDENT plugin_opt_arg
                {
                    $$ = ast.NewDefElem($1, $2)
                }
        ;

/* repl_gram.y:389-392 */
plugin_opt_arg:
            SCONST							{ $$ = ast.NewString($1) }
            | /* EMPTY */					{ $$ = nil }
        ;

/* repl_gram.y:394-399 */
generic_option_list:
            generic_option_list ',' generic_option
                { $$ = append($1, $3) }
            | generic_option
                { $$ = []*ast.DefElem{$1} }
            ;

/* repl_gram.y:401-418 */
generic_option:
            ident_or_keyword
                {
                    $$ = ast.NewDefElem($1, nil)
                }
            | ident_or_keyword IDENT
                {
                    $$ = ast.NewDefElem($1, ast.NewString($2))
                }
            | ident_or_keyword SCONST
                {
                    $$ = ast.NewDefElem($1, ast.NewString($2))
                }
            | ident_or_keyword UCONST
                {
                    $$ = ast.NewDefElem($1, ast.NewInteger(int($2)))
                }
            ;

/* repl_gram.y:420-442 (physical-only keywords dropped) */
ident_or_keyword:
            IDENT							{ $$ = $1 }
            | K_IDENTIFY_SYSTEM				{ $$ = "identify_system" }
            | K_SHOW						{ $$ = "show" }
            | K_START_REPLICATION			{ $$ = "start_replication" }
            | K_CREATE_REPLICATION_SLOT		{ $$ = "create_replication_slot" }
            | K_DROP_REPLICATION_SLOT		{ $$ = "drop_replication_slot" }
            | K_ALTER_REPLICATION_SLOT		{ $$ = "alter_replication_slot" }
            | K_WAIT						{ $$ = "wait" }
            | K_TIMELINE					{ $$ = "timeline" }
            | K_LOGICAL						{ $$ = "logical" }
            | K_SLOT						{ $$ = "slot" }
            | K_RESERVE_WAL					{ $$ = "reserve_wal" }
            | K_TEMPORARY					{ $$ = "temporary" }
            | K_TWO_PHASE					{ $$ = "two_phase" }
            | K_EXPORT_SNAPSHOT				{ $$ = "export_snapshot" }
            | K_NOEXPORT_SNAPSHOT			{ $$ = "noexport_snapshot" }
            | K_USE_SNAPSHOT				{ $$ = "use_snapshot" }
        ;

%%
