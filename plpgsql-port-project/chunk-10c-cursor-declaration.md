# Chunk 1.10c — Cursor declaration (`decl_cursor`)

The last cursor piece: declaring a bound cursor in the DECLARE section —
`name [[NO] SCROLL] CURSOR [(args)] {IS|FOR} query`. This is where the
variable-vs-cursor `decl_statement` disambiguation lives, which is why it was
split out from the cursor statements.

Ported from `pl_gram.y` (`decl_statement` cursor arm, `opt_scrollable`,
`decl_cursor_args`, `decl_cursor_arglist`, `decl_cursor_arg`, `decl_is_for`,
`decl_cursor_query`) and `plpgsql.h` (`PLpgSQL_var` cursor fields).

## The disambiguation (the reason for the split)

`decl_statement` gains a second alternative sharing the `any_identifier` prefix:

```text
decl_statement:
    any_identifier decl_const decl_datatype decl_notnull decl_defval   (variable)
  | any_identifier opt_scrollable K_CURSOR decl_cursor_args decl_is_for decl_cursor_query  (cursor)
```

After `any_identifier`, the parser chooses: shift `K_CONSTANT` (variable),
`K_SCROLL`/`K_NO` (cursor scroll), reduce `opt_scrollable → ε` on **`K_CURSOR`**
(cursor), or default-reduce `decl_const → ε` (variable, then `decl_datatype`
scans the type). `opt_scrollable → ε`'s lookahead is exactly `{K_CURSOR}` and
`decl_const → ε` is the default reduction, so the two are disjoint. **PG declares
`%expect 0`**, i.e. this is conflict-free in bison; the plan is to mirror it and
confirm `y.output` stays at our 0-conflict baseline. If goyacc diverges from
bison here, the fallback is a single-production peek dispatch (like `makeWordStmt`).

`ALIAS` (PG's third `decl_statement` arm) is **included** here too, since we're
already touching this state: `any_identifier K_ALIAS K_FOR decl_aliasitem ';'`,
where `decl_aliasitem` is `T_WORD | unreserved_keyword`. PG resolves the item to
an existing variable's namespace entry; we capture the target name as text. PG
has no decl-level alias node (it's a pure namespace side-effect), so we add a
small `PLpgSQL_alias` datum `{Refname, Target}` to carry it in the decl list.

## The datatype-terminator wrinkle

`decl_cursor_arg` is `any_identifier decl_datatype` (an arg name + type), and the
arg list is `'(' … ',' … ')'`. Our `readDatatype` currently stops at
`;`/`:=`/`=`/`DEFAULT`/`NOT` — not `,`/`)`, which cursor args need. Fix: add `,`
and `)` to `readDatatype`'s terminators. This is safe for the existing variable
context because those tokens only match at paren depth 0, which never follows a
variable-decl type (inner type parens like `numeric(10,2)` are at depth ≥1). So
one superset terminator set serves both contexts — matching PG, whose
`read_datatype` is context-independent.

## Grammar

```text
opt_scrollable:  /*empty*/ {0} | K_NO K_SCROLL {NO_SCROLL} | K_SCROLL {SCROLL}
decl_cursor_args: /*empty*/ | '(' decl_cursor_arglist ')'
decl_cursor_arglist: decl_cursor_arg | decl_cursor_arglist ',' decl_cursor_arg
decl_cursor_arg: any_identifier decl_datatype        // PLpgSQL_var{name, type}
decl_is_for:  K_IS | K_FOR
decl_cursor_query: /*empty*/ { readSQLConstruct(DEFAULT, ';') }   // the bound query
```

- `decl_cursor_query` is an empty scan production like `decl_defval`.
- `decl_cursor_arglist` appends via an `appendCursorArg` helper (goyacc
  fast-append hazard — same as `appendDatum`).
- PG's mid-rule `plpgsql_ns_push` (cursor-arg namespace) is dropped — no
  namespaces here.

## AST

Extend `PLpgSQL_var` with PG's cursor fields (a cursor is a `refcursor` variable
with extra properties):

- `CursorExplicitExpr *PLpgSQL_expr` — the bound query (nil ⇒ ordinary variable).
- `CursorOptions int` — `FAST_PLAN | scroll`, like OPEN.
- `CursorArgs []*PLpgSQL_var` — the declared args (each a name + `DataType`);
  replaces PG's `cursor_explicit_argrow` datum-number reference.

`PLpgSQL_var.SqlString` branches: when `CursorExplicitExpr != nil`, render the
cursor form; otherwise the existing variable form.

Deparse (canonical `FOR`, `IS` collapses to it):

```text
name[ SCROLL| NO SCROLL] CURSOR[ (arg1 type1, arg2 type2)] FOR <query>
```

## Scope — out (deferred)

- `ALIAS` declarations.
- Resolution (cursor query/args kept as text; no refcursor datum, no argrow).
- `PLpgSQL_expr.Parsed` still nil.

## Conflict / risk

- **The `decl_statement` disambiguation** — the whole reason for the split.
  Verify `y.output` reports 0 conflicts after adding the cursor arm; if not,
  fall back to a peek dispatch.
- The `readDatatype` terminator change must not regress the existing DECLARE
  cases — the `declare_cases.json` suite guards this.

## Acceptance

New `testdata/decl_cursor_cases.json`, golden deparse via `PLPGSQL_REWRITE=1`:

- `DECLARE c CURSOR FOR SELECT 1; BEGIN END`.
- `SCROLL` / `NO SCROLL` variants; `IS` (deparses to `FOR`).
- Args: `DECLARE c CURSOR (a int, b text) FOR SELECT a, b; BEGIN END`.
- A cursor decl alongside ordinary variable decls in the same DECLARE section.
- All existing `declare_cases.json` still pass (terminator change is safe).
- Round-trip stable; `make parser` 0 new conflicts; `go build` +
  `golangci-lint` clean; package `ast` byte-identical to upstream.
