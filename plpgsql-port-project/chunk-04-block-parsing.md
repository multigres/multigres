# Chunk 1.4 — Minimal block parsing (`BEGIN … END;`)

First real grammar work. Add the productions for the outermost block so
`ParsePLpgSQL` parses bodies like `BEGIN END;`, `BEGIN NULL; END;`, labeled
blocks, and nested blocks into a `PLpgSQL_stmt_block`. Ported from
`postgres/src/pl/plpgsql/src/pl_gram.y` (`pl_function`, `pl_block`, `proc_sect`,
`proc_stmt`, `stmt_null`, `opt_block_label`, `opt_label`).

## PG-faithful decisions (these differ from the original phase-1 sketch)

- **No `PLpgSQL_stmt_null` node.** PG does not build a node for `NULL` — its
  `stmt_null` returns `NULL` and `proc_sect` explicitly drops it
  (`if ($2 == NULL) $$ = $1`). So `BEGIN NULL; END;` parses to a block with an
  **empty body**. We mirror that exactly: `stmt_null` yields `nil`, `proc_sect`
  skips `nil`. (The phase sketch said "define a trivial stmt_null node" — PG
  says otherwise, and you asked to stay close to PG, so we don't.)
- **Empty input becomes a parse error.** A PL/pgSQL body must be a block; PG has
  no empty-body production. The chunk-1.1 scaffolding accepted `""` and returned
  an empty function as a landing point — that goes away. `ParsePLpgSQL("")` now
  returns an error; `ParsePLpgSQL("BEGIN END")` returns the function.

## Scope

### In

- Grammar productions in `plpgsql.y`:
  - `pl_function: pl_block opt_semi` — sets `function.Action = block`. (PG's
    `comp_options` — `#option dump` etc. — is deferred; rare and not relevant to
    static analysis.)
  - `pl_block: opt_block_label K_BEGIN proc_sect K_END opt_label` — builds a
    `PLpgSQL_stmt_block` (Label, Body), and validates the end label against the
    start label.
  - `proc_sect: /* empty */ | proc_sect proc_stmt` — the statement list, with
    PG's "drop nil statements" behavior.
  - `proc_stmt: pl_block ';' | stmt_null` — a nested block (with its trailing
    `;`) or `NULL`. Other statement families (`stmt_assign`, `stmt_if`, …) are
    added by their own chunks.
  - `stmt_null: K_NULL ';'` → `nil`.
  - `opt_block_label: /* empty */ | LESS_LESS any_identifier GREATER_GREATER`.
  - `opt_label: /* empty */ | any_identifier`.
  - `any_identifier: T_WORD | unreserved_keyword`, with the full
    `unreserved_keyword` production (all 82 `K_*` unreserved tokens, listed as
    in pl_gram.y with the default `$$ = $1` carrying each keyword's text). So an
    unreserved keyword can be used as a label/identifier, matching PG.
  - `opt_semi: /* empty */ | ';'`.
- `%union`/`%type` additions: `block *PLpgSQL_stmt_block`, `stmt
plpgsqlast.Stmt`, `stmts []plpgsqlast.Stmt`; labels reuse the `str` `%struct`
  field.
- A `checkLabels(start, end string) error` helper mirroring PG's `check_labels`:
  an end label is allowed only if it matches the block's start label; otherwise
  a parse error ("end label X differs from block's label Y" / "end label X
  specified for unlabelled block"). Reported via the lexer's `Error`.
- Update `api.go` doc + the chunk-1.1 smoke test for the new "empty input is an
  error / blocks parse" behavior.
- Table-driven parse tests asserting the AST (see Acceptance).

### Out (deferred to the chunk that needs it)

- `decl_sect` / `DECLARE` and the datum family — chunk 1.5. (For now the block
  has no declaration section; `pl_block` starts at `opt_block_label`.)
- `exception_sect` / `EXCEPTION` — chunk 1.12.
- The `T_DATUM` alternative of `any_identifier` — `T_DATUM` is not emitted until
  variable resolution (chunk 1.14), so it is added then. (`unreserved_keyword`
  is included now, so keyword labels like `<<query>>` parse.)
- All other statement families — their own chunks.

## Implementation notes

- **`pl_block` returns `*PLpgSQL_stmt_block` but `proc_stmt` is a `Stmt`** — the
  block implements `Stmt`, so `proc_stmt: pl_block ';' { $$ = $1 }` assigns
  cleanly.
- **nil-interface care:** `stmt_null` sets `$$ = nil` (a literal nil `Stmt`), so
  `proc_sect`'s `$2 == nil` test is a true nil-interface check, not the
  nil-pointer-in-interface gotcha.
- **No lexer changes** — chunk 1.2 already emits `K_BEGIN`, `K_END`, `K_NULL`,
  `LESS_LESS`, `GREATER_GREATER`, `T_WORD`, and `;`.

## Acceptance

Table-driven parse tests (assert the AST, not JSON for now — JSON/corpus
fixtures arrive with the regression harness in 1.15):

- `BEGIN END` and `BEGIN END;` → function with an empty-label, empty-body block.
- `BEGIN NULL; END;` and `BEGIN NULL; NULL; END;` → empty-body block (NULL
  discarded, matching PG).
- `<<outer>> BEGIN END outer;` → block `Label == "outer"`.
- `<<outer>> BEGIN END;` → `Label == "outer"`, no end label, OK.
- `BEGIN BEGIN NULL; END; END;` → outer block whose `Body` is one nested block.
- `<<a>> BEGIN END b;` and `BEGIN END x;` → parse error (label mismatch /
  end-label-on-unlabelled-block).
- `ParsePLpgSQL("")` → error.
- `make parser` regenerates `plpgsql.go` deterministically; `go build` green;
  `golangci-lint` clean; package `ast` untouched.

## Risks / things to verify

- **goyacc conflicts.** The first real productions may surface shift/reduce
  conflicts (e.g. around `opt_label` / `opt_semi`). Resolve or document; the
  grammar must generate cleanly under `make parser`.
- **End-label error path.** Confirm a label mismatch makes `ParsePLpgSQL` return
  an error (the lexer's `Error` sets `lex.err`, which `api.go` surfaces) rather
  than silently producing a tree.
- **`%type <str>` on productions.** Labels flow through the `str` `%struct`
  field the lexer fills; confirm goyacc threads `$1`/`$$` correctly for
  string-typed productions (postgres.y does this, so the pattern is proven).
