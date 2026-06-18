# Chunk 1.3a — Clone/rewrite generator machinery for `plpgsqlast`

Wire `go/tools/asthelpergen` to the `plpgsqlast` subpackage so the PL/pgSQL
nodes get generated `Clone`/`Rewrite` helpers against the `plpgsqlast.Node`
interface — the analogue of what package `ast` already has, self-contained in
the subpackage. This is the infrastructure the Tier-1 walker will use to
traverse a parsed body.

## What asthelpergen generates

Pointed at a package with `--iface <pkg>.Node`, it discovers every type
implementing that interface and emits two files into the package:

- `ast_rewrite.go` — `Rewrite()` visitor + `rewriteRefOf<T>` per type.
- `ast_clone.go` — `CloneNode()` + `CloneRefOf<T>` per type.

So `plpgsqlast` gets its own `plpgsqlast.Rewrite` / `plpgsqlast.CloneNode`,
keyed on `plpgsqlast.Node`, completely separate from `ast`'s.

## The boundary problem (the whole reason this chunk is non-trivial)

`PLpgSQL_expr.Parsed` is typed `ast.Stmt` — a foreign interface from the
_other_ hierarchy. That is the one cross-hierarchy edge, and the two generators
treat it very differently:

- **Rewrite — already correct, no work needed.** `rewriteAllFields` only
  descends into a field when `types.Implements(field.Type(), plpgsqlast.Node)`.
  `ast.Stmt` does not implement `plpgsqlast.Node`, so `Parsed` is silently
  skipped — exactly the opaque boundary we want. The Tier-1 walker hands
  `Parsed` to the SQL-side `analyzeStatement` separately; `plpgsqlast.Rewrite`
  must NOT descend into it, and it won't.

- **Clone — broken as-is.** `ptrToStructMethod` deep-clones _every_ owned,
  non-basic field, so for `Parsed` it would emit `CloneStmt(n.Parsed)`. Two
  failures: (1) the generator prints types with `noQualifier`, so `ast.Stmt`
  collapses to `"Stmt"`, colliding with our own `plpgsqlast.Stmt`; (2)
  `findImplementations` searches only the `plpgsqlast` scope, finds no
  implementors of `ast.Stmt`, and emits a dead `CloneStmt` returning `nil` —
  i.e. it would drop the parsed SQL on clone even if it compiled.

So this chunk is really: **get clone to treat the `ast.Stmt` boundary as
opaque (shallow-copy), the same way rewrite already does.**

## Options for the clone boundary

### A. Type `Parsed` as `any`

`readValueOfType` returns `any`/`interface{}` fields verbatim, so clone
shallow-copies and rewrite skips — zero tool change. **Cost:** throws away the
clean `ast.Stmt` typing we just established; every caller casts. Rejected on
the "stay close to PG / clean types" grounds we set for chunk 1.3.

### B. Teach clone to shallow-copy foreign-package types (recommended)

Make the clone generator deep-clone only types it _owns_ (same package as the
root interface) and shallow-copy anything from another package — the ownership
principle rewrite already embodies via its `Implements` gate. Concretely: in
`cloneGen.ptrToStructMethod`'s field loop, if `field.Type()`'s package differs
from the generated package, skip the deep-clone line (the leading `out := *n`
already shallow-copies it). This sidesteps the `noQualifier` collision entirely
(no `Clone*` call is emitted for the field) and keeps `Parsed ast.Stmt`.

- **Pro:** keeps the clean type; principled and general; makes clone consistent
  with rewrite; opt-in-by-nature (only triggers on foreign fields).
- **Con:** a change to the shared `asthelpergen`. Must verify it does not change
  the existing `ast` output — see risk below.

### C. Unexported `_`-prefixed field + typed accessors

Name the field `_parsed ast.Stmt` (clone skips `_`-prefixed fields; rewrite
skips non-Node), expose `Parsed()`/`SetParsed()`. Keeps the real type
internally, no tool change. **Cost:** unexported storage + accessor methods,
and the walker reads via a method, not a field. Uglier than B at the call site.

**Recommendation: B.** It keeps the PG-faithful `Parsed ast.Stmt`, encodes the
correct ownership rule once in the tool, and aligns clone with rewrite.

## Scope

### In

- New `go/common/parser/ast/plpgsqlast/generate.go` with the `//go:generate`
  directive: `asthelpergen --in . --iface <…>/plpgsqlast.Node`.
- Hook it into `make parser` (the `parser` target runs
  `go generate ./go/common/parser/...`, which should pick it up — verify, and
  add the path if not).
- Generated `plpgsqlast/ast_clone.go` and `plpgsqlast/ast_rewrite.go`, committed.
- The clone-boundary fix (option B) in `go/tools/asthelpergen/clone_gen.go`.
- A small round-trip test: clone a `plpgsqlast` tree (a block holding a
  `PLpgSQL_expr` whose `Parsed` is a non-nil `ast.Stmt`) and assert the clone is
  deep for plpgsqlast nodes but **shares** the same `Parsed` pointer (shallow at
  the boundary); a `Rewrite` smoke test that visits the block and does not
  descend into `Parsed`.

### Out

- Any new node types (those come with their grammar chunks).
- Using the walker in the planner — that is Phase 2.

## Acceptance

- `make parser` regenerates both `plpgsqlast` files deterministically; output
  stable across reruns.
- **Package `ast` output is unchanged** by the clone-gen edit (regenerate and
  confirm `ast_clone.go`/`ast_rewrite.go` are byte-identical to upstream).
- `go build ./...` green; `golangci-lint` clean.
- `plpgsqlast.Rewrite` visits `PLpgSQL_function` → block → body and does not
  enter `PLpgSQL_expr.Parsed`; `plpgsqlast.CloneNode` deep-copies the tree while
  sharing the `Parsed` `ast.Stmt`.

## Risks / things to verify

- **`ast` regen diff (the main risk).** Option B changes shared clone-gen
  behavior. If package `ast` has any field of a foreign-package _named_ type
  that is currently deep-cloned, its output would change and CI's
  `validate-generated-files` would fail. Mitigation: after the edit, run
  `make parser` and diff `ast_clone.go`/`ast_rewrite.go` against upstream; they
  must be unchanged. If they change, narrow the rule (e.g. limit to foreign
  _interface_ types, which is all we need for `ast.Stmt`).
- **`make parser` ordering.** `plpgsqlast` must be generated by the same
  `go generate ./go/common/parser/...` sweep; confirm the directive is picked up
  and the tool builds the package (it imports `ast`, so the dependency must be
  loadable by `packages.Load`).
- **License header / determinism.** asthelpergen writes its own header and is
  deterministic; confirm no churn from the pre-commit license hook on the
  generated files.
