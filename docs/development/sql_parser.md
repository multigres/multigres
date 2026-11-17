# SQL Parser and AST

## Overview

`go/common/parser` is a Go port of PostgreSQL's SQL parser. It turns SQL text
into a tree of `ast.Node` values that mirror PostgreSQL's parse nodes, and every
node can print itself back as SQL. The multigateway parses each statement to
plan it; the multipooler and `go/common/preparedstatement` also call it.

The parser only depends on itself within `go/common` (the `parser-isolation`
rule in `.golangci.yml`), so it can be used as a standalone library. That is why
it has its own error type instead of using `mterrors`.

The PL/pgSQL parser under `go/common/parser/plpgsql` is built the same way and
is described in
[PL/pgSQL parser](../query_serving/plpgsql_parser.md).

## Where the code lives

```text
go/common/parser/
  parse.go          ParseSQL entry points and the parser pool
  lexer.go          Lexer: tokens, lookahead filtering, error recording
  context.go        ParseContext, ParseOptions (lexer state and settings)
  strings.go, unicode.go, delimited.go, comments.go, charclass.go
                    lexing of literals, identifiers and comments
  keywords.go       keyword name -> goyacc token table
  postgres.y        the grammar (port of PostgreSQL's gram.y)
  postgres.go       GENERATED from postgres.y, do not edit
  utility.go        helpers called from grammar actions
  generate.go       go:generate directives for the grammar
  goyacc/           the parser generator (a vendored goyacc)
  ast/              node types, deparse, generated clone/rewrite helpers
  datum/, pgoid/    leaf packages for Datum values and type OIDs
  plpgsql/          the PL/pgSQL parser
  testdata/         JSON test corpora
```

## Flow

```text
SQL text
   |
   v
Lexer (lexer.go)            tokens, keyword lookup, lookahead tokens
   |
   v
yyParser (postgres.go)      generated from postgres.y; actions build ast nodes
   |
   v
[]ast.Stmt                  one entry per statement
   |
   v
stmt.SqlString()            deparse back to SQL
```

`ParseSQL(input)` returns `[]ast.Stmt`. Callers that track the session's
`standard_conforming_strings` setting use
`ParseSQLWithStandardConformingStrings`, because that setting changes how string
literals are lexed. Parser instances are reused through a `sync.Pool`.

The lexer applies the same token filtering as PostgreSQL's `base_yylex`: it
turns some keywords into lookahead tokens such as `WITH_LA` and `NOT_LA`
depending on the token that follows, and converts Unicode tokens (`UIDENT`,
`USCONST`) to their plain forms.

The grammar's top rule hands the statement list to the lexer with
`SetParseTree`; `ParseSQL` reads it back, or returns the lexer's first error.

### Errors

A failed parse returns a `*parser.ParseSyntaxError`. It carries the
PostgreSQL-shaped message, an optional detail and hint, the cursor position for
the ErrorResponse `P` field, and an optional SQLSTATE override. It does not
depend on `mterrors`; the serving layer converts it with
`mterrors.NewParseErrorAt`, which uses SQLSTATE `42601` unless the error names
another one.

## The AST

Node types live in `go/common/parser/ast`, grouped into files by area
(`statements.go`, `ddl_statements.go`, `expressions.go`, and so on).
`go/common/parser/ast/README.md` tracks which PostgreSQL node types have been
ported.

Every node implements `ast.Node`:

```go
type Node interface {
    NodeTag() NodeTag
    Location() int
    SetLocation(location int)
    String() string
    SqlString() string
}
```

- Nodes embed `ast.BaseNode`, which stores the tag and source location.
- Statements also implement `ast.Stmt` (`StatementType() string`); expressions
  implement `ast.Expression`.
- `ast.NodeList` is the list type used where PostgreSQL uses `List`.
- `NodeTag` values (`T_SelectStmt`, ...) are declared in `ast/nodes.go`, next to
  the `String()` switch that names them.

### Deparse

`SqlString()` prints a node as SQL. `BaseNode.SqlString()` panics, so a node
that can appear in a parsed statement must define its own. Use the quoting
helpers in `ast/sql_utils.go` (`QuoteIdentifier`, `QuoteStringLiteral`,
`QuoteQualifiedIdentifier`, ...) instead of writing identifiers or literals raw;
the tests described below fail when an identifier is emitted without the quoting
it needs.

### Walking, rewriting and cloning

- `ast.Rewrite(node, pre, post)` walks a tree. `pre` runs before a node's
  children and `post` after; returning `false` from `pre` skips the children,
  and returning `false` from `post` stops the walk. The `Cursor` passed to each
  callback exposes `Node()`, `Parent()`, `Replace()` and `ReplaceAndRevisit()`.
  The API is in `ast/rewriter_api.go`.
- `ast.CloneNode(n)` and the per-type `CloneRefOf<Type>` functions deep-copy a
  tree.

The traversal in `ast_rewrite.go` and the copies in `ast_clone.go` are generated
by `asthelpergen` (see `go/tools/asthelpergen/README.md`). It finds every type
that implements `ast.Node`, so a new node type needs no registration, but the
generated files must be regenerated and committed.

### Keywords

Keywords are described in two tables that must list the same names:

- `go/common/parser/keywords.go` maps a keyword to its goyacc token.
- `go/common/parser/ast/keywords.go` holds the classification (unreserved,
  column name, type or function name, reserved) and whether the keyword can be a
  bare column label. It is the single source for classification; the parser
  table copies it at init.

`TestKeywordTablesInSync` fails if the tables drift apart.

## The grammar

`postgres.y` is a goyacc grammar that follows PostgreSQL's `gram.y`. Its
sections are:

1. A Go prologue (`%{ ... %}`) with imports and small helper types.
2. `%union`, listing the Go type of each semantic value (`node`, `stmt`, `list`,
   `str`, ...).
3. `%token` and `%type` declarations, and operator precedence.
4. The rules, after `%%`. Actions are Go code that build `ast` nodes.

The generator in `go/common/parser/goyacc` is run with `-f`, which enables the
fast-append optimization. When the first write to `$$` in an action is the
literal `$$ = append($1, x)`, goyacc compiles it to an in-place append on the
slice held in the value stack. That is only sound when `$1` is a non-nil slice.
If the list's base case is `$$ = nil`, the in-place write corrupts shared
runtime memory; `appendElsif` in `plpgsql/read_construct.go` documents the
failure in detail. Route such an append through a helper function (as the
PL/pgSQL grammar does with `appendElsif`, `appendException` and others) so the
generator emits an ordinary append. Lists built as `*ast.NodeList` use
`list.Append(...)` and are not affected.

## How to add a statement or grammar rule

The tablegroup DDL (`CREATE TABLEGROUP`, `ALTER TABLEGROUP`) is a compact,
complete example: nodes in `ast/sharding.go`, rules under `CreateTableGroupStmt`
in `postgres.y`, cases in `testdata/sharding_cases.json`.

1. **Define the node** in the matching file under `ast/`. Embed `BaseNode`, add
   a constructor that sets the tag, and implement `StatementType()` (for a
   statement) and `SqlString()`.
2. **Add a `NodeTag`** constant in `ast/nodes.go` and its case in the
   `String()` switch.
3. **Add keywords**, if the syntax needs new ones:
   - declare the token in `postgres.y` with `%token <keyword>`;
   - add it to the keyword-category rule it belongs to (`unreserved_keyword`,
     `col_name_keyword`, `type_func_name_keyword` or `reserved_keyword`) and to
     `bare_label_keyword` if it can be a bare label;
   - add it to both keyword tables.
4. **Write the rules** in `postgres.y`. Declare each new nonterminal with
   `%type <...>`, and add a new statement to the `stmt` rule so it is reachable.
5. **Regenerate**:

   ```bash
   make parser      # runs go generate ./go/common/parser/...
   ```

   This regenerates `postgres.go`, `plpgsql/plpgsql.go`, and the clone and
   rewrite helpers in `ast/` and `ast/plpgsqlast/`. Commit the regenerated files
   with your change. See [Code generation](code_generation.md).

6. **Check for conflicts.** goyacc writes `y.output` next to the grammar (it is
   gitignored). Its summary line must stay at zero:

   ```text
   0 shift/reduce, 0 reduce/reduce conflicts reported
   ```

7. **Add test cases** (next section) and run the parser unit tests.

## Tests

Parser tests are data-driven. A case is a JSON object:

```json
{
  "comment": "what the case shows",
  "query": "SELECT 1",
  "expected": "optional deparse output when it differs from query",
  "error": "optional substring of the expected error"
}
```

`TestParseTestSuite` in `parse_test.go` runs these checks:

| Test                                  | Corpus                                                                                                           | Checks                                                                                                    |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `TestParsing`                         | `testdata/{select,misc,ddl,dml,set,sharding}_cases.json`                                                         | The query parses and deparses to `expected` (or to `query`), and the deparsed SQL parses to the same text |
| `TestParseDeparseRoundtrip`           | The `TestParsing` files, `testdata/deparse_cases.json`, `deparse_complex_cases.json`, `testdata/postgres/*.json` | Parse, deparse and re-parse yield the same AST after canonicalization                                     |
| `TestParseIdentifierRewriteRoundtrip` | `testdata/postgres/*.json`                                                                                       | With every identifier rewritten to need quoting, the deparsed SQL still parses                            |
| `TestPostgresTestsParsing`            | `testdata/postgres/*.json`                                                                                       | Same checks as `TestParsing`, on statements converted from PostgreSQL's regression suite                  |
| `TestOne`                             | `testdata/onecase.json`                                                                                          | Scratch file for a single case during development                                                         |

When a file in `TestParsing` has failures, the suite writes the actual output to
`testdata/expected/<name>.json` (gitignored) so you can compare it with the
corpus or copy it over.

The `testdata/postgres` corpus is converted from PostgreSQL's regression SQL;
`testdata/postgres/README.md` explains how to regenerate it.

Run the unit tests with:

```text
/mt-dev unit ./go/common/parser/...
```

## Pitfalls

- **Generated files are checked in.** A change to `postgres.y` or to a node type
  without the regenerated output fails the `validate-generated-files` CI job.
- **A new node without `SqlString()` panics** the first time it is deparsed.
- **Deparse must round-trip.** The output of `SqlString()` has to parse back to
  an equivalent tree, including when identifiers need quoting.
- **Do not import other `go/common` packages** from the parser tree; the lint
  rule rejects it.
- **Use the shared OID constants** (`ast.TEXTOID`, ...) rather than literals.
  They are generated from PostgreSQL's catalog data; see
  [Code generation](code_generation.md).
