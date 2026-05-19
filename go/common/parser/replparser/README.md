# replparser

Parser for PostgreSQL's replication-protocol commands (`IDENTIFY_SYSTEM`,
`CREATE_REPLICATION_SLOT`, `START_REPLICATION`, etc.), used by multigateway
when a client connects with the `replication=database` startup parameter.

Ported from PG 17.6:

- Grammar: `src/backend/replication/repl_gram.y`
- Lexer: `src/backend/replication/repl_scanner.l`
- AST nodes: `src/include/nodes/replnodes.h` (in `parser/ast`)

Scope is **logical replication only**. Physical-replication commands
(`BASE_BACKUP`, `TIMELINE_HISTORY`, `UPLOAD_MANIFEST`, physical
`START_REPLICATION`) are intentionally unparseable here.

## Why a separate package

PostgreSQL maintains the replication grammar separately from the main SQL
grammar, and we mirror that split. Two practical reasons reinforce the
separation in Go:

1. **Token-name collisions.** goyacc emits token constants (`IDENT`,
   `SCONST`, `UCONST`, `RECPTR`) at package scope. `parser/postgres.go`
   already defines `IDENT` and `SCONST` for the SQL grammar. The `-p`
   prefix flag renames parser-runtime symbols (`replYyParse`,
   `replYySymType`) but **not** token constants, so a same-package
   coexistence would not compile.
2. **Clean dispatch.** The multigateway handler calls
   `replparser.IsReplicationCommand` to peek the first token before
   committing to a parser, mirroring PG's
   `replication_scanner_is_replication_command` (`repl_scanner.l:294`).

## Files

- `grammar.y` — goyacc source.
- `grammar.go` — generated; regenerate via `make parser`.
- `lexer.go` — hand-written lexer (port of `repl_scanner.l`).
- `parse.go` — `ParseReplicationCommand`, `IsReplicationCommand` entry
  points.
