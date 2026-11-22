# Development Guides

Contributor guides for the shared libraries and developer tooling that the
service-level design docs do not cover. Each guide says where the code lives,
how the pieces fit together, and what to do when you change it.

| Guide                                     | Covers                                                                                    |
| ----------------------------------------- | ----------------------------------------------------------------------------------------- |
| [SQL parser and AST](sql_parser.md)       | `go/common/parser`: lexer, goyacc grammar, AST nodes, deparse, and how to add a statement |
| [Code generation](code_generation.md)     | Every generated file, the tool and source it comes from, and how CI checks it             |
| [Integration tests](integration_tests.md) | `go/test/endtoend` harnesses, shared clusters, the port pool, and the fake servers        |
| [Error handling](error_handling.md)       | `go/common/mterrors`: RPC codes, `PgDiagnostic`, Multigres error codes, crossing gRPC     |
| [Lint rules](lint_rules.md)               | Import boundaries, the custom `ruleguard` rules, logging rules, and how to add a rule     |

Related material that already exists elsewhere:

- [Building Multigres](../building.md) and [Contributing](../contributing.md)
  for setup and workflow.
- [PL/pgSQL parser](../query_serving/plpgsql_parser.md) for the PL/pgSQL
  grammar, which shares the generator and conventions described in
  [SQL parser and AST](sql_parser.md).
- [Testing strategy](../query_serving/testing_strategy.md) for the PostgreSQL
  compatibility suites.
