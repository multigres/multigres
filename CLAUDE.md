# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multigres is Vitess for PostgreSQL. It provides horizontal scaling, connection pooling, and cluster orchestration for PostgreSQL deployments.

## Getting Started

### Build Commands

```bash
make tools      # Install build dependencies (protoc, goyacc, etc.)
make build      # Build Go binaries to bin/
make proto      # Generate protobuf files
make parser     # Generate PostgreSQL parser from grammar
make build-all  # Proto + parser + binaries
make test       # Run all tests
make test-short # Run short tests only (skips PostgreSQL integration tests)
make test-race  # Run tests with race detector
make clean      # Remove build artifacts
```

### Running Tests

**IMPORTANT**: Always use the `/mt-dev` skill for running tests. Do NOT run `go test` commands directly.

**Unit tests** (fast, isolated):
```bash
/mt-dev unit all                          # All unit tests
/mt-dev unit ./go/multipooler/...         # Specific package
/mt-dev unit ./go/multipooler TestConnPool # Specific test
```

**Integration tests** (end-to-end, slower):
```bash
/mt-dev integration all                    # All integration tests
/mt-dev integration multipooler            # Specific package
/mt-dev integration multiorch TestFailover # Specific test
```

**Common flags**:
- `-v` for verbose output
- `-race` for race detection
- `-count=10` to check for flaky tests
- `-cover` for coverage reports

**When developing**:
1. After writing code, run relevant unit tests first (fast feedback)
2. Then run integration tests (comprehensive validation)
3. Use the skill for all test execution - it handles building and provides better output

### Development Workflow

- Run `make proto` after modifying `.proto` files
- Use `/mt-dev` skill for all test execution (unit and integration)
- Unit tests run directly; integration tests automatically run `make build` first

## Architecture

Client connections flow through multigateway → multipooler → PostgreSQL. The multiorch service handles failover and consensus. See the architecture reference doc for details on services, packages, and data flow.

## Engineering Principles

This is mission-critical infrastructure. Prioritize reliability, security, and maintainability.

### Performance

- The query path (multigateway → multipooler → postgres) is latency-sensitive

### Security

- Validate all external input at system boundaries
- Never log credentials, tokens, or sensitive query data
- **Supply chain security**:
  - Pin GitHub Actions to exact commit SHAs, not version tags
  - Verify SHA256 checksums for downloaded tools (see `tools/tool_checksums.sh`)
  - Use minimal permissions in CI workflows

## Commits and PRs

Follow [Conventional Commits](https://www.conventionalcommits.org/) for commit messages and PR titles:

```text
<type>(<scope>): <description>

feat(multigateway): add connection timeout configuration
fix(multipooler): handle nil pointer in health check
docs(readme): update installation instructions
test(pgctld): add integration tests for failover
refactor(parser): simplify AST node handling
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`, `build`, `ci`, `perf`

Do NOT add these lines to commit messages:

- `Generated with [Claude Code](https://claude.com/claude-code)`
- `Co-Authored-By: Claude <noreply@anthropic.com>`

## Code Patterns

### Formatting and Linting

- Formatting: gofumpt + goimports (local prefix: `github.com/multigres`)
- Linting: golangci-lint
- Protobufs follow [Google Cloud API Design Guide](https://cloud.google.com/apis/design/)

## Reference Documentation

@.claude/docs/architecture.md

## Personal Preferences

@~/.claude/multigres.md
