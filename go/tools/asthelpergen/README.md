# AST Helper Generator

This tool automatically generates helper methods for Multigres AST nodes.

## Purpose

The AST Helper Generator analyzes the AST node types in `go/common/parser/ast/`
and automatically generates:

1. **Rewrite Methods** (`ast_rewrite.go`): Tree-rewriting functionality
   using the visitor pattern
   - Pre-order and post-order traversal
   - Node replacement capability
   - Cursor-based API for safe transformations

2. **Clone Methods** (`ast_clone.go`): Deep cloning functionality for all AST types
   - Nil-safe cloning
   - Recursive deep copy
   - Handles pointers, slices, and interfaces correctly

## Why Code Generation?

With 200+ AST node types in Multigres, manually writing and maintaining
Rewrite and Clone methods would be:

- Extremely time-consuming (10,000+ lines of boilerplate)
- Error-prone (easy to miss fields or handle edge cases incorrectly)
- Hard to maintain (every AST change requires updating helpers)

Code generation solves this by:

- Automatically discovering all AST types via reflection
- Generating type-safe, comprehensive helper methods
- Ensuring consistency across all node types
- Automatically handling new types when the AST evolves

## Architecture

- **Type Introspection**: `golang.org/x/tools/go/packages` to analyze Go types
- **Code Generation**: `github.com/dave/jennifer/jen` to generate clean,
  idiomatic Go code
- **Plugin Architecture**: Separate generators for rewrite, clone, and future extensions

### Components

- `asthelpergen.go`: Main orchestrator
  - Package loading and type discovery
  - Finds all types implementing the `Node` interface
  - Coordinates generator plugins

- `rewrite_gen.go`: Rewrite generator
  - Generates pre/post-order traversal methods
  - Handles node replacement logic
  - Creates public `Rewrite()` API

- `clone_gen.go`: Clone generator
  - Generates deep cloning methods
  - Handles recursive structures
  - Creates public `CloneNode()` API

- `main/main.go`: CLI entry point
  - Parses command-line flags
  - Runs generators and writes output files

## Usage

### Running the Generator

From the AST package directory:

```bash
cd go/common/parser/ast
go generate
```

This executes the `//go:generate` directive in `generate.go`, which runs:

```bash
go run ../../tools/asthelpergen/main \
  --in . \
  --iface github.com/supabase/multigres/go/common/parser/ast.Node
```

### Command-Line Flags

- `--in <package>`: Package(s) to analyze (default: current directory)
- `--iface <interface>`: Fully qualified root interface name (e.g., `github.com/supabase/multigres/go/parser/ast.Node`)
- `--clone_exclude <types>`: Comma-separated list of types to exclude from deep cloning

## Generated Files

The generator creates the following files in `go/common/parser/ast/`:

1. **ast_rewrite.go** (~5000+ lines)
   - One `rewriteRefOf<Type>()` method per AST type
   - `rewriteNode()` dispatcher with type switch
   - Internal `application` struct for traversal state

2. **ast_clone.go** (~5000+ lines)
   - `CloneNode()` main dispatcher function
   - One `CloneRefOf<Type>()` per concrete type
   - Slice and interface handling

3. **rewriter_api.go** (~200 lines)
   - Public `Rewrite()` function
   - `Cursor` type with `Node()`, `Parent()`, `Replace()` methods
   - `ApplyFunc` callback type definition

## Development

### Adding New Generators

To add a new generator (e.g., for equality checking):

1. Create `<feature>_gen.go` implementing the `generator` interface
2. Register it in `asthelpergen.go`
3. Update the CLI flags in `main/main.go` if needed

### Modifying Generated Code

If you need to change the generated output:

1. Modify the appropriate generator (`rewrite_gen.go`, `clone_gen.go`, etc.)
2. Re-run `go generate` to regenerate files
3. Test thoroughly to ensure changes work across all node types

### Testing

Test the generator by:

1. Running it on the Multigres AST: `cd go/common/parser/ast && go generate`
2. Verifying generated files compile: `go build`
3. Running AST tests: `go test ./go/common/parser/ast/...`
