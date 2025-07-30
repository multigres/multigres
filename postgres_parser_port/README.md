# PostgreSQL Parser Port to Go - Project Documentation

This directory contains all documentation and progress tracking for porting the PostgreSQL parser from C to Go for the Multigres project.

## Quick Start for New Sessions

1. **Read the current status**: Check `status.md` for the latest project status and completed work
2. **Review the plan**: See `project_plan.md` for the overall roadmap
3. **Check current phase**: Review status.md for current phase requirements and next steps
4. **Implementation reference**: Use `postgres_ast_structs.md` for PostgreSQL AST reference

## Directory Structure

```
postgres_parser_port/
├── README.md                    # This file - overview and quick start
├── project_plan.md             # Master engineering plan
├── status.md                   # Single source of truth for project status
├── technical_decisions.md     # Key architectural decisions made
├── postgres_ast_structs.md    # PostgreSQL AST structures reference
└── (phase-1.5 completed)      # Phase 1.5 documentation archived
```

## How to Use This System

### For New Contributors
1. **Start with `status.md`** - Get overall project status and completion level
2. **Review `project_plan.md`** - Understand overall engineering plan and next phases
3. **Check `postgres_ast_structs.md`** - Reference for PostgreSQL AST structures
4. **Follow next phase guidance** in status documentation

### For Continuing Work
1. **Check `status.md`** for high-level project status and current phase
2. **Review current phase requirements** as outlined in status document
3. **Use `project_plan.md`** for overall roadmap and priorities
4. **Follow implementation guidelines** from previous phase documentation

### After Each Work Session
1. **Update `status.md`** if major milestones reached
2. **Update completion percentages** and phase status as appropriate
3. **Document decisions** in `technical_decisions.md` if needed
4. **Update project plan** if scope or timeline changes

## Project Goals

We are porting the PostgreSQL parser (from `/Users/manangupta/postgres/src/backend/parser`) to Go, following patterns established by Vitess, with these key requirements:

1. **Thread Safety**: Remove all global state, make parser context explicit
2. **Go Idioms**: Use standard Go patterns, modules, and tooling
3. **Goyacc Integration**: Use goyacc tooling for PostgreSQL grammar (like Vitess does for MySQL)
4. **Test Compatibility**: Ensure parsed AST matches PostgreSQL exactly
5. **Maintainability**: Clear structure, documentation, and build system

## Key Context for Claude Sessions

- **PostgreSQL Source**: `../postgres/src/backend/parser`
- **Vitess Reference**: `../vitess` (especially `go/vt/sqlparser/`)
- **Target Location**: All Go code goes under `multigres/go/`
- **Testing**: Must validate against PostgreSQL regression tests
- **Thread Safety**: Critical requirement - no global state allowed