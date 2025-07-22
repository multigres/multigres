# PostgreSQL Parser Port to Go - Project Documentation

This directory contains all documentation and progress tracking for porting the PostgreSQL parser from C to Go for the Multigres project.

## Quick Start for New Sessions

1. **Read the current status**: Check `status.md` for the latest project status and completed work
2. **Review the plan**: See `project_plan.md` for the overall roadmap
3. **Check current phase**: Look at `phase-1.5/` for detailed current phase documentation
4. **Next steps**: The status doc and phase documentation will tell you exactly what to work on next

## Directory Structure

```
postgres_parser_port/
├── README.md                    # This file - overview and quick start
├── project_plan.md             # Master engineering plan
├── status.md                   # Single source of truth for project status
├── technical_decisions.md     # Key architectural decisions made
└── phase-1.5/                 # Current phase documentation
    ├── README.md               # Phase 1.5 overview and goals
    ├── implementation_plan.md  # Detailed roadmap for AST completion
    ├── missing_nodes.md        # Complete inventory of 185+ missing nodes
    └── progress_tracking.md    # Session-by-session progress within phase
```

## How to Use This System

### For New Contributors
1. **Start with `status.md`** - Get overall project status and completion level
2. **Read `phase-1.5/README.md`** - Understand current phase goals and strategy
3. **Review `phase-1.5/missing_nodes.md`** - See what specific nodes need implementation
4. **Check `phase-1.5/progress_tracking.md`** - See recent progress and next planned work

### For Continuing Work
1. **Check `status.md`** for high-level project status
2. **Review `phase-1.5/progress_tracking.md`** for latest session progress
3. **Use `phase-1.5/implementation_plan.md`** for detailed implementation roadmap
4. **Follow implementation guidelines** in phase documentation

### After Each Work Session
1. **Update `status.md`** if major milestones reached
2. **Update `phase-1.5/progress_tracking.md`** with session accomplishments
3. **Mark completed nodes** in `phase-1.5/missing_nodes.md`
4. **Document decisions** in `technical_decisions.md` if needed

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