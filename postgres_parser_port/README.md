# PostgreSQL Parser Port to Go - Project Documentation

This directory contains all documentation and progress tracking for porting the PostgreSQL parser from C to Go for the Multigres project.

## Quick Start for New Sessions

1. **Read the current status**: Check `progress/current_status.md` for the latest completed work
2. **Review the plan**: See `project_plan.md` for the overall roadmap
3. **Check session logs**: Look at `sessions/` for detailed session-by-session progress
4. **Next steps**: The current status doc will tell you exactly what to work on next

## Directory Structure

```
postgres_parser_port/
├── README.md                    # This file - overview and quick start
├── project_plan.md             # Master engineering plan
├── progress/
│   ├── current_status.md       # Always up-to-date status and next steps
│   ├── completed_phases.md     # Summary of completed work
│   └── technical_decisions.md  # Key architectural decisions made
├── sessions/
│   ├── session_001.md          # First session log
│   ├── session_002.md          # Second session log  
│   └── ...                     # One file per work session
└── reference/
    ├── postgres_analysis.md    # Analysis of PostgreSQL parser structure
    ├── vitess_patterns.md      # Lessons from Vitess parser implementation
    └── testing_strategy.md     # Comprehensive testing approach
```

## How to Use This System

### For New Claude Sessions
1. Always start by reading `progress/current_status.md`
2. This will tell you:
   - What was completed in the last session
   - What needs to be done next
   - Any blockers or open questions
   - Specific files to examine or create

### After Each Work Session
1. Update `progress/current_status.md` with what was accomplished
2. Create a new session log in `sessions/session_XXX.md`
3. Update `progress/completed_phases.md` if a major milestone was reached

### For Long-Term Planning
- Refer to `project_plan.md` for the overall roadmap
- Update the plan if scope or approach changes significantly
- Document major technical decisions in `progress/technical_decisions.md`

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