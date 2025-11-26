# Architecture

## Core Services (go/cmd/)

- **multigateway** - PostgreSQL proxy accepting client connections, routes queries
- **multipooler** - Connection pooling service, communicates with pgctld
- **pgctld** - PostgreSQL interface daemon, connects directly to PostgreSQL instances
- **multiorch** - Cluster orchestration for consensus and failover
- **multiadmin** - Administrative service for cluster management
- **multigres** - CLI tool for cluster management

## Data Flow

1. Client → **multigateway** (accepts PostgreSQL connections)
2. **multigateway** → **multipooler** (query routing and pooling)
3. **multipooler** → **pgctld** (database interface)
4. **pgctld** → PostgreSQL (actual database)
5. **multiorch** handles failover and consensus across cells

## Topology

The system uses etcd for service discovery and topology storage. The topology is organized by cells (zones), with each cell having its own set of services.

## Directory Structure and Dependencies

```
./go/cmd/...      # Commands - can depend on anything
./go/services/... # Service code - cannot depend on cmd/ or other services
./go/common/...   # Shared code - cannot depend on cmd/ or services/
./go/tools/...    # Generic utilities - cannot depend on any repo code outside tools/
```

- **go/tools/**: Generic helpers (timers, retry, etc.) that aren't multigres-specific
- **go/common/**: Shared multigres code (error codes, gRPC clients, protocol code, etc.)

## Generated Files

Files with `// Code generated` comments should not be edited directly. Regenerate with `make proto` (protobufs) or `make parser` (SQL parser/AST). When debugging, trace to source files instead of analyzing generated code.
