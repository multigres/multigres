# Implementation Plan: Shard Analyzers and Recovery Actions

**Date**: 2025-11-25
**Author**: Claude
**Status**: Revised - Shard-wide infrastructure first

## Overview

This plan implements the first two analyzers and their corresponding recovery actions for the MultiOrch recovery system:

1. **ShardNeedsBootstrapAnalyzer** - Detects when all nodes in a shard are empty (need fresh initialization)
2. **ShardHasNoPrimaryAnalyzer** - Detects when nodes are initialized but no primary exists

**Key Change**: This revised plan implements shard-wide analysis infrastructure FIRST (Task 1), then builds the analyzers on top of it. This avoids implementing analyzers with per-pooler logic that would need to be refactored later.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Recovery Loop (1s cycle)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  AnalysisGenerator                                                          │
│       │                                                                     │
│       ├──> GenerateAnalyses() (per-pooler)                                  │
│       └──> GenerateShardAnalyses() (shard-wide) [NEW]                       │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ Shard Analyzers (operate on complete shard state)                    │   │
│  │                                                                      │   │
│  │  ┌─────────────────────────┐    ┌─────────────────────────┐          │   │
│  │  │ ShardNeedsBootstrap     │    │ ShardHasNoPrimary       │          │   │
│  │  │ Analyzer                │    │ Analyzer                │          │   │
│  │  │                         │    │                         │          │   │
│  │  │ Input: ShardAnalysis    │    │ Input: ShardAnalysis    │          │   │
│  │  │ Detects: all nodes      │    │ Detects: nodes          │          │   │
│  │  │ uninitialized           │    │ initialized, no primary │          │   │
│  │  └───────────┬─────────────┘    └───────────┬─────────────┘          │   │
│  │              │                              │                        │   │
│  │              │ Problem{                     │ Problem{               │   │
│  │              │   Code: ShardNeedsBootstrap  │   Code: ShardHasNo     │   │
│  │              │   Action: BootstrapRecovery  │         Primary        │   │
│  │              │ }                            │   Action: AppointLeader│   │
│  │              │                              │          Recovery      │   │
│  │              │                              │ }                      │   │
│  └──────────────┼──────────────────────────────┼────────────────────────┘   │
│                 │                              │                            │
│                 ▼                              ▼                            │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ Recovery Actions (receive Problem, fetch cohort internally)          │   │
│  │                                                                      │   │
│  │  ┌─────────────────────────┐    ┌─────────────────────────┐          │   │
│  │  │ BootstrapRecoveryAction │    │ AppointLeaderRecovery   │          │   │
│  │  │                         │    │ Action                  │          │   │
│  │  │ - Receives factory deps │    │                         │          │   │
│  │  │ - Fetches cohort from   │    │ - Receives factory deps │          │   │
│  │  │   poolerStore           │    │ - Fetches cohort from   │          │   │
│  │  │ - Delegates to existing │    │   poolerStore           │          │   │
│  │  │   BootstrapShardAction  │    │ - Delegates to existing │          │   │
│  │  └─────────────────────────┘    │   AppointLeaderAction   │          │   │
│  │                                 └─────────────────────────┘          │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Design Decisions

### 1. Shard-Wide Analysis Infrastructure

Instead of per-pooler analyzers with deduplication, we implement:
- `ShardAnalysis` - aggregates all pooler states for a shard
- `ShardAnalyzer` interface - analyzes complete shard state
- `GenerateShardAnalyses()` - creates ShardAnalysis for each shard

This provides analyzers with complete shard context from the start.

### 2. Recovery Action Dependencies

Recovery actions receive `*RecoveryActionFactory` which provides:
- `poolerStore` - to fetch the cohort via `Range()` method
- `rpcClient` - for RPC calls to poolers
- `topoStore` - for topology lookups
- `coordinator` - for AppointLeader
- `logger` - for logging

### 3. Analyzer Registration

Use explicit factory-based registration:

```go
func DefaultShardAnalyzers() []ShardAnalyzer {
    if defaultShardAnalyzers == nil && analyzerFactory != nil {
        defaultShardAnalyzers = []ShardAnalyzer{
            NewShardNeedsBootstrapAnalyzer(analyzerFactory),
            NewShardHasNoPrimaryAnalyzer(analyzerFactory),
        }
    }
    return defaultShardAnalyzers
}
```

## Implementation Tasks

### Task 1: Implement Shard-Wide Analysis Infrastructure

This task creates the foundation for shard-wide analyzers.

#### 1.1: Add Problem Codes to types.go

**File**: `go/multiorch/recovery/analysis/types.go`

Add new problem code and rename existing:

```go
const (
    // Primary problems (catastrophic - block everything else).
    ProblemPrimaryDead         ProblemCode = "PrimaryDead"
    ProblemPrimaryDiskStalled  ProblemCode = "PrimaryDiskStalled"
    ProblemShardHasNoPrimary   ProblemCode = "ShardHasNoPrimary"   // RENAMED from ClusterHasNoPrimary
    ProblemShardNeedsBootstrap ProblemCode = "ShardNeedsBootstrap" // NEW
    // ... rest unchanged
)

// CheckName constants for built-in analyzers
const (
    CheckShardNeedsBootstrap CheckName = "ShardNeedsBootstrapAnalyzer"
    CheckShardHasNoPrimary   CheckName = "ShardHasNoPrimaryAnalyzer"
)
```

**Verification**: `go build ./go/multiorch/...`

#### 1.2: Create ShardAnalysis struct

**File**: `go/multiorch/recovery/analysis/shard_analysis.go` (NEW)

```go
// Copyright 2025 Supabase, Inc.
// Licensed under the Apache License, Version 2.0

package analysis

import (
    "github.com/multigres/multigres/go/multiorch/store"
    clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// ShardAnalysis aggregates analysis data for all poolers in a shard.
// This enables shard-wide analyzers to make decisions based on the
// complete state of the shard rather than individual pooler state.
type ShardAnalysis struct {
    Key     ShardKey
    Poolers []*store.ReplicationAnalysis

    // Computed summaries (populated by Compute())
    TotalPoolers         int
    ReachablePoolers     int
    UnreachablePoolers   int
    InitializedPoolers   int
    UninitializedPoolers int
    PrimaryCount         int
    ReplicaCount         int
    HasHealthyPrimary    bool
}

// NewShardAnalysis creates a ShardAnalysis from a list of pooler analyses.
func NewShardAnalysis(key ShardKey, poolers []*store.ReplicationAnalysis) *ShardAnalysis {
    sa := &ShardAnalysis{
        Key:     key,
        Poolers: poolers,
    }
    sa.Compute()
    return sa
}

// Compute calculates summary statistics from the pooler list.
func (sa *ShardAnalysis) Compute() {
    sa.TotalPoolers = len(sa.Poolers)
    sa.ReachablePoolers = 0
    sa.UnreachablePoolers = 0
    sa.InitializedPoolers = 0
    sa.UninitializedPoolers = 0
    sa.PrimaryCount = 0
    sa.ReplicaCount = 0
    sa.HasHealthyPrimary = false

    for _, p := range sa.Poolers {
        if p == nil {
            continue
        }

        if p.IsUnreachable {
            sa.UnreachablePoolers++
        } else {
            sa.ReachablePoolers++
        }

        // Initialized = has valid check and is either primary or has primary configured
        isInitialized := p.LastCheckValid && (p.IsPrimary || p.PrimaryConnInfoHost != "")
        if isInitialized {
            sa.InitializedPoolers++
        } else if !p.IsUnreachable {
            // Only count as uninitialized if reachable but not initialized
            sa.UninitializedPoolers++
        }

        if p.IsPrimary {
            sa.PrimaryCount++
            if p.LastCheckValid && !p.IsUnreachable {
                sa.HasHealthyPrimary = true
            }
        }

        if p.PoolerType == clustermetadatapb.PoolerType_REPLICA {
            sa.ReplicaCount++
        }
    }
}

// AllUninitializedOrUnreachable returns true if no pooler is initialized.
// This indicates the shard needs bootstrap.
func (sa *ShardAnalysis) AllUninitializedOrUnreachable() bool {
    return sa.InitializedPoolers == 0 && sa.ReachablePoolers > 0
}

// HasInitializedNodesButNoPrimary returns true if some nodes are initialized
// but there is no healthy primary. This indicates the shard needs leader election.
func (sa *ShardAnalysis) HasInitializedNodesButNoPrimary() bool {
    return sa.InitializedPoolers > 0 && !sa.HasHealthyPrimary
}
```

**Verification**: `go build ./go/multiorch/...`

#### 1.3: Add ShardAnalyzer interface

**File**: `go/multiorch/recovery/analysis/analyzer.go`

Add new interface after existing `Analyzer` interface:

```go
// ShardAnalyzer analyzes an entire shard and detects shard-wide problems.
// Use this interface for problems that require seeing all poolers in a shard
// (e.g., "all nodes uninitialized", "no primary in shard").
type ShardAnalyzer interface {
    // Name returns the unique name of this analyzer.
    Name() CheckName

    // AnalyzeShard examines all poolers in a shard and returns any detected problems.
    AnalyzeShard(analysis *ShardAnalysis) []Problem
}
```

**Verification**: `go build ./go/multiorch/...`

#### 1.4: Add GenerateShardAnalyses to generator

**File**: `go/multiorch/recovery/analysis/generator.go`

Add method after `GenerateAnalyses()`:

```go
// GenerateShardAnalyses returns a ShardAnalysis for each shard.
// This provides shard-wide context for analyzers that need to see all poolers in a shard.
func (g *AnalysisGenerator) GenerateShardAnalyses() []*ShardAnalysis {
    var shardAnalyses []*ShardAnalysis

    for database, tableGroups := range g.poolersByShard {
        for tableGroup, shards := range tableGroups {
            for shard, poolers := range shards {
                shardKey := ShardKey{
                    Database:   database,
                    TableGroup: tableGroup,
                    Shard:      shard,
                }

                // Convert PoolerHealth to ReplicationAnalysis for each pooler
                analyses := make([]*store.ReplicationAnalysis, 0, len(poolers))
                for _, ph := range poolers {
                    if ph == nil {
                        continue
                    }
                    // Reuse existing generateAnalysisForPooler method
                    analysis := g.generateAnalysisForPooler(ph, database, tableGroup, shard)
                    analyses = append(analyses, analysis)
                }

                shardAnalyses = append(shardAnalyses, NewShardAnalysis(shardKey, analyses))
            }
        }
    }

    return shardAnalyses
}
```

**Verification**: `go build ./go/multiorch/...`

#### 1.5: Update recovery loop to run shard analyzers

**File**: `go/multiorch/recovery/recovery_loop.go`

Update `performRecoveryCycle()` to run shard analyzers:

```go
func (re *Engine) performRecoveryCycle() {
    // Create generator - this builds the poolersByShard map once
    generator := analysis.NewAnalysisGenerator(re.poolerStore)

    var problems []analysis.Problem

    // Run per-pooler analyzers (existing)
    poolerAnalyses := generator.GenerateAnalyses()
    poolerAnalyzers := analysis.DefaultAnalyzers()
    for _, poolerAnalysis := range poolerAnalyses {
        for _, analyzer := range poolerAnalyzers {
            detectedProblems := analyzer.Analyze(poolerAnalysis)
            problems = append(problems, detectedProblems...)
        }
    }

    // Run shard-wide analyzers (NEW)
    shardAnalyses := generator.GenerateShardAnalyses()
    shardAnalyzers := analysis.DefaultShardAnalyzers()
    for _, shardAnalysis := range shardAnalyses {
        for _, analyzer := range shardAnalyzers {
            detectedProblems := analyzer.AnalyzeShard(shardAnalysis)
            problems = append(problems, detectedProblems...)
        }
    }

    if len(problems) == 0 {
        return
    }

    // ... rest unchanged ...
}
```

**Verification**: `go build ./go/multiorch/...`

---

### Task 2: Create Recovery Action Infrastructure

#### 2.1: Create RecoveryActionFactory

**File**: `go/multiorch/recovery/analysis/action_factory.go` (NEW)

```go
// Copyright 2025 Supabase, Inc.
// Licensed under the Apache License, Version 2.0

package analysis

import (
    "log/slog"

    "github.com/multigres/multigres/go/clustermetadata/topo"
    "github.com/multigres/multigres/go/multipooler/rpcclient"
    "github.com/multigres/multigres/go/multiorch/coordinator"
    "github.com/multigres/multigres/go/multiorch/store"
)

// RecoveryActionFactory creates recovery actions with proper dependencies.
type RecoveryActionFactory struct {
    poolerStore *store.Store[string, *store.PoolerHealth]
    rpcClient   rpcclient.MultiPoolerClient
    topoStore   topo.Store
    coordinator *coordinator.Coordinator
    logger      *slog.Logger
}

// NewRecoveryActionFactory creates a new factory.
func NewRecoveryActionFactory(
    poolerStore *store.Store[string, *store.PoolerHealth],
    rpcClient rpcclient.MultiPoolerClient,
    topoStore topo.Store,
    coordinator *coordinator.Coordinator,
    logger *slog.Logger,
) *RecoveryActionFactory {
    return &RecoveryActionFactory{
        poolerStore: poolerStore,
        rpcClient:   rpcClient,
        topoStore:   topoStore,
        coordinator: coordinator,
        logger:      logger,
    }
}

// NewBootstrapRecovery creates a BootstrapRecoveryAction.
func (f *RecoveryActionFactory) NewBootstrapRecovery() *BootstrapRecoveryAction {
    return NewBootstrapRecoveryAction(f.poolerStore, f.rpcClient, f.topoStore, f.logger)
}

// NewAppointLeaderRecovery creates an AppointLeaderRecoveryAction.
func (f *RecoveryActionFactory) NewAppointLeaderRecovery() *AppointLeaderRecoveryAction {
    return NewAppointLeaderRecoveryAction(f.poolerStore, f.coordinator, f.logger)
}
```

**Verification**: `go build ./go/multiorch/...`

#### 2.2: Create BootstrapRecoveryAction

**File**: `go/multiorch/recovery/analysis/bootstrap_recovery_action.go` (NEW)

```go
// Copyright 2025 Supabase, Inc.
// Licensed under the Apache License, Version 2.0

package analysis

import (
    "context"
    "log/slog"
    "time"

    "github.com/multigres/multigres/go/clustermetadata/topo"
    "github.com/multigres/multigres/go/multipooler/rpcclient"
    "github.com/multigres/multigres/go/multiorch/actions"
    "github.com/multigres/multigres/go/multiorch/store"
)

// BootstrapRecoveryAction wraps BootstrapShardAction to implement RecoveryAction interface.
type BootstrapRecoveryAction struct {
    poolerStore *store.Store[string, *store.PoolerHealth]
    action      *actions.BootstrapShardAction
    logger      *slog.Logger
}

// NewBootstrapRecoveryAction creates a new BootstrapRecoveryAction.
func NewBootstrapRecoveryAction(
    poolerStore *store.Store[string, *store.PoolerHealth],
    rpcClient rpcclient.MultiPoolerClient,
    topoStore topo.Store,
    logger *slog.Logger,
) *BootstrapRecoveryAction {
    return &BootstrapRecoveryAction{
        poolerStore: poolerStore,
        action:      actions.NewBootstrapShardAction(rpcClient, topoStore, logger),
        logger:      logger,
    }
}

// Execute performs the bootstrap recovery.
func (a *BootstrapRecoveryAction) Execute(ctx context.Context, problem Problem) error {
    // Fetch cohort from poolerStore
    cohort := a.getShardCohort(problem.Database, problem.TableGroup, problem.Shard)

    a.logger.InfoContext(ctx, "executing bootstrap recovery",
        "database", problem.Database,
        "tablegroup", problem.TableGroup,
        "shard", problem.Shard,
        "cohort_size", len(cohort),
    )

    return a.action.Execute(ctx, problem.Shard, problem.Database, cohort)
}

// Metadata returns information about this recovery action.
func (a *BootstrapRecoveryAction) Metadata() RecoveryMetadata {
    return RecoveryMetadata{
        Name:        "BootstrapShard",
        Description: "Initialize empty shard with new primary and standbys",
        Timeout:     5 * time.Minute,
        Retryable:   true,
    }
}

// RequiresLock returns true - bootstrap modifies shard state.
func (a *BootstrapRecoveryAction) RequiresLock() bool {
    return true
}

// RequiresHealthyPrimary returns false - bootstrap creates the primary.
func (a *BootstrapRecoveryAction) RequiresHealthyPrimary() bool {
    return false
}

// Priority returns the priority of this recovery action.
func (a *BootstrapRecoveryAction) Priority() Priority {
    return PriorityShardBootstrap
}

// getShardCohort retrieves all poolers belonging to the specified shard.
func (a *BootstrapRecoveryAction) getShardCohort(database, tablegroup, shard string) []*store.PoolerHealth {
    var cohort []*store.PoolerHealth

    a.poolerStore.Range(func(poolerID string, ph *store.PoolerHealth) bool {
        if ph == nil {
            return true
        }
        if ph.Database == database && ph.TableGroup == tablegroup && ph.Shard == shard {
            cohort = append(cohort, ph)
        }
        return true
    })

    return cohort
}
```

**Verification**: `go build ./go/multiorch/...`

#### 2.3: Create AppointLeaderRecoveryAction

**File**: `go/multiorch/recovery/analysis/appoint_leader_recovery_action.go` (NEW)

```go
// Copyright 2025 Supabase, Inc.
// Licensed under the Apache License, Version 2.0

package analysis

import (
    "context"
    "log/slog"
    "time"

    "github.com/multigres/multigres/go/multiorch/actions"
    "github.com/multigres/multigres/go/multiorch/coordinator"
    "github.com/multigres/multigres/go/multiorch/store"
)

// AppointLeaderRecoveryAction wraps AppointLeaderAction to implement RecoveryAction interface.
type AppointLeaderRecoveryAction struct {
    poolerStore *store.Store[string, *store.PoolerHealth]
    action      *actions.AppointLeaderAction
    logger      *slog.Logger
}

// NewAppointLeaderRecoveryAction creates a new AppointLeaderRecoveryAction.
func NewAppointLeaderRecoveryAction(
    poolerStore *store.Store[string, *store.PoolerHealth],
    coord *coordinator.Coordinator,
    logger *slog.Logger,
) *AppointLeaderRecoveryAction {
    return &AppointLeaderRecoveryAction{
        poolerStore: poolerStore,
        action:      actions.NewAppointLeaderAction(coord, logger),
        logger:      logger,
    }
}

// Execute performs the leader appointment recovery.
func (a *AppointLeaderRecoveryAction) Execute(ctx context.Context, problem Problem) error {
    // Fetch cohort from poolerStore
    cohort := a.getShardCohort(problem.Database, problem.TableGroup, problem.Shard)

    a.logger.InfoContext(ctx, "executing appoint leader recovery",
        "database", problem.Database,
        "tablegroup", problem.TableGroup,
        "shard", problem.Shard,
        "cohort_size", len(cohort),
    )

    return a.action.Execute(ctx, problem.Shard, problem.Database, cohort)
}

// Metadata returns information about this recovery action.
func (a *AppointLeaderRecoveryAction) Metadata() RecoveryMetadata {
    return RecoveryMetadata{
        Name:        "AppointLeader",
        Description: "Elect new primary for initialized shard without leader",
        Timeout:     2 * time.Minute,
        Retryable:   true,
    }
}

// RequiresLock returns true - leader election modifies shard state.
func (a *AppointLeaderRecoveryAction) RequiresLock() bool {
    return true
}

// RequiresHealthyPrimary returns false - this action creates a primary.
func (a *AppointLeaderRecoveryAction) RequiresHealthyPrimary() bool {
    return false
}

// Priority returns the priority of this recovery action.
func (a *AppointLeaderRecoveryAction) Priority() Priority {
    return PriorityEmergency
}

// getShardCohort retrieves all poolers belonging to the specified shard.
func (a *AppointLeaderRecoveryAction) getShardCohort(database, tablegroup, shard string) []*store.PoolerHealth {
    var cohort []*store.PoolerHealth

    a.poolerStore.Range(func(poolerID string, ph *store.PoolerHealth) bool {
        if ph == nil {
            return true
        }
        if ph.Database == database && ph.TableGroup == tablegroup && ph.Shard == shard {
            cohort = append(cohort, ph)
        }
        return true
    })

    return cohort
}
```

**Verification**: `go build ./go/multiorch/...`

---

### Task 3: Implement Shard Analyzers

#### 3.1: Create ShardNeedsBootstrapAnalyzer

**File**: `go/multiorch/recovery/analysis/shard_needs_bootstrap_analyzer.go` (NEW)

```go
// Copyright 2025 Supabase, Inc.
// Licensed under the Apache License, Version 2.0

package analysis

import (
    "fmt"
    "time"

    clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// ShardNeedsBootstrapAnalyzer detects when all nodes in a shard are uninitialized.
// This indicates the shard needs fresh initialization (bootstrap).
type ShardNeedsBootstrapAnalyzer struct {
    factory *RecoveryActionFactory
}

// NewShardNeedsBootstrapAnalyzer creates a new analyzer.
func NewShardNeedsBootstrapAnalyzer(factory *RecoveryActionFactory) *ShardNeedsBootstrapAnalyzer {
    return &ShardNeedsBootstrapAnalyzer{
        factory: factory,
    }
}

// Name returns the analyzer name.
func (a *ShardNeedsBootstrapAnalyzer) Name() CheckName {
    return CheckShardNeedsBootstrap
}

// AnalyzeShard checks if the shard needs bootstrap.
func (a *ShardNeedsBootstrapAnalyzer) AnalyzeShard(sa *ShardAnalysis) []Problem {
    if sa == nil || len(sa.Poolers) == 0 {
        return nil
    }

    // Check if ALL reachable nodes are uninitialized
    if !sa.AllUninitializedOrUnreachable() {
        return nil
    }

    // Get a representative pooler for the problem report
    var poolerID *clustermetadatapb.ID
    for _, p := range sa.Poolers {
        if p != nil && p.PoolerID != nil {
            poolerID = p.PoolerID
            break
        }
    }

    return []Problem{{
        Code:        ProblemShardNeedsBootstrap,
        CheckName:   CheckShardNeedsBootstrap,
        PoolerID:    poolerID,
        Database:    sa.Key.Database,
        TableGroup:  sa.Key.TableGroup,
        Shard:       sa.Key.Shard,
        Description: fmt.Sprintf("Shard %s/%s/%s needs bootstrap - all %d reachable nodes are uninitialized",
            sa.Key.Database, sa.Key.TableGroup, sa.Key.Shard, sa.ReachablePoolers),
        Priority:       PriorityShardBootstrap,
        Scope:          ScopeShard,
        DetectedAt:     time.Now(),
        RecoveryAction: a.factory.NewBootstrapRecovery(),
    }}
}
```

**Verification**: `go build ./go/multiorch/...`

#### 3.2: Create ShardHasNoPrimaryAnalyzer

**File**: `go/multiorch/recovery/analysis/shard_has_no_primary_analyzer.go` (NEW)

```go
// Copyright 2025 Supabase, Inc.
// Licensed under the Apache License, Version 2.0

package analysis

import (
    "fmt"
    "time"

    clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// ShardHasNoPrimaryAnalyzer detects when initialized nodes exist but no primary.
// This indicates the shard needs leader election (appoint leader).
type ShardHasNoPrimaryAnalyzer struct {
    factory *RecoveryActionFactory
}

// NewShardHasNoPrimaryAnalyzer creates a new analyzer.
func NewShardHasNoPrimaryAnalyzer(factory *RecoveryActionFactory) *ShardHasNoPrimaryAnalyzer {
    return &ShardHasNoPrimaryAnalyzer{
        factory: factory,
    }
}

// Name returns the analyzer name.
func (a *ShardHasNoPrimaryAnalyzer) Name() CheckName {
    return CheckShardHasNoPrimary
}

// AnalyzeShard checks if the shard has no primary despite having initialized nodes.
func (a *ShardHasNoPrimaryAnalyzer) AnalyzeShard(sa *ShardAnalysis) []Problem {
    if sa == nil || len(sa.Poolers) == 0 {
        return nil
    }

    // Check if nodes are initialized but no healthy primary
    if !sa.HasInitializedNodesButNoPrimary() {
        return nil
    }

    // Get a representative pooler for the problem report
    var poolerID *clustermetadatapb.ID
    for _, p := range sa.Poolers {
        if p != nil && p.PoolerID != nil {
            poolerID = p.PoolerID
            break
        }
    }

    return []Problem{{
        Code:        ProblemShardHasNoPrimary,
        CheckName:   CheckShardHasNoPrimary,
        PoolerID:    poolerID,
        Database:    sa.Key.Database,
        TableGroup:  sa.Key.TableGroup,
        Shard:       sa.Key.Shard,
        Description: fmt.Sprintf("Shard %s/%s/%s has no primary - %d initialized nodes but no leader",
            sa.Key.Database, sa.Key.TableGroup, sa.Key.Shard, sa.InitializedPoolers),
        Priority:       PriorityEmergency,
        Scope:          ScopeShard,
        DetectedAt:     time.Now(),
        RecoveryAction: a.factory.NewAppointLeaderRecovery(),
    }}
}
```

**Verification**: `go build ./go/multiorch/...`

#### 3.3: Register shard analyzers

**File**: `go/multiorch/recovery/analysis/analyzer.go`

Add after existing `DefaultAnalyzers()`:

```go
var (
    defaultAnalyzers      []Analyzer
    defaultShardAnalyzers []ShardAnalyzer
    analyzerFactory       *RecoveryActionFactory
)

// SetAnalyzerFactory sets the factory used to create recovery actions.
// This must be called before DefaultAnalyzers() or DefaultShardAnalyzers().
func SetAnalyzerFactory(factory *RecoveryActionFactory) {
    analyzerFactory = factory
    // Reset analyzers so they get recreated with new factory
    defaultAnalyzers = nil
    defaultShardAnalyzers = nil
}

// DefaultShardAnalyzers returns the current set of shard-wide analyzers.
func DefaultShardAnalyzers() []ShardAnalyzer {
    if defaultShardAnalyzers == nil && analyzerFactory != nil {
        defaultShardAnalyzers = []ShardAnalyzer{
            NewShardNeedsBootstrapAnalyzer(analyzerFactory),
            NewShardHasNoPrimaryAnalyzer(analyzerFactory),
        }
    }
    if defaultShardAnalyzers == nil {
        return []ShardAnalyzer{}
    }
    return defaultShardAnalyzers
}
```

**Verification**: `go build ./go/multiorch/...`

---

### Task 4: Initialize Factory in Engine

#### 4.1: Update Engine to add coordinator dependency

**File**: `go/multiorch/recovery/engine.go`

Add coordinator field and update NewEngine:

```go
// In Engine struct, add:
type Engine struct {
    // ... existing fields ...
    coordinator *coordinator.Coordinator  // NEW
}

// Update NewEngine signature:
func NewEngine(
    ts topo.Store,
    logger *slog.Logger,
    config *config.Config,
    shardWatchTargets []config.WatchTarget,
    rpcClient rpcclient.MultiPoolerClient,
    coord *coordinator.Coordinator,  // NEW
) *Engine {
    ctx, cancel := context.WithCancel(context.Background())

    poolerStore := store.NewStore[string, *store.PoolerHealth]()

    engine := &Engine{
        ts:                ts,
        logger:            logger,
        config:            config,
        rpcClient:         rpcClient,
        poolerStore:       poolerStore,
        healthCheckQueue:  NewQueue(logger, config),
        shardWatchTargets: shardWatchTargets,
        coordinator:       coord,  // NEW
        ctx:               ctx,
        cancel:            cancel,
    }

    // Initialize analyzer factory
    factory := analysis.NewRecoveryActionFactory(
        poolerStore,
        rpcClient,
        ts,
        coord,
        logger,
    )
    analysis.SetAnalyzerFactory(factory)

    // ... rest of initialization ...
    return engine
}
```

**Verification**: `go build ./go/multiorch/...`

#### 4.2: Update callers of NewEngine

Search for all callers and update:

```bash
grep -r "NewEngine" go/cmd/multiorch/ go/multiorch/recovery/*_test.go
```

Update each caller to pass coordinator. Example:

```go
// Before:
engine := recovery.NewEngine(ts, logger, config, targets, rpcClient)

// After:
coord := coordinator.New(ts, rpcClient, logger)
engine := recovery.NewEngine(ts, logger, config, targets, rpcClient, coord)
```

**Verification**: `go build ./go/cmd/multiorch/...`

---

### Task 5: Write Unit Tests

#### 5.1: Test ShardNeedsBootstrapAnalyzer

**File**: `go/multiorch/recovery/analysis/shard_needs_bootstrap_analyzer_test.go` (NEW)

```go
// Copyright 2025 Supabase, Inc.
// Licensed under the Apache License, Version 2.0

package analysis

import (
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"

    "github.com/multigres/multigres/go/multiorch/store"
    clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestShardNeedsBootstrapAnalyzer_DetectsUninitializedShard(t *testing.T) {
    analyzer := NewShardNeedsBootstrapAnalyzer(nil) // nil factory ok for detection tests

    sa := NewShardAnalysis(
        ShardKey{Database: "testdb", TableGroup: "default", Shard: "shard1"},
        []*store.ReplicationAnalysis{
            {
                PoolerID:            &clustermetadatapb.ID{Name: "pooler1"},
                Database:            "testdb",
                TableGroup:          "default",
                Shard:               "shard1",
                IsPrimary:           false,
                PrimaryConnInfoHost: "",
                LastCheckValid:      false,
                IsUnreachable:       false, // Reachable but uninitialized
            },
            {
                PoolerID:            &clustermetadatapb.ID{Name: "pooler2"},
                Database:            "testdb",
                TableGroup:          "default",
                Shard:               "shard1",
                IsPrimary:           false,
                PrimaryConnInfoHost: "",
                LastCheckValid:      false,
                IsUnreachable:       false,
            },
        },
    )

    problems := analyzer.AnalyzeShard(sa)

    require.Len(t, problems, 1)
    assert.Equal(t, ProblemShardNeedsBootstrap, problems[0].Code)
    assert.Equal(t, ScopeShard, problems[0].Scope)
    assert.Equal(t, PriorityShardBootstrap, problems[0].Priority)
}

func TestShardNeedsBootstrapAnalyzer_SkipsInitializedShard(t *testing.T) {
    analyzer := NewShardNeedsBootstrapAnalyzer(nil)

    sa := NewShardAnalysis(
        ShardKey{Database: "testdb", TableGroup: "default", Shard: "shard1"},
        []*store.ReplicationAnalysis{
            {
                PoolerID:            &clustermetadatapb.ID{Name: "pooler1"},
                Database:            "testdb",
                TableGroup:          "default",
                Shard:               "shard1",
                IsPrimary:           true, // Has a primary
                LastCheckValid:      true,
                IsUnreachable:       false,
            },
        },
    )

    problems := analyzer.AnalyzeShard(sa)
    assert.Empty(t, problems)
}
```

**Verification**: `go test ./go/multiorch/recovery/analysis/... -v -run TestShardNeedsBootstrap`

#### 5.2: Test ShardHasNoPrimaryAnalyzer

**File**: `go/multiorch/recovery/analysis/shard_has_no_primary_analyzer_test.go` (NEW)

```go
// Copyright 2025 Supabase, Inc.
// Licensed under the Apache License, Version 2.0

package analysis

import (
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"

    "github.com/multigres/multigres/go/multiorch/store"
    clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestShardHasNoPrimaryAnalyzer_DetectsInitializedShardWithoutPrimary(t *testing.T) {
    analyzer := NewShardHasNoPrimaryAnalyzer(nil)

    sa := NewShardAnalysis(
        ShardKey{Database: "testdb", TableGroup: "default", Shard: "shard1"},
        []*store.ReplicationAnalysis{
            {
                PoolerID:            &clustermetadatapb.ID{Name: "pooler1"},
                Database:            "testdb",
                TableGroup:          "default",
                Shard:               "shard1",
                PoolerType:          clustermetadatapb.PoolerType_REPLICA,
                IsPrimary:           false,
                LastCheckValid:      true,
                IsUnreachable:       false,
                PrimaryReachable:    false, // Primary is unreachable
                PrimaryConnInfoHost: "old-primary.example.com",
            },
        },
    )

    problems := analyzer.AnalyzeShard(sa)

    require.Len(t, problems, 1)
    assert.Equal(t, ProblemShardHasNoPrimary, problems[0].Code)
    assert.Equal(t, ScopeShard, problems[0].Scope)
    assert.Equal(t, PriorityEmergency, problems[0].Priority)
}

func TestShardHasNoPrimaryAnalyzer_SkipsHealthyShard(t *testing.T) {
    analyzer := NewShardHasNoPrimaryAnalyzer(nil)

    sa := NewShardAnalysis(
        ShardKey{Database: "testdb", TableGroup: "default", Shard: "shard1"},
        []*store.ReplicationAnalysis{
            {
                PoolerID:       &clustermetadatapb.ID{Name: "pooler1"},
                Database:       "testdb",
                TableGroup:     "default",
                Shard:          "shard1",
                IsPrimary:      true, // Has healthy primary
                LastCheckValid: true,
                IsUnreachable:  false,
            },
        },
    )

    problems := analyzer.AnalyzeShard(sa)
    assert.Empty(t, problems)
}
```

**Verification**: `go test ./go/multiorch/recovery/analysis/... -v -run TestShardHasNoPrimary`

---

## Summary of New Files

1. `go/multiorch/recovery/analysis/shard_analysis.go` - ShardAnalysis struct
2. `go/multiorch/recovery/analysis/action_factory.go` - Factory for recovery actions
3. `go/multiorch/recovery/analysis/bootstrap_recovery_action.go` - Bootstrap wrapper
4. `go/multiorch/recovery/analysis/appoint_leader_recovery_action.go` - AppointLeader wrapper
5. `go/multiorch/recovery/analysis/shard_needs_bootstrap_analyzer.go` - Bootstrap analyzer
6. `go/multiorch/recovery/analysis/shard_has_no_primary_analyzer.go` - NoPrimary analyzer
7. `go/multiorch/recovery/analysis/shard_needs_bootstrap_analyzer_test.go` - Unit tests
8. `go/multiorch/recovery/analysis/shard_has_no_primary_analyzer_test.go` - Unit tests

## Files to Modify

1. `go/multiorch/recovery/analysis/types.go` - Add problem codes and check names
2. `go/multiorch/recovery/analysis/analyzer.go` - Add ShardAnalyzer interface, registration
3. `go/multiorch/recovery/analysis/generator.go` - Add GenerateShardAnalyses()
4. `go/multiorch/recovery/engine.go` - Add coordinator, init factory
5. `go/multiorch/recovery/recovery_loop.go` - Run shard analyzers
6. `go/cmd/multiorch/main.go` - Pass coordinator to NewEngine
7. Test files that call NewEngine

## Verification Steps

After each subtask:
```bash
go build ./go/multiorch/...
```

After all tasks:
```bash
go test ./go/multiorch/recovery/analysis/... -v
go test ./go/multiorch/... -v
go vet ./go/multiorch/...
```
