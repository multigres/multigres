# Shard Bootstrap and Leader Appointment Detection

## Overview

Implement detection for two critical shard-level problems:
1. **ShardNeedsBootstrap**: All nodes in a shard are uninitialized - requires bootstrap action
2. **ShardHasNoPrimary**: Nodes are initialized but no primary exists - requires leader appointment

## Key Insight

The existing per-pooler analyzer pattern combined with `filterAndPrioritize()` deduplication already supports shard-wide problem detection. We don't need new infrastructure - just new analyzers that detect shard-wide problems from per-pooler state.

## Implementation Plan

### Task 1: Add Problem Codes and Detection Logic

**1.1 Add new problem codes to `types.go`**

Add two new problem codes at the top of the priority hierarchy:

```go
const (
    // Shard bootstrap problems (highest priority - shard cannot function)
    ProblemShardNeedsBootstrap ProblemCode = "ShardNeedsBootstrap"
    ProblemShardHasNoPrimary   ProblemCode = "ShardHasNoPrimary"

    // ... existing problems
)
```

Update check name constants:

```go
const (
    CheckShardNeedsBootstrap CheckName = "ShardNeedsBootstrap"
    CheckShardHasNoPrimary   CheckName = "ShardHasNoPrimary"
    // ... existing checks
)
```

**1.2 Add initialization detection to `PoolerHealth`**

Add a helper method to `store/pooler_info.go`:

```go
// IsInitialized returns true if the pooler has been initialized.
// A pooler is considered initialized if:
// - It's reachable (IsLastCheckValid)
// - For PRIMARY: PrimaryLSN is non-empty
// - For REPLICA: ReplicaLastReplayLSN or ReplicaLastReceiveLSN is non-empty
func (p *PoolerHealth) IsInitialized() bool {
    if !p.IsLastCheckValid {
        return false // unreachable nodes are uninitialized
    }

    if p.TopoPoolerType == clustermetadatapb.PoolerType_PRIMARY {
        return p.PrimaryLSN != ""
    }

    // For replica
    return p.ReplicaLastReplayLSN != "" || p.ReplicaLastReceiveLSN != ""
}
```

**1.3 Add initialization state to `ReplicationAnalysis`**

Update `store/replication_analysis.go`:

```go
type ReplicationAnalysis struct {
    // ... existing fields

    // Initialization state
    IsInitialized bool  // Whether this pooler has been initialized
}
```

Update `analysis/generator.go` in `generateAnalysisForPooler()`:

```go
func (g *AnalysisGenerator) generateAnalysisForPooler(...) *store.ReplicationAnalysis {
    analysis := &store.ReplicationAnalysis{
        // ... existing fields
        IsInitialized: pooler.IsInitialized(),
    }
    // ... rest of function
}
```

### Task 2: Implement Per-Pooler Analyzers

**2.1 Create `ShardNeedsBootstrapAnalyzer`**

Create `analysis/shard_needs_bootstrap_analyzer.go`:

```go
// ShardNeedsBootstrapAnalyzer detects when all nodes in a shard are uninitialized.
// This is a per-pooler analyzer that returns a shard-wide problem.
// The recovery loop's filterAndPrioritize() will deduplicate multiple instances.
type ShardNeedsBootstrapAnalyzer struct{}

func (a *ShardNeedsBootstrapAnalyzer) Name() CheckName {
    return CheckShardNeedsBootstrap
}

func (a *ShardNeedsBootstrapAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) []Problem {
    // Only analyze if this pooler is uninitialized
    if poolerAnalysis.IsInitialized {
        return nil
    }

    // If this pooler is uninitialized AND there's no primary in the shard,
    // then the whole shard likely needs bootstrap
    if poolerAnalysis.PrimaryPoolerID == nil {
        return []Problem{{
            Code:        ProblemShardNeedsBootstrap,
            CheckName:   CheckShardNeedsBootstrap,
            PoolerID:    poolerAnalysis.PoolerID,
            Database:    poolerAnalysis.Database,
            TableGroup:  poolerAnalysis.TableGroup,
            Shard:       poolerAnalysis.Shard,
            Description: fmt.Sprintf("Shard %s/%s/%s has no initialized nodes and needs bootstrap",
                poolerAnalysis.Database, poolerAnalysis.TableGroup, poolerAnalysis.Shard),
            Priority:       PriorityShardBootstrap,
            Scope:          ScopeShard,
            DetectedAt:     time.Now(),
            RecoveryAction: nil, // TODO: Create BootstrapRecoveryAction
        }}
    }

    return nil
}
```

**2.2 Create `ShardHasNoPrimaryAnalyzer`**

Create `analysis/shard_has_no_primary_analyzer.go`:

```go
// ShardHasNoPrimaryAnalyzer detects when nodes are initialized but no primary exists.
// This is a per-pooler analyzer that returns a shard-wide problem.
// The recovery loop's filterAndPrioritize() will deduplicate multiple instances.
type ShardHasNoPrimaryAnalyzer struct{}

func (a *ShardHasNoPrimaryAnalyzer) Name() CheckName {
    return CheckShardHasNoPrimary
}

func (a *ShardHasNoPrimaryAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) []Problem {
    // Only analyze replicas (primaries can't detect missing primary)
    if poolerAnalysis.IsPrimary {
        return nil
    }

    // If this replica is initialized but has no primary, the shard needs leader appointment
    if poolerAnalysis.IsInitialized && poolerAnalysis.PrimaryPoolerID == nil {
        return []Problem{{
            Code:        ProblemShardHasNoPrimary,
            CheckName:   CheckShardHasNoPrimary,
            PoolerID:    poolerAnalysis.PoolerID,
            Database:    poolerAnalysis.Database,
            TableGroup:  poolerAnalysis.TableGroup,
            Shard:       poolerAnalysis.Shard,
            Description: fmt.Sprintf("Shard %s/%s/%s has initialized nodes but no primary",
                poolerAnalysis.Database, poolerAnalysis.TableGroup, poolerAnalysis.Shard),
            Priority:       PriorityShardBootstrap,
            Scope:          ScopeShard,
            DetectedAt:     time.Now(),
            RecoveryAction: nil, // TODO: Create AppointLeaderRecoveryAction
        }}
    }

    return nil
}
```

**2.3 Register analyzers**

Update `analysis/analyzer.go` in `DefaultAnalyzers()`:

```go
func DefaultAnalyzers() []Analyzer {
    return []Analyzer{
        &ShardNeedsBootstrapAnalyzer{},
        &ShardHasNoPrimaryAnalyzer{},
        // ... existing analyzers
    }
}
```

### Task 3: Implement Recovery Actions (Placeholder)

**3.1 Create recovery action stubs**

For now, create placeholder actions that log but don't execute. Full implementation will come later.

Create `analysis/bootstrap_recovery_action.go`:

```go
type BootstrapRecoveryAction struct {
    logger *slog.Logger
}

func (a *BootstrapRecoveryAction) Execute(ctx context.Context, problem Problem) error {
    a.logger.InfoContext(ctx, "bootstrap recovery action triggered (not yet implemented)",
        "database", problem.Database,
        "tablegroup", problem.TableGroup,
        "shard", problem.Shard)
    // TODO: Implement actual bootstrap logic
    return fmt.Errorf("bootstrap action not yet implemented")
}

func (a *BootstrapRecoveryAction) RequiresHealthyPrimary() bool {
    return false // bootstrap doesn't need a primary
}

func (a *BootstrapRecoveryAction) RequiresLock() bool {
    return true // bootstrap requires exclusive shard lock
}

func (a *BootstrapRecoveryAction) Metadata() RecoveryMetadata {
    return RecoveryMetadata{
        Timeout:   60 * time.Second,
        Retryable: false, // bootstrap should not auto-retry
    }
}
```

Create `analysis/appoint_leader_recovery_action.go`:

```go
type AppointLeaderRecoveryAction struct {
    logger *slog.Logger
}

func (a *AppointLeaderRecoveryAction) Execute(ctx context.Context, problem Problem) error {
    a.logger.InfoContext(ctx, "appoint leader recovery action triggered (not yet implemented)",
        "database", problem.Database,
        "tablegroup", problem.TableGroup,
        "shard", problem.Shard)
    // TODO: Implement actual leader appointment logic
    return fmt.Errorf("appoint leader action not yet implemented")
}

func (a *AppointLeaderRecoveryAction) RequiresHealthyPrimary() bool {
    return false // leader appointment doesn't need existing primary
}

func (a *AppointLeaderRecoveryAction) RequiresLock() bool {
    return true // leader appointment requires exclusive shard lock
}

func (a *AppointLeaderRecoveryAction) Metadata() RecoveryMetadata {
    return RecoveryMetadata{
        Timeout:   30 * time.Second,
        Retryable: true, // can retry if it fails
    }
}
```

**3.2 Wire up actions to analyzers**

Update the analyzers to use the placeholder actions:

In `shard_needs_bootstrap_analyzer.go`:
```go
RecoveryAction: &BootstrapRecoveryAction{logger: slog.Default()},
```

In `shard_has_no_primary_analyzer.go`:
```go
RecoveryAction: &AppointLeaderRecoveryAction{logger: slog.Default()},
```

### Task 4: Add Tests

**4.1 Test initialization detection**

Add tests to `store/pooler_info_test.go`:

```go
func TestPoolerHealth_IsInitialized(t *testing.T) {
    tests := []struct {
        name     string
        pooler   *PoolerHealth
        expected bool
    }{
        {
            name: "unreachable primary is uninitialized",
            pooler: &PoolerHealth{
                IsLastCheckValid: false,
                TopoPoolerType:   clustermetadatapb.PoolerType_PRIMARY,
                PrimaryLSN:       "0/123ABC",
            },
            expected: false,
        },
        {
            name: "reachable primary with LSN is initialized",
            pooler: &PoolerHealth{
                IsLastCheckValid: true,
                TopoPoolerType:   clustermetadatapb.PoolerType_PRIMARY,
                PrimaryLSN:       "0/123ABC",
            },
            expected: true,
        },
        {
            name: "reachable primary without LSN is uninitialized",
            pooler: &PoolerHealth{
                IsLastCheckValid: true,
                TopoPoolerType:   clustermetadatapb.PoolerType_PRIMARY,
                PrimaryLSN:       "",
            },
            expected: false,
        },
        {
            name: "reachable replica with replay LSN is initialized",
            pooler: &PoolerHealth{
                IsLastCheckValid:      true,
                TopoPoolerType:        clustermetadatapb.PoolerType_REPLICA,
                ReplicaLastReplayLSN:  "0/123ABC",
            },
            expected: true,
        },
        {
            name: "reachable replica with receive LSN is initialized",
            pooler: &PoolerHealth{
                IsLastCheckValid:      true,
                TopoPoolerType:        clustermetadatapb.PoolerType_REPLICA,
                ReplicaLastReceiveLSN: "0/123ABC",
            },
            expected: true,
        },
        {
            name: "reachable replica without LSNs is uninitialized",
            pooler: &PoolerHealth{
                IsLastCheckValid: true,
                TopoPoolerType:   clustermetadatapb.PoolerType_REPLICA,
            },
            expected: false,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got := tt.pooler.IsInitialized()
            require.Equal(t, tt.expected, got)
        })
    }
}
```

**4.2 Test ShardNeedsBootstrapAnalyzer**

Add tests to `analysis/shard_needs_bootstrap_analyzer_test.go`:

```go
func TestShardNeedsBootstrapAnalyzer_Analyze(t *testing.T) {
    analyzer := &ShardNeedsBootstrapAnalyzer{}

    t.Run("detects uninitialized shard", func(t *testing.T) {
        analysis := &store.ReplicationAnalysis{
            PoolerID:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
            Database:        "db",
            TableGroup:      "tg",
            Shard:           "0",
            IsInitialized:   false,
            PrimaryPoolerID: nil, // no primary exists
        }

        problems := analyzer.Analyze(analysis)
        require.Len(t, problems, 1)
        require.Equal(t, ProblemShardNeedsBootstrap, problems[0].Code)
        require.Equal(t, ScopeShard, problems[0].Scope)
        require.Equal(t, PriorityShardBootstrap, problems[0].Priority)
    })

    t.Run("ignores initialized pooler", func(t *testing.T) {
        analysis := &store.ReplicationAnalysis{
            IsInitialized:   true,
            PrimaryPoolerID: nil,
        }

        problems := analyzer.Analyze(analysis)
        require.Len(t, problems, 0)
    })

    t.Run("ignores uninitialized pooler if primary exists", func(t *testing.T) {
        analysis := &store.ReplicationAnalysis{
            IsInitialized:   false,
            PrimaryPoolerID: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
        }

        problems := analyzer.Analyze(analysis)
        require.Len(t, problems, 0)
    })
}
```

**4.3 Test ShardHasNoPrimaryAnalyzer**

Add tests to `analysis/shard_has_no_primary_analyzer_test.go`:

```go
func TestShardHasNoPrimaryAnalyzer_Analyze(t *testing.T) {
    analyzer := &ShardHasNoPrimaryAnalyzer{}

    t.Run("detects initialized shard with no primary", func(t *testing.T) {
        analysis := &store.ReplicationAnalysis{
            PoolerID:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
            Database:        "db",
            TableGroup:      "tg",
            Shard:           "0",
            IsPrimary:       false,
            IsInitialized:   true,
            PrimaryPoolerID: nil, // no primary exists
        }

        problems := analyzer.Analyze(analysis)
        require.Len(t, problems, 1)
        require.Equal(t, ProblemShardHasNoPrimary, problems[0].Code)
        require.Equal(t, ScopeShard, problems[0].Scope)
        require.Equal(t, PriorityShardBootstrap, problems[0].Priority)
    })

    t.Run("ignores primary", func(t *testing.T) {
        analysis := &store.ReplicationAnalysis{
            IsPrimary:       true,
            IsInitialized:   true,
            PrimaryPoolerID: nil,
        }

        problems := analyzer.Analyze(analysis)
        require.Len(t, problems, 0)
    })

    t.Run("ignores uninitialized replica", func(t *testing.T) {
        analysis := &store.ReplicationAnalysis{
            IsPrimary:       false,
            IsInitialized:   false,
            PrimaryPoolerID: nil,
        }

        problems := analyzer.Analyze(analysis)
        require.Len(t, problems, 0)
    })

    t.Run("ignores replica with primary", func(t *testing.T) {
        analysis := &store.ReplicationAnalysis{
            IsPrimary:       false,
            IsInitialized:   true,
            PrimaryPoolerID: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
        }

        problems := analyzer.Analyze(analysis)
        require.Len(t, problems, 0)
    })
}
```

**4.4 Test deduplication**

Add integration test to `recovery/recovery_loop_test.go`:

```go
func TestRecoveryLoop_DeduplicatesShardWideProblems(t *testing.T) {
    // Setup: Create 3 uninitialized replicas in same shard
    // Each will generate a ProblemShardNeedsBootstrap
    // Verify that only 1 problem is processed (filterAndPrioritize deduplicates)

    // TODO: Implement integration test
}
```

## Testing Strategy

1. **Unit tests**: Test each analyzer in isolation with mock `ReplicationAnalysis`
2. **Unit tests**: Test `IsInitialized()` helper with various pooler states
3. **Integration tests**: Test deduplication in recovery loop with multiple poolers reporting same problem
4. **Manual testing**: Create test cluster with uninitialized nodes and verify detection

## Verification

After implementation, verify:
1. ✅ Uninitialized shard (all nodes uninitialized, no primary) → `ProblemShardNeedsBootstrap` detected
2. ✅ Initialized shard with no primary (nodes initialized, no primary) → `ProblemShardHasNoPrimary` detected
3. ✅ Multiple poolers reporting same problem → only 1 problem processed (deduplication works)
4. ✅ Recovery actions are called (even if they're placeholders for now)
5. ✅ All unit tests pass

## Future Work

This plan focuses on **detection only**. Recovery action implementation will come later:
- Bootstrap action: Call `coordinator.BootstrapShard()` with appropriate cohort
- Appoint leader action: Call `coordinator.AppointLeader()` to elect primary

## Notes

- This design leverages existing infrastructure (per-pooler analyzers + deduplication)
- No new abstraction layers needed
- Keeps the codebase simple and consistent
- Deduplication happens naturally via `filterAndPrioritize()`
- Problem detection is stateless - analyzers just read `ReplicationAnalysis` fields
