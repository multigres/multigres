# Serving State Management

Design doc for the multipooler serving state management system introduced in MUL-61.

## Problem

The multipooler had no unified serving state management:

- `MultiPoolerManager.queryServingState` duplicated `multipooler.ServingStatus` and was manually kept in sync
- `QueryPoolerServer.SetServingType()` stored state but didn't enforce behavior
- `setServingReadOnly()` manually updated state, synced topology, and stopped heartbeat — but never called `qsc.SetServingType()`, leaving the query service out of sync
- `changeTypeLocked()` managed heartbeat directly, duplicating logic
- `SERVING_RDONLY` existed as a proto enum value but was semantically redundant with `(REPLICA, SERVING)`

## Decisions

### Decision 1: Remove `SERVING_RDONLY`

**Chosen: Remove it.**

Vitess uses only 3 internal serving states and determines read-only vs read-write behavior from tablet type (PRIMARY vs REPLICA), not serving state. We follow the same pattern:

| Combination | Behavior |
|---|---|
| `(PRIMARY, SERVING)` | Accept reads + writes |
| `(REPLICA, SERVING)` | Accept reads only |
| `(DRAINED, *)` | Offline, no user queries |
| `(*, NOT_SERVING)` | Reject all queries |

The demotion flow now goes through `NOT_SERVING` instead of `SERVING_RDONLY`:

```
Before:  (PRIMARY, SERVING) -> (PRIMARY, SERVING_RDONLY) -> (REPLICA, SERVING)
After:   (PRIMARY, SERVING) -> (PRIMARY, NOT_SERVING)    -> (REPLICA, SERVING)
```

Brief query blackout during type transition (same as Vitess).

**Alternative considered: Keep `SERVING_RDONLY`.** A PRIMARY in `SERVING_RDONLY` would accept reads but reject writes, giving gateways time to redirect write traffic. This avoids blackout but adds a state that's semantically redundant with `(REPLICA, SERVING)`, making the state model more complex. We chose simplicity — the blackout is brief and matches Vitess behavior.

### Decision 2: State ownership

**Chosen: Current state on the `multipooler` record, no separate target state.**

The `MultiPooler` proto record (`multipooler.Type` and `multipooler.ServingStatus`) is the single source of truth. The `StateManager` doesn't store target state — `SetState()` receives the desired type and status as arguments, converges components, and updates the record atomically. Since only one transition runs at a time (callers hold `ssm.mu`), there's no need to track a separate target.

```go
type StateManager struct {
    multipooler  *clustermetadatapb.MultiPooler  // current state lives here
    components   []StateAware                     // registered components
}
```

This replaces the old `queryServingState` field which was a third copy of the serving status.

**Alternative considered: Separate target state fields.** `StateManager` would store `targetType`/`targetStatus` to allow a future background retry loop to re-attempt convergence toward the last-set target. We deferred this — target state can be added when async retry is needed. For now one transition at a time is sufficient.

**Alternative considered: Fully separate state tracking.** `StateManager` would track both target and current state independently from the `multipooler` record. More explicit separation of "local state" vs "topology state", but introduces duplication. We chose to avoid it since the `multipooler` record is already authoritative.

### Decision 3: Component notification model

**Chosen: Interface-based registration with parallel convergence.**

Components implement the `StateAware` interface directly and are registered with the manager. On state change, all components are notified in parallel via `errgroup`. The multipooler record is only updated after all components converge successfully.

```go
type StateAware interface {
    OnStateChange(ctx context.Context, poolerType PoolerType, servingStatus PoolerServingStatus) error
}
```

`QueryPoolerServer` and `ReplTracker` implement `OnStateChange` directly on the type — no adapter wrappers needed. Go's structural typing means they satisfy `StateAware` without importing the `manager` package. `OnStateChange` is also added to the `PoolerController` interface so `pm.qsc` (typed as `PoolerController`) satisfies `StateAware`.

**Why parallel**: Components are independent — the query service and heartbeat tracker don't depend on each other's transitions. Parallel execution reduces latency during state changes. If ordering ever matters for a future component, it can be handled by that component internally (checking another component's state before proceeding).

**Alternative considered: Direct calls.** Manager holds concrete references and calls each component's methods directly. Simpler, no interface indirection. But adding a component means modifying convergence code, and sequential execution adds unnecessary latency.

**Alternative considered: Adapter wrappers.** Thin adapter structs (`queryServiceAdapter`, `heartbeatAdapter`) that bridge existing component APIs to `StateAware`. This avoids modifying the component types but adds indirection — the components can just implement the interface directly.

**Alternative considered: Broadcast channel.** Components listen on a shared channel. Fully decoupled and async, but complex error handling (who owns errors?), channel lifecycle management, and harder to know when convergence is complete.

### Decision 4: Convergence model

**Chosen: Synchronous with future async retry.**

`SetState()` fans out `OnStateChange` to all components in parallel and waits. Errors are returned to the caller. A background retry loop can be added later for transient failures.

```go
func (ssm *StateManager) SetState(ctx, poolerType, servingStatus) error {
    g, ctx := errgroup.WithContext(ctx)
    for _, c := range ssm.components {
        g.Go(func() error {
            return c.OnStateChange(ctx, poolerType, servingStatus)
        })
    }
    if err := g.Wait(); err != nil {
        return err
    }
    ssm.multipooler.Type = poolerType
    ssm.multipooler.ServingStatus = servingStatus
    return nil
}
```

**Alternative considered: Background convergence goroutine.** A dedicated goroutine watches for target changes and continuously converges. `SetState()` would be non-blocking. Pros: natural retry, coalesces rapid changes. Cons: more complex lifecycle, harder to report errors, caller can't know when transition completes without additional synchronization. We chose synchronous for now — it matches how the code was already structured and the callers expect errors back.

## What changed

### Proto

- Removed `SERVING_RDONLY = 5` from `PoolerServingStatus`

### `PoolerController` interface

`SetServingType` was replaced by `OnStateChange` which takes both `PoolerType` and `PoolerServingStatus`:

```go
// Before
SetServingType(ctx context.Context, servingStatus PoolerServingStatus) error

// After
OnStateChange(ctx context.Context, poolerType PoolerType, servingStatus PoolerServingStatus) error
```

### `QueryPoolerServer`

- Added `poolerType` field
- `IsServing()` now only checks `servingStatus == SERVING` (no more `SERVING_RDONLY`)
- `OnStateChange` stores both type and status

### `ReplTracker`

- Added `OnStateChange` method: starts heartbeat for `(PRIMARY, SERVING)`, stops otherwise
- Delegates to unexported `makePrimary()`/`makeNonPrimary()`

### `StateManager` (new)

Coordinates state transitions via the `StateAware` interface:
- Components implement `OnStateChange` directly and are registered via constructor or `Register()`
- `SetState()` fans out to all components in parallel via `errgroup`
- Multipooler record updated only after all components succeed

### Manager

- Replaced `queryServingState` field with `servingState *StateManager`
- Creates `StateManager` with `pm.qsc` at init
- Registers `pm.replTracker` when heartbeat starts
- `setServingReadOnly()` replaced by `setNotServing()` which delegates to `servingState.SetState()`
- `changeTypeLocked()` delegates to `servingState.SetState()` instead of managing heartbeat directly
- `demotionState.isServingReadOnly` renamed to `isNotServing`

### Demotion flow (after)

```
EmergencyDemote:

1. pm.setNotServing(ctx, state)
   -> servingState.SetState(ctx, PRIMARY, NOT_SERVING)
      -> [parallel] qsc.OnStateChange(ctx, PRIMARY, NOT_SERVING)        // rejects all queries
      -> [parallel] replTracker.OnStateChange(ctx, PRIMARY, NOT_SERVING) // stops heartbeat
      -> multipooler record updated
   -> topo synced

2. [drain connections, stop postgres — outside state manager]

3. changeTypeLocked(ctx, REPLICA)
   -> servingState.SetState(ctx, REPLICA, SERVING)
      -> [parallel] qsc.OnStateChange(ctx, REPLICA, SERVING)        // accepts reads
      -> [parallel] replTracker.OnStateChange(ctx, REPLICA, SERVING) // stays non-primary
      -> multipooler record updated
   -> topo synced
```

## Files changed

| File | Change |
|---|---|
| `proto/clustermetadata.proto` | Removed `SERVING_RDONLY` |
| `go/services/multipooler/manager/serving_state.go` | New: `StateManager`, `StateAware` interface |
| `go/services/multipooler/manager/serving_state_test.go` | New: 9 unit tests |
| `go/services/multipooler/manager/manager.go` | Replaced `queryServingState`, rewrote demotion helpers |
| `go/services/multipooler/manager/rpc_manager.go` | Updated `changeTypeLocked` and emergency demotion |
| `go/services/multipooler/poolerserver/controller.go` | Added `PoolerType` to `SetServingType`, added `OnStateChange` |
| `go/services/multipooler/poolerserver/pooler.go` | Added `poolerType` field, `OnStateChange`, simplified `IsServing` |
| `go/services/multipooler/heartbeat/repltracker.go` | Added `OnStateChange` |
| `go/services/multipooler/manager/pg_multischema_test.go` | Updated mock signature |

## Future work

- **StateManager owns `multipooler` record**: The manager currently shares the `multipooler` pointer. The state manager could own it more explicitly.
- **Query enforcement**: `QueryPoolerServer` stores `poolerType` but doesn't yet enforce read-only for replicas. The TODO remains for when transaction/query engines are added.
- **Async retry**: Add a background retry loop for transient convergence failures (similar to Vitess's `retryTransition`).
- **Health check integration**: Future health check component would implement `StateAware` and check `qsc.IsServing()` before reporting healthy.
- **Topology sync as component**: Currently topology sync happens after `SetState()` in the caller. Could be a `StateAware` component.
