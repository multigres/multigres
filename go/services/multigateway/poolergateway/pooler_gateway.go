// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package poolergateway handles selection and communication with multipooler instances.
// It is responsible for:
// - Selecting healthy poolers for a given tablegroup via loadBalancer
// - Providing QueryService instances for query execution
//
// This is analogous to Vitess's TabletGateway component.
package poolergateway

// TODO: Add PoolerGateway integration tests that verify end-to-end query routing
// through loadBalancer to poolerConnection. Currently the selection logic is
// tested via loadBalancer unit tests.

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/buffer"
)

// A Gateway is the query processing module for each shard,
// which is used by ScatterConn.
type Gateway interface {
	// the query service that this Gateway wraps around
	queryservice.QueryService

	// QueryServiceByID returns a QueryService
	QueryServiceByID(ctx context.Context, id *clustermetadatapb.ID, target *query.Target) (queryservice.QueryService, error)
}

// errorAction classifies whether an error should trigger buffering.
type errorAction int

const (
	actionFail   errorAction = iota // Pass through error to caller
	actionBuffer                    // Buffer and retry after failover
)

// classifyError determines whether an error is eligible for failover buffering.
// Only PRIMARY traffic is buffered, and only for:
//   - MTF01: multipooler signals planned failover (SERVING_RDONLY)
//   - 25006: PostgreSQL read_only_sql_transaction (in-flight query hit a
//     primary that has already transitioned to replica)
func classifyError(err error, target *query.Target) errorAction {
	if target.PoolerType != clustermetadatapb.PoolerType_PRIMARY {
		return actionFail
	}
	if mterrors.IsErrorCode(err, mterrors.MTF01.ID, mterrors.PgSSReadOnlyTransaction) {
		return actionBuffer
	}
	return actionFail
}

// PoolerGateway selects and manages connections to multipooler instances.
type PoolerGateway struct {
	// loadBalancer manages pooler connections and selects connections for queries.
	loadBalancer *loadBalancer

	// buffer holds requests during PRIMARY failovers. nil if buffering is disabled.
	buffer *buffer.Buffer

	// cache owns the pooler discovery lifecycle (topology watch, per-pooler
	// health streams, and per-pooler *poolerConnection riders). Closing the
	// gateway shuts the cache down, which tears down everything in turn.
	cache *poolerwatch.PoolerCache[*poolerConnection]

	// logger for debugging
	logger *slog.Logger
}

// PoolerGatewayOpts groups the construction parameters for a PoolerGateway.
type PoolerGatewayOpts struct {
	// Ctx is the service-lifetime context; cancelled on shutdown. Required.
	Ctx context.Context
	// Source is the topology source for the pooler cache. Required.
	Source topoclient.Store
	// LocalCell is the cell where this gateway runs. Required.
	LocalCell string
	// Logger is used for all diagnostic logging. Required.
	Logger *slog.Logger
	// DialOpt configures transport credentials for pooler gRPC connections.
	// Required.
	DialOpt grpc.DialOption
	// Buffer is the failover buffer. When non-nil, OnPrimaryServing drains
	// it when a new primary reports SERVING. Optional.
	Buffer *buffer.Buffer
	// LowLag is the preferred replication-lag threshold for replicas.
	LowLag time.Duration
	// HighTolerance is the absolute maximum replication lag for replicas.
	HighTolerance time.Duration
}

// NewPoolerGateway constructs a PoolerGateway along with its underlying
// pooler cache and load balancer.
func NewPoolerGateway(opts PoolerGatewayOpts) *PoolerGateway {
	cache := poolerwatch.New(opts.Ctx, poolerwatch.Config[*poolerConnection]{
		Source: opts.Source,
		// LastReachedTimestamp is a no-op under the gateway's current
		// zero-grace config — entries are evicted at the NoNode moment —
		// but wiring it now means raising the grace to a non-zero value
		// later is a one-line config change. Today this returns the
		// connection's LastResponse, which is bumped only on a successful
		// health update; broadening to count error responses too is a
		// follow-up shared with orch.
		LastReachedTimestamp: func(c *poolerConnection) time.Time {
			if c == nil {
				return time.Time{}
			}
			h := c.Health()
			if h == nil {
				return time.Time{}
			}
			return h.LastResponse
		},
		Logger: opts.Logger,
	})

	var onPrimaryServing func(tableGroup, shard string)
	if opts.Buffer != nil {
		onPrimaryServing = func(tableGroup, shard string) {
			opts.Buffer.StopBuffering(&clustermetadatapb.ShardKey{
				TableGroup: tableGroup,
				Shard:      shard,
			})
		}
	}

	lb := newLoadBalancer(loadBalancerOpts{
		Ctx:              opts.Ctx,
		LocalCell:        opts.LocalCell,
		Logger:           opts.Logger,
		DialOpt:          opts.DialOpt,
		LowLag:           opts.LowLag,
		HighTolerance:    opts.HighTolerance,
		OnPrimaryServing: onPrimaryServing,
		Cache:            cache,
	})

	// Start pooler discovery. The cache owns the per-pooler *poolerConnection
	// rider: OnLive constructs the connection (and folds any topology
	// self_leadership into the LB's leaders map), OnUpdate refreshes topology
	// metadata, and OnGone closes it. ShutdownGrace/MissingGracePeriod are zero —
	// load balancing wants immediate visibility into membership changes.
	cache.Start(poolerwatch.Hooks[*poolerConnection]{
		OnLive: func(p *clustermetadatapb.MultiPooler, _ *poolerConnection) *poolerConnection {
			conn, err := newPoolerConnection(opts.Ctx, p, opts.Logger, opts.DialOpt, lb.onPoolerHealthUpdate)
			if err != nil {
				opts.Logger.ErrorContext(opts.Ctx, "failed to create pooler connection",
					"pooler_id", topoclient.ComponentIDString(p.Id), "error", err)
				return nil
			}
			lb.mergeTopologyLeader(p)
			lb.notifyIfLeaderServing(p, conn)
			return conn
		},
		OnUpdate: func(_, curr *clustermetadatapb.MultiPooler, conn *poolerConnection) {
			if conn == nil {
				return
			}
			conn.UpdatePoolerInfo(curr)
			lb.mergeTopologyLeader(curr)
			lb.notifyIfLeaderServing(curr, conn)
		},
		OnGone: func(p *clustermetadatapb.MultiPooler, conn *poolerConnection, _ poolerwatch.GoneReason) {
			if conn != nil {
				if err := conn.Shutdown(); err != nil {
					opts.Logger.ErrorContext(opts.Ctx, "error closing pooler connection",
						"pooler_id", topoclient.ComponentIDString(p.Id), "error", err)
				}
			}
			lb.onPoolerGone(p)
		},
	})

	return &PoolerGateway{
		loadBalancer: lb,
		buffer:       opts.Buffer,
		cache:        cache,
		logger:       opts.Logger,
	}
}

// isSingleQuery reports whether a request is a single autocommit query — no
// existing reserved connection and not about to create one. Such queries can be
// served on a pooler that is draining for a planned failover, so they skip the
// proactive failover-buffer block (see withBuffering). It mirrors the pooler's
// own classification (poolerserver.admissionKind); each caller computes
// willReserve from the signal it carries (reservation options, portal MaxRows).
func isSingleQuery(reservedConnID uint64, willReserve bool) bool {
	return reservedConnID == 0 && !willReserve
}

// withBuffering wraps a query execution with failover buffering. It handles:
//  1. Proactive buffering — if the shard is already known to be failing over,
//     the request waits before sending any query (avoids a wasted round-trip).
//  2. Reactive buffering on GetConnection error — if no PRIMARY is in topology.
//  3. Reactive buffering on query error — if the PRIMARY is demoted mid-query.
//
// The inner function receives the poolerConnection and executes the actual
// operation. Callers capture multi-return results via closure variables.
// On retry, the loop iterates so inner runs against a fresh connection from
// the new PRIMARY. Retries are capped at constants.MaxBufferingRetries.
//
// Callback safety on retry: inner receives a fresh poolerConnection on each
// attempt, so callers must not carry over connection-specific state between
// retries. For streaming callbacks this is safe because the two error codes
// that trigger buffering both fire before any data is streamed:
//   - MTF01: returned by StartRequest() before query execution begins
//   - 25006: returned by PostgreSQL at statement start before any output
//
// The gateway handler executes individual statements (not multi-statement
// batches), so the callback is never invoked with partial results before a
// buffer-triggering error.
func (pg *PoolerGateway) withBuffering(
	ctx context.Context,
	target *query.Target,
	singleQuery bool,
	inner func(conn *poolerConnection) error,
) error {
	bufferedOnce := false
	sk := &clustermetadatapb.ShardKey{
		TableGroup: target.TableGroup,
		Shard:      target.Shard,
	}

	var err error
	for range constants.MaxBufferingRetries + 1 {
		if pg.buffer != nil && !bufferedOnce && target.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
			var retryDone buffer.RetryDoneFunc
			var bufErr error
			if err == nil {
				// Proactive: first attempt, check if shard is already buffering.
				//
				// Single autocommit queries skip the proactive block: a pooler
				// draining for a planned failover can still serve them during the
				// first drain stage, so we send them through and only buffer
				// reactively (below) if they actually bounce with MTF01. New
				// transactions/reservations keep the proactive block — the pooler
				// rejects them throughout the drain, so a wasted round-trip is
				// pointless.
				if !singleQuery {
					retryDone, bufErr = pg.buffer.WaitIfAlreadyBuffering(ctx, sk)
				}
			} else {
				// Reactive: after a buffer-worthy error, wait for failover to end.
				retryDone, bufErr = pg.buffer.WaitForFailoverEnd(ctx, sk)
			}
			if bufErr != nil {
				return bufErr
			}
			if retryDone != nil {
				// defer is intentional here: retryDone signals the buffer's drain
				// goroutine that the retry is complete, so it must run at function
				// exit (after the operation finishes), not at loop iteration end.
				// bufferedOnce ensures we only enter this block once per call.
				defer retryDone()
				bufferedOnce = true
			}
		}

		// Get connection.
		var conn *poolerConnection
		conn, err = pg.loadBalancer.getConnection(target)
		if err != nil {
			if classifyError(err, target) == actionBuffer {
				continue
			}
			return err
		}

		pg.logger.DebugContext(ctx, "selected pooler for target",
			"tablegroup", target.TableGroup,
			"shard", target.Shard,
			"pooler_type", target.PoolerType.String(),
			"pooler_id", conn.ID())

		// Execute operation.
		err = inner(conn)
		if err != nil && classifyError(err, target) == actionBuffer {
			continue
		}
		return err
	}
	return err
}

// QueryServiceByID implements Gateway.
// It returns a QueryService for a specific pooler ID.
// This is used for reserved connections where queries must be routed to a specific
// pooler instance (e.g., for session affinity with prepared statements and portals).
func (pg *PoolerGateway) QueryServiceByID(ctx context.Context, id *clustermetadatapb.ID, target *query.Target) (queryservice.QueryService, error) {
	// Get connection by pooler ID
	conn, err := pg.loadBalancer.getConnectionByID(id)
	if err != nil {
		return nil, err
	}

	pg.logger.DebugContext(ctx, "got connection by pooler ID",
		"pooler_id", conn.ID(),
		"tablegroup", target.TableGroup,
		"shard", target.Shard)

	// Return the connection's QueryService
	return conn.QueryService(), nil
}

// StreamExecute implements queryservice.QueryService.
func (pg *PoolerGateway) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	reservationOptions *query.ReservationOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (*query.ReservedState, error) {
	var state *query.ReservedState
	// StreamExecute creates a reservation only when ReservationOptions is set.
	err := pg.withBuffering(ctx, target, isSingleQuery(options.GetReservedConnectionId(), reservationOptions != nil), func(conn *poolerConnection) error {
		var err error
		state, err = conn.QueryService().StreamExecute(ctx, target, sql, options, reservationOptions, callback)
		return err
	})
	return state, err
}

// ExecuteQuery implements queryservice.QueryService.
// This should be used sparingly only when we know the result set is small,
// otherwise StreamExecute should be used.
func (pg *PoolerGateway) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	var result *sqltypes.Result
	var state *query.ReservedState
	// ExecuteQuery cannot create a reservation.
	err := pg.withBuffering(ctx, target, isSingleQuery(options.GetReservedConnectionId(), false), func(conn *poolerConnection) error {
		var err error
		result, state, err = conn.QueryService().ExecuteQuery(ctx, target, sql, options)
		return err
	})
	return result, state, err
}

// PortalStreamExecute implements queryservice.QueryService.
func (pg *PoolerGateway) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	portalOptions *multipoolerpb.PortalExecuteOptions,
	reservationOptions *query.ReservationOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (*query.ReservedState, error) {
	var state *query.ReservedState
	// A portal reserves a connection as a suspendable cursor (MaxRows > 0) or when
	// it carries reservation reasons (e.g. a deferred BEGIN folded into the first
	// portal); only a fetch-all portal with no reasons is a single query — mirrors
	// the pooler's classification and the executor's reserve decision.
	portalReserves := options.GetMaxRows() > 0 || reservationOptions.GetReasons() != 0
	err := pg.withBuffering(ctx, target, isSingleQuery(options.GetReservedConnectionId(), portalReserves), func(conn *poolerConnection) error {
		var err error
		state, err = conn.QueryService().PortalStreamExecute(ctx, target, preparedStatement, portal, options, portalOptions, reservationOptions, callback)
		return err
	})
	return state, err
}

// Describe implements queryservice.QueryService.
func (pg *PoolerGateway) Describe(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
) (*query.StatementDescription, error) {
	var desc *query.StatementDescription
	// Describe never creates a reservation.
	err := pg.withBuffering(ctx, target, isSingleQuery(options.GetReservedConnectionId(), false), func(conn *poolerConnection) error {
		var err error
		desc, err = conn.QueryService().Describe(ctx, target, preparedStatement, portal, options)
		return err
	})
	return desc, err
}

// CopyReady implements queryservice.QueryService.
// It initiates a COPY FROM STDIN operation and returns format information.
func (pg *PoolerGateway) CopyReady(
	ctx context.Context,
	target *query.Target,
	copyQuery string,
	options *query.ExecuteOptions,
	reservationOptions *query.ReservationOptions,
) (int16, []int16, *query.ReservedState, error) {
	var (
		format     int16
		colFormats []int16
		state      *query.ReservedState
	)
	err := pg.withBuffering(ctx, target, false, func(conn *poolerConnection) error {
		var err error
		format, colFormats, state, err = conn.QueryService().CopyReady(ctx, target, copyQuery, options, reservationOptions)
		return err
	})
	return format, colFormats, state, err
}

// CopyOutReady implements queryservice.QueryService.
// It initiates a COPY ... TO STDOUT operation and returns format information
// plus any pre-CopyOutResponse notices.
func (pg *PoolerGateway) CopyOutReady(
	ctx context.Context,
	target *query.Target,
	copyQuery string,
	options *query.ExecuteOptions,
	reservationOptions *query.ReservationOptions,
) (int16, []int16, []*mterrors.PgDiagnostic, *query.ReservedState, error) {
	var (
		format     int16
		colFormats []int16
		notices    []*mterrors.PgDiagnostic
		state      *query.ReservedState
	)
	err := pg.withBuffering(ctx, target, false, func(conn *poolerConnection) error {
		var err error
		format, colFormats, notices, state, err = conn.QueryService().CopyOutReady(ctx, target, copyQuery, options, reservationOptions)
		return err
	})
	return format, colFormats, notices, state, err
}

// CopyOutStream implements queryservice.QueryService.
// Pumps CopyData / NoticeResponse frames from the multipooler back through
// the supplied callback until RESULT/ERROR.
//
// Uses loadBalancer.GetConnection directly rather than withBuffering — same
// as CopySendData / CopyFinalize for the FROM-STDIN data phase. The stream
// has already been established by CopyOutReady and lives on a specific
// poolerConnection's grpcQueryService.copyStreams map keyed by the
// reserved connection ID; routing this call through withBuffering's
// proactive failover check (which may stall, retry, and land on a
// different poolerConnection) would only surface a confusing
// "no active COPY stream for reserved connection X" error in place of
// the real failure. Failover during a live COPY stream is a connection-level
// failure that the executor's CopyOutStream handles via
// IsConnectionError → Release(ReleaseError).
func (pg *PoolerGateway) CopyOutStream(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
	onMessage func(client.CopyOutMessage) error,
) (*sqltypes.Result, *query.ReservedState, error) {
	conn, err := pg.loadBalancer.getConnection(target)
	if err != nil {
		return nil, nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	return conn.QueryService().CopyOutStream(ctx, target, options, onMessage)
}

// Close tears down the pooler cache: stops the topology watch, fires
// OnGone for every entry (which closes per-pooler connections + cancels
// per-pooler health stream goroutines), waits for everything to exit.
func (pg *PoolerGateway) Close() error {
	pg.cache.Shutdown()
	return nil
}

// PoolerCount returns the number of poolers currently tracked by the cache.
// Used by readiness checks.
func (pg *PoolerGateway) PoolerCount() int { return pg.cache.Len() }

// CellStatuses returns per-cell topology snapshot for the status page.
func (pg *PoolerGateway) CellStatuses() []poolerwatch.CellStatus {
	return pg.cache.CellStatuses()
}

// LeadershipForID returns the consensus leadership role of the connected
// pooler with the given ID, or empty string if the gateway is not
// connected to it.
func (pg *PoolerGateway) LeadershipForID(id topoclient.ComponentID) string {
	conn, ok := pg.cache.GetRider(id)
	if !ok || conn == nil {
		return ""
	}
	return pg.loadBalancer.leadershipFor(conn)
}

// Ensure PoolerGateway implements Gateway
var _ Gateway = (*PoolerGateway)(nil)

// GetAuthCredentials fetches authentication credentials from an available pooler.
// It uses withBuffering so that auth requests are buffered during planned failovers,
// just like query execution.
func (pg *PoolerGateway) GetAuthCredentials(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
	target := &query.Target{
		TableGroup: "default", // TODO: Make configurable or discover from database
		Shard:      constants.DefaultShard,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}

	var resp *multipoolerpb.GetAuthCredentialsResponse
	err := pg.withBuffering(ctx, target, false, func(conn *poolerConnection) error {
		var err error
		resp, err = conn.ServiceClient().GetAuthCredentials(ctx, req)
		// Convert gRPC error so classifyError can read the PgDiagnostic SQLSTATE for buffering.
		return mterrors.FromGRPC(err)
	})
	return resp, err
}

// GetConnection selects a poolerConnection matching the target. External
// entry point for callers that need to dispatch directly to a pooler
// (e.g. the LISTEN/NOTIFY manager). Query routing inside the gateway uses
// the load balancer through withBuffering.
func (pg *PoolerGateway) GetConnection(target *query.Target) (*poolerConnection, error) {
	return pg.loadBalancer.getConnection(target)
}

// CopySendData implements queryservice.QueryService.
// It sends a chunk of data for an active COPY operation.
func (pg *PoolerGateway) CopySendData(
	ctx context.Context,
	target *query.Target,
	data []byte,
	options *query.ExecuteOptions,
) error {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.getConnection(target)
	if err != nil {
		return err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.QueryService().CopySendData(ctx, target, data, options)
}

// CopyFinalize implements queryservice.QueryService.
// It completes a COPY operation, sending final data and returning the result.
func (pg *PoolerGateway) CopyFinalize(
	ctx context.Context,
	target *query.Target,
	finalData []byte,
	options *query.ExecuteOptions,
) (*sqltypes.Result, *query.ReservedState, error) {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.getConnection(target)
	if err != nil {
		return nil, nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.QueryService().CopyFinalize(ctx, target, finalData, options)
}

// CopyAbort implements queryservice.QueryService.
// It aborts a COPY operation.
func (pg *PoolerGateway) CopyAbort(
	ctx context.Context,
	target *query.Target,
	errorMsg string,
	options *query.ExecuteOptions,
) (*query.ReservedState, error) {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.getConnection(target)
	if err != nil {
		return nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.QueryService().CopyAbort(ctx, target, errorMsg, options)
}

// ConcludeTransaction implements queryservice.QueryService.
// It concludes a transaction with COMMIT or ROLLBACK.
func (pg *PoolerGateway) ConcludeTransaction(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
	conclusion multipoolerpb.TransactionConclusion,
	releasePortalNames []string,
	releaseAllPortals bool,
	chain bool,
) (*sqltypes.Result, *query.ReservedState, error) {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.getConnection(target)
	if err != nil {
		return nil, nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.QueryService().ConcludeTransaction(ctx, target, options, conclusion, releasePortalNames, releaseAllPortals, chain)
}

// DiscardTempTables implements queryservice.QueryService.
// It sends DISCARD TEMP on a reserved connection and removes the temp table reason.
func (pg *PoolerGateway) DiscardTempTables(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
) (*sqltypes.Result, *query.ReservedState, error) {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.getConnection(target)
	if err != nil {
		return nil, nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	// Delegate to the pooler's QueryService
	return conn.QueryService().DiscardTempTables(ctx, target, options)
}

// ReleaseReservedConnection implements queryservice.QueryService.
// It forcefully releases a reserved connection regardless of reason.
func (pg *PoolerGateway) ReleaseReservedConnection(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
) error {
	// Get a connection matching the target
	conn, err := pg.loadBalancer.getConnection(target)
	if err != nil {
		return err
	}

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	return conn.QueryService().ReleaseReservedConnection(ctx, target, options)
}

// StreamReplication implements queryservice.QueryService.
//
// All replication routes to the shard's PRIMARY (consensus leader) in v1, so
// the routing target is the init's target with PoolerType forced to PRIMARY.
// The target proto is cloned rather than mutated in place — init belongs to the
// caller and may be reused.
//
// Routes via loadBalancer.getConnection directly, NOT withBuffering: a
// replication stream is pinned to one pooler for its lifetime, so there is
// nothing to retry onto. A failover mid-stream is a connection-level failure
// the caller handles by tearing the tunnel down, exactly as CopyOutStream
// treats a live COPY stream.
func (pg *PoolerGateway) StreamReplication(
	ctx context.Context,
	init *multipoolerpb.StreamReplicationInit,
) (multipoolerpb.MultiPoolerService_StreamReplicationClient, error) {
	target, _ := proto.Clone(init.GetTarget()).(*query.Target)
	if target == nil {
		target = &query.Target{}
	}
	target.PoolerType = clustermetadatapb.PoolerType_PRIMARY

	conn, err := pg.loadBalancer.getConnection(target)
	if err != nil {
		return nil, err
	}

	pg.logger.DebugContext(ctx, "selected pooler for replication target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", conn.ID())

	return conn.QueryService().StreamReplication(ctx, init)
}
