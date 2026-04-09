// Copyright 2026 Supabase, Inc.
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

package manager

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/executor"
)

// errRuleConflict is returned by updateRule when a withPreviousRule compare-and-swap
// check fails: the current rule's term/subterm did not match the expected values.
var errRuleConflict = errors.New("rule conflict: current rule version mismatch")

// ruleHistoryRecord represents a row from multigres.rule_history or multigres.current_rule.
type ruleHistoryRecord struct {
	CoordinatorTerm         int64
	RuleSubterm             int64
	EventType               string
	LeaderID                *poolerID // nil if not set
	CoordinatorID           *string   // informational only; component type is not stored
	WALPosition             *string
	Operation               *string
	Reason                  string
	CohortMembers           []poolerID
	AcceptedMembers         []poolerID
	DurabilityPolicyName    *string
	DurabilityQuorumType    *string
	DurabilityRequiredCount *int32
	DurabilityAsyncFallback *string
	CreatedAt               time.Time
}

// ruleNumber identifies a specific rule version by coordinator term and subterm.
type ruleNumber struct {
	coordinatorTerm int64
	ruleSubterm     int64
}

// ruleUpdateBuilder constructs the parameters for updateRule.
// coordinatorID, eventType, reason, and createdAt are always required.
// Fields not set via builder methods retain their current value in current_rule.
type ruleUpdateBuilder struct {
	// required
	termNumber    int64
	coordinatorID *clustermetadatapb.ID
	eventType     string
	reason        string
	createdAt     time.Time

	// optional; nil means keep the existing value in current_rule
	leaderID      *clustermetadatapb.ID
	cohortMembers []*clustermetadatapb.ID

	// history-only optional fields
	walPosition     string
	operation       string
	acceptedMembers []*clustermetadatapb.ID

	force        bool
	previousRule *ruleNumber // for compare-and-swap; nil means no check
}

func newRuleUpdate(termNumber int64, coordinatorID *clustermetadatapb.ID, eventType, reason string, createdAt time.Time) *ruleUpdateBuilder {
	return &ruleUpdateBuilder{
		termNumber:    termNumber,
		coordinatorID: coordinatorID,
		eventType:     eventType,
		reason:        reason,
		createdAt:     createdAt,
	}
}

func (b *ruleUpdateBuilder) withLeader(id *clustermetadatapb.ID) *ruleUpdateBuilder {
	b.leaderID = id
	return b
}

func (b *ruleUpdateBuilder) withCohort(members []*clustermetadatapb.ID) *ruleUpdateBuilder {
	b.cohortMembers = members
	return b
}

func (b *ruleUpdateBuilder) withWALPosition(pos string) *ruleUpdateBuilder {
	b.walPosition = pos
	return b
}

func (b *ruleUpdateBuilder) withOperation(op string) *ruleUpdateBuilder {
	b.operation = op
	return b
}

func (b *ruleUpdateBuilder) withAcceptedMembers(members []*clustermetadatapb.ID) *ruleUpdateBuilder {
	b.acceptedMembers = members
	return b
}

func (b *ruleUpdateBuilder) withForce() *ruleUpdateBuilder {
	b.force = true
	return b
}

// withPreviousRule adds a compare-and-swap check: the update only proceeds if the
// current rule matches the given coordinator term and subterm.
//
//nolint:unused // CAS support is wired into the SQL query; callers will be added soon.
func (b *ruleUpdateBuilder) withPreviousRule(coordinatorTerm, ruleSubterm int64) *ruleUpdateBuilder {
	b.previousRule = &ruleNumber{coordinatorTerm: coordinatorTerm, ruleSubterm: ruleSubterm}
	return b
}

// parsePoolerIDStrings converts a slice of "cell_name" app name strings into poolerIDs.
// Returns nil for nil input, preserving the distinction between "not set" and "empty".
func parsePoolerIDStrings(names []string) ([]poolerID, error) {
	if names == nil {
		return nil, nil
	}
	result := make([]poolerID, 0, len(names))
	for _, s := range names {
		id, err := parseApplicationName(s)
		if err != nil {
			return nil, err
		}
		result = append(result, poolerID{id: id, appName: s})
	}
	return result, nil
}

// scanRuleHistoryRow scans string-typed DB columns into a ruleHistoryRecord,
// parsing leader_id, cohort_members, and accepted_members into poolerIDs.
// leaderIDStr, cohortNames, and acceptedNames are intermediary scan targets.
func scanRuleHistoryRow(rec *ruleHistoryRecord, leaderIDStr *string, cohortNames, acceptedNames []string) error {
	if leaderIDStr != nil {
		id, err := parseApplicationName(*leaderIDStr)
		if err != nil {
			return mterrors.Wrapf(err, "failed to parse leader_id %q", *leaderIDStr)
		}
		p := poolerID{id: id, appName: *leaderIDStr}
		rec.LeaderID = &p
	}
	cohort, err := parsePoolerIDStrings(cohortNames)
	if err != nil {
		return mterrors.Wrap(err, "failed to parse cohort_members")
	}
	rec.CohortMembers = cohort

	accepted, err := parsePoolerIDStrings(acceptedNames)
	if err != nil {
		return mterrors.Wrap(err, "failed to parse accepted_members")
	}
	rec.AcceptedMembers = accepted
	return nil
}

// ruleStorer is the interface for reading and writing the current shard rule.
// *ruleStore implements this; tests use fakeRuleStore.
type ruleStorer interface {
	observePosition(ctx context.Context) (*clustermetadatapb.NodePosition, error)
	updateRule(ctx context.Context, update *ruleUpdateBuilder) (*clustermetadatapb.NodePosition, error)
}

// ruleStore manages the current shard rule in postgres.
//
// All DB operations that write or read the current rule go through ruleStore,
// ensuring consistent access to rule state.
type ruleStore struct {
	logger       *slog.Logger
	queryService executor.InternalQueryService
}

// newRuleStore creates a ruleStore.
func newRuleStore(
	logger *slog.Logger,
	qs executor.InternalQueryService,
) *ruleStore {
	return &ruleStore{
		logger:       logger,
		queryService: qs,
	}
}

// ----------------------------------------------------------------------------
// Schema Operations
// ----------------------------------------------------------------------------

// createCurrentRuleTable creates the current_rule table.
// This table holds a single row per shard representing the current cluster rule.
// It is used as a locking target (SELECT FOR UPDATE) and a fast read path for
// current state, while rule_history provides the append-only audit log.
//
// Non-essential audit fields (event_type, wal_position, accepted_members, reason,
// operation) are stored only in rule_history to keep this table focused on
// operational state.
//
// created_at records when this particular rule was written, matching the
// corresponding rule_history.created_at for the same (coordinator_term, rule_subterm).
func (pm *MultiPoolerManager) createCurrentRuleTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.exec(execCtx, `CREATE TABLE IF NOT EXISTS multigres.current_rule (
		shard_id                  BYTEA PRIMARY KEY,
		coordinator_term          BIGINT NOT NULL,
		rule_subterm              BIGINT NOT NULL,
		leader_id                 TEXT,
		coordinator_id            TEXT,
		cohort_members            TEXT[] NOT NULL,
		durability_policy_name    TEXT,
		durability_quorum_type    TEXT,
		durability_required_count INT,
		durability_async_fallback TEXT,
		created_at                TIMESTAMPTZ NOT NULL
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create current_rule table")
	}
	return nil
}

// initializeCurrentRule inserts the zero-state sentinel row for the default shard.
// This row must exist before any rule is written so that updateRule can SELECT FOR
// UPDATE on it to serialise concurrent writes.
// coordinator_term=0 means no rule has been applied yet.
// Uses ON CONFLICT DO NOTHING so it is safe to call multiple times.
func (pm *MultiPoolerManager) initializeCurrentRule(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	err := pm.execArgs(execCtx, `
		INSERT INTO multigres.current_rule
		  (shard_id, coordinator_term, rule_subterm, cohort_members, created_at)
		VALUES ($1, 0, 0, '{}', now())
		ON CONFLICT (shard_id) DO NOTHING`,
		[]byte("0"))
	if err != nil {
		return mterrors.Wrap(err, "failed to initialize current_rule")
	}
	return nil
}

// createRuleHistoryTable creates the rule_history table.
// Each row records a cluster state change (promotion, cohort membership, durability policy).
// The composite primary key (coordinator_term, rule_subterm) uniquely identifies each rule;
// rule_subterm is assigned by the application as MAX(rule_subterm)+1 within a coordinator_term.
func (pm *MultiPoolerManager) createRuleHistoryTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.exec(execCtx, `CREATE TABLE IF NOT EXISTS multigres.rule_history (
		coordinator_term          BIGINT NOT NULL,
		rule_subterm              BIGINT NOT NULL,
		event_type                TEXT NOT NULL,
		leader_id                 TEXT,
		coordinator_id            TEXT,
		wal_position              TEXT,
		accepted_members          TEXT[],
		reason                    TEXT NOT NULL,
		cohort_members            TEXT[] NOT NULL,
		durability_policy_name    TEXT,
		durability_quorum_type    TEXT,
		durability_required_count INT,
		durability_async_fallback TEXT,
		operation                 TEXT,
		created_at                TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (coordinator_term, rule_subterm)
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create rule_history table")
	}

	return nil
}

// ----------------------------------------------------------------------------
// Read/Write Operations
// ----------------------------------------------------------------------------

// observePosition reads the current rule and WAL LSN from postgres and returns
// the observed position. Returns nil if no rule has been applied yet
// (coordinator_term = 0).
//
// Returns an error if postgres is unreachable.
func (rs *ruleStore) observePosition(ctx context.Context) (*clustermetadatapb.NodePosition, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	result, err := rs.queryService.QueryArgs(queryCtx, `
		SELECT coordinator_term, rule_subterm, leader_id, coordinator_id, cohort_members,
		       durability_policy_name, durability_quorum_type, durability_required_count,
		       durability_async_fallback,
		       CASE
		         WHEN pg_is_in_recovery()
		           THEN COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
		         ELSE pg_current_wal_lsn()
		       END::text AS current_lsn
		FROM multigres.current_rule
		WHERE shard_id = $1`, []byte("0"))
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query current position")
	}
	if len(result.Rows) == 0 {
		return nil, nil
	}

	var coordinatorTerm, ruleSubterm int64
	var leaderIDStr, coordinatorIDStr *string
	var cohortNames []string
	var durabilityPolicyName, durabilityQuorumType, durabilityAsyncFallback *string
	var durabilityRequiredCount *int64
	var lsn string
	if err := executor.ScanRow(result.Rows[0],
		&coordinatorTerm,
		&ruleSubterm,
		&leaderIDStr,
		&coordinatorIDStr,
		&cohortNames,
		&durabilityPolicyName,
		&durabilityQuorumType,
		&durabilityRequiredCount,
		&durabilityAsyncFallback,
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan current position")
	}

	// coordinator_term=0 is the sentinel initial state; no rule has been applied yet.
	if coordinatorTerm == 0 {
		return nil, nil
	}

	var coordinatorIDStrVal string
	if coordinatorIDStr != nil {
		coordinatorIDStrVal = *coordinatorIDStr
	}
	pos, err := buildNodePosition(
		coordinatorTerm, ruleSubterm,
		leaderIDStr, coordinatorIDStrVal, cohortNames,
		durabilityPolicyName, durabilityQuorumType, durabilityRequiredCount, durabilityAsyncFallback,
		lsn,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse current position")
	}
	return pos, nil
}

// updateRule atomically writes a new rule by updating multigres.current_rule and
// appending to multigres.rule_history in a single CTE statement.
//
// The rule_subterm is assigned as:
//   - 0 if termNumber is greater than the current coordinator_term (new term)
//   - current rule_subterm + 1 if termNumber equals the current coordinator_term
//
// Fields not set via the builder (leaderID, cohortMembers) retain their current
// values in current_rule. All provided values are written to rule_history.
//
// current_rule is locked with SELECT FOR UPDATE before the update, serialising
// concurrent writes at the database level in addition to the caller's action lock.
//
// Returns the node's position (rule + WAL LSN) at the time of the write,
// or nil if force mode skipped the write.
//
// This operation uses the remote-operation-timeout and will fail if it cannot
// complete within that time. A timeout typically indicates that synchronous
// replication is not functioning.
func (rs *ruleStore) updateRule(ctx context.Context, update *ruleUpdateBuilder) (*clustermetadatapb.NodePosition, error) {
	if update.force {
		// Force mode skips history recording entirely. Force operations are emergency
		// operations that must configure replication GUCs regardless. The write would
		// block on sync replication with unreachable standbys, consuming the parent
		// context's deadline and causing subsequent GUC changes to fail.
		rs.logger.InfoContext(ctx, "Skipping rule update in force mode",
			"coordinator_term", update.termNumber,
			"event_type", update.eventType)
		return nil, nil
	}

	// Convert optional leader ID; empty string causes NULLIF→COALESCE to keep existing.
	var leaderStr string
	if update.leaderID != nil {
		pid, err := newPoolerID(update.leaderID)
		if err != nil {
			return nil, mterrors.Wrap(err, "invalid leader ID")
		}
		leaderStr = pid.appName
	}

	// Convert optional cohort; nil slice becomes SQL NULL, triggering COALESCE to keep existing.
	var cohortParam []string
	if update.cohortMembers != nil {
		pids, err := toPoolerIDs(update.cohortMembers)
		if err != nil {
			return nil, mterrors.Wrap(err, "invalid cohort member ID")
		}
		cohortParam = poolerIDsToAppNames(pids)
	}

	var acceptedParam []string
	if len(update.acceptedMembers) > 0 {
		pids, err := toPoolerIDs(update.acceptedMembers)
		if err != nil {
			return nil, mterrors.Wrap(err, "invalid accepted member ID")
		}
		acceptedParam = poolerIDsToAppNames(pids)
	}

	coordinatorIDStr := topoclient.ClusterIDString(update.coordinatorID)

	// For compare-and-swap: pass the expected term/subterm as SQL parameters.
	// NULL causes the WHERE clause to skip the check, allowing any current state.
	var previousTerm, previousSubterm *int64
	if update.previousRule != nil {
		previousTerm = &update.previousRule.coordinatorTerm
		previousSubterm = &update.previousRule.ruleSubterm
	}

	// Use the remote operation timeout for history writes. This write validates that synchronous
	// replication is functioning - it must wait long enough for standbys to connect and acknowledge.
	execCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
	defer cancel()

	result, err := rs.queryService.QueryArgs(execCtx, `
		WITH
		  locked AS (
		    SELECT coordinator_term, rule_subterm, leader_id, coordinator_id, cohort_members,
		           durability_policy_name, durability_quorum_type, durability_required_count,
		           durability_async_fallback
		    FROM multigres.current_rule
		    WHERE shard_id = $1
		      AND ($11::bigint IS NULL OR (coordinator_term = $11 AND rule_subterm = $12))
		    FOR UPDATE
		  ),
		  next_sub AS (
		    SELECT CASE
		      WHEN $2 > locked.coordinator_term THEN 0
		      ELSE locked.rule_subterm + 1
		    END AS value
		    FROM locked
		  ),
		  updated AS (
		    UPDATE multigres.current_rule
		    SET coordinator_term = $2,
		        rule_subterm     = next_sub.value,
		        leader_id        = COALESCE(NULLIF($4, ''), locked.leader_id),
		        coordinator_id   = NULLIF($5, ''),
		        cohort_members   = COALESCE($9, locked.cohort_members),
		        created_at       = $13
		    FROM next_sub, locked
		    WHERE shard_id = $1
		      AND ($2 > locked.coordinator_term OR ($2 = locked.coordinator_term AND next_sub.value > locked.rule_subterm))
		    RETURNING current_rule.coordinator_term, current_rule.rule_subterm,
		              current_rule.leader_id, current_rule.coordinator_id, current_rule.cohort_members,
		              current_rule.durability_policy_name, current_rule.durability_quorum_type,
		              current_rule.durability_required_count, current_rule.durability_async_fallback
		  ),
		  inserted AS (
		    INSERT INTO multigres.rule_history
		      (coordinator_term, rule_subterm, event_type, leader_id, coordinator_id,
		       wal_position, operation, reason, cohort_members, accepted_members, created_at)
		    SELECT updated.coordinator_term, updated.rule_subterm,
		           $3,
		           updated.leader_id,
		           updated.coordinator_id,
		           NULLIF($6, ''), NULLIF($7, ''), $8,
		           updated.cohort_members,
		           $10,
		           $13
		    FROM updated
		    RETURNING coordinator_term
		  )
		SELECT updated.coordinator_term, updated.rule_subterm,
		       updated.leader_id, updated.coordinator_id, updated.cohort_members,
		       updated.durability_policy_name, updated.durability_quorum_type,
		       updated.durability_required_count, updated.durability_async_fallback,
		       CASE
		         WHEN pg_is_in_recovery()
		           THEN COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
		         ELSE pg_current_wal_lsn()
		       END::text AS current_lsn
		FROM updated, inserted`,
		[]byte("0"),
		update.termNumber,
		update.eventType,
		leaderStr,
		coordinatorIDStr,
		update.walPosition,
		update.operation,
		update.reason,
		cohortParam,
		acceptedParam,
		previousTerm,
		previousSubterm,
		update.createdAt,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to write rule history record")
	}

	// Zero rows means either:
	//   - CAS check failed (expectedPreviousRule didn't match)
	//   - advancement check failed (term/subterm would not advance current state — bug)
	//   - shard row missing from current_rule (should never happen after initialisation)
	if len(result.Rows) == 0 {
		if update.previousRule != nil {
			return nil, errRuleConflict
		}
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"rule update rejected for term %d: current_rule already at equal or higher position",
			update.termNumber)
	}

	var coordinatorTerm, ruleSubterm int64
	var leaderIDStr *string
	var coordinatorIDStrResult string
	var cohortNames []string
	var durabilityPolicyName, durabilityQuorumType, durabilityAsyncFallback *string
	var durabilityRequiredCount *int64
	var lsn string
	if err := executor.ScanSingleRow(result,
		&coordinatorTerm,
		&ruleSubterm,
		&leaderIDStr,
		&coordinatorIDStrResult,
		&cohortNames,
		&durabilityPolicyName,
		&durabilityQuorumType,
		&durabilityRequiredCount,
		&durabilityAsyncFallback,
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan written rule position")
	}

	pos, err := buildNodePosition(
		coordinatorTerm, ruleSubterm,
		leaderIDStr, coordinatorIDStrResult, cohortNames,
		durabilityPolicyName, durabilityQuorumType, durabilityRequiredCount, durabilityAsyncFallback,
		lsn,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse written rule position")
	}
	return pos, nil
}

// buildNodePosition constructs a *clustermetadatapb.NodePosition from raw DB column values.
// leaderIDStr and coordinatorIDStr are app-name formatted strings (e.g. "zone1_pooler-name").
// durability fields are nil when not set in the DB.
func buildNodePosition(
	coordinatorTerm, ruleSubterm int64,
	leaderIDStr *string,
	coordinatorIDStr string,
	cohortNames []string,
	durabilityPolicyName, durabilityQuorumType *string,
	durabilityRequiredCount *int64,
	durabilityAsyncFallback *string,
	lsn string,
) (*clustermetadatapb.NodePosition, error) {
	rule := &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{
			CoordinatorTerm: coordinatorTerm,
			RuleSubterm:     ruleSubterm,
		},
	}

	if leaderIDStr != nil {
		id, err := parseApplicationName(*leaderIDStr)
		if err != nil {
			return nil, mterrors.Wrapf(err, "failed to parse leader_id %q", *leaderIDStr)
		}
		rule.PrimaryId = id
	}

	if coordinatorIDStr != "" {
		id, err := parseApplicationName(coordinatorIDStr)
		if err != nil {
			return nil, mterrors.Wrapf(err, "failed to parse coordinator_id %q", coordinatorIDStr)
		}
		rule.CoordinatorId = id
	}

	cohortIDs, err := appNamesToIDs(cohortNames)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse cohort_members")
	}
	rule.CohortMembers = cohortIDs

	if durabilityPolicyName != nil || durabilityQuorumType != nil || durabilityRequiredCount != nil || durabilityAsyncFallback != nil {
		dp := &clustermetadatapb.DurabilityPolicy{}
		if durabilityPolicyName != nil {
			dp.PolicyName = *durabilityPolicyName
		}
		if durabilityQuorumType != nil {
			v, ok := clustermetadatapb.QuorumType_value[*durabilityQuorumType]
			if !ok {
				return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "unknown quorum_type %q", *durabilityQuorumType)
			}
			dp.QuorumType = clustermetadatapb.QuorumType(v)
		}
		if durabilityRequiredCount != nil {
			dp.RequiredCount = int32(*durabilityRequiredCount)
		}
		if durabilityAsyncFallback != nil {
			v, ok := clustermetadatapb.AsyncReplicationFallbackMode_value[*durabilityAsyncFallback]
			if !ok {
				return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "unknown async_fallback %q", *durabilityAsyncFallback)
			}
			dp.AsyncFallback = clustermetadatapb.AsyncReplicationFallbackMode(v)
		}
		rule.DurabilityPolicy = dp
	}

	return &clustermetadatapb.NodePosition{
		Rule: rule,
		Lsn:  lsn,
	}, nil
}

// queryRuleHistory returns the most recent rule history records in descending
// order by (coordinator_term, rule_subterm). Returns at most limit records.
func (rs *ruleStore) queryRuleHistory(ctx context.Context, limit int) ([]ruleHistoryRecord, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	result, err := rs.queryService.QueryArgs(queryCtx, `
		SELECT coordinator_term, rule_subterm, event_type, leader_id, coordinator_id,
		       wal_position, operation, reason, cohort_members, accepted_members,
		       durability_policy_name, durability_quorum_type, durability_required_count,
		       durability_async_fallback, created_at
		FROM multigres.rule_history
		ORDER BY coordinator_term DESC, rule_subterm DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query rule_history")
	}

	records := make([]ruleHistoryRecord, 0, len(result.Rows))
	for _, row := range result.Rows {
		var rec ruleHistoryRecord
		var leaderIDStr *string
		var cohortNames, acceptedNames []string
		var durabilityRequiredCount *int64
		if err := executor.ScanRow(row,
			&rec.CoordinatorTerm,
			&rec.RuleSubterm,
			&rec.EventType,
			&leaderIDStr,
			&rec.CoordinatorID,
			&rec.WALPosition,
			&rec.Operation,
			&rec.Reason,
			&cohortNames,
			&acceptedNames,
			&rec.DurabilityPolicyName,
			&rec.DurabilityQuorumType,
			&durabilityRequiredCount,
			&rec.DurabilityAsyncFallback,
			&rec.CreatedAt,
		); err != nil {
			return nil, mterrors.Wrap(err, "failed to parse rule_history row")
		}
		if durabilityRequiredCount != nil {
			v := int32(*durabilityRequiredCount)
			rec.DurabilityRequiredCount = &v
		}
		if err := scanRuleHistoryRow(&rec, leaderIDStr, cohortNames, acceptedNames); err != nil {
			return nil, mterrors.Wrap(err, "failed to parse rule_history row")
		}
		records = append(records, rec)
	}
	return records, nil
}

// appNamesToIDs converts a slice of app-name formatted strings to proto IDs.
func appNamesToIDs(names []string) ([]*clustermetadatapb.ID, error) {
	ids := make([]*clustermetadatapb.ID, 0, len(names))
	for _, name := range names {
		id, err := parseApplicationName(name)
		if err != nil {
			return nil, mterrors.Wrapf(err, "invalid ID %q", name)
		}
		ids = append(ids, id)
	}
	return ids, nil
}
