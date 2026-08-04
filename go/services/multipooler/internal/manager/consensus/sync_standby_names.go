// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consensus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/timeouts"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/tools/retry"
)

// FormatStandbyList formats a list of pooler IDs as a comma-separated list of quoted application names.
func FormatStandbyList(ids []ReplicaID) string {
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = fmt.Sprintf(`"%s"`, id.appName)
	}
	return strings.Join(quoted, ", ")
}

// BuildSynchronousStandbyNamesValue constructs the synchronous_standby_names value string
// This produces values like: FIRST 1 ("standby-1", "standby-2") or ANY 1 ("standby-1", "standby-2")
func BuildSynchronousStandbyNamesValue(method multipoolermanagerdatapb.SynchronousMethod, numSync int32, names []ReplicaID) (string, error) {
	if len(names) == 0 {
		return "", nil
	}

	var methodStr string
	switch method {
	case multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST:
		methodStr = "FIRST"
	case multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY:
		methodStr = "ANY"
	default:
		return "", mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid synchronous method: %s, must be FIRST or ANY", method.String()))
	}

	return fmt.Sprintf("%s %d (%s)", methodStr, numSync, FormatStandbyList(names)), nil
}

// ----------------------------------------------------------------------------
// Validation Helpers
// ----------------------------------------------------------------------------
// ValidateStandbyIDs validates that the list is non-empty and converts each ID to its ReplicaID.
func ValidateStandbyIDs(standbyIDs []*clustermetadatapb.ID) ([]ReplicaID, error) {
	if len(standbyIDs) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "standby_ids cannot be empty")
	}
	pids, err := ToReplicaIDs(standbyIDs)
	if err != nil {
		return pids, mterrors.Wrap(err, "invalid standby_ids")
	}
	return pids, nil
}

// ValidateSyncReplicationParams validates the parameters for setting synchronous_standby_names.
func ValidateSyncReplicationParams(numSync int32, standbyIDs []*clustermetadatapb.ID) ([]ReplicaID, error) {
	// Validate numSync is non-negative
	if numSync < 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("num_sync must be non-negative, got: %d", numSync))
	}

	// If standbyIDs are provided, validate them
	if len(standbyIDs) > 0 {
		// Validate that numSync doesn't exceed the number of standbys (PostgreSQL requirement)
		// Note: numSync=0 is allowed and will be defaulted to 1 in setSynchronousStandbyNames
		if numSync > int32(len(standbyIDs)) {
			return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
				fmt.Sprintf("num_sync (%d) cannot exceed number of standby_ids (%d)", numSync, len(standbyIDs)))
		}

		// Validate each standby ID
		names, err := ValidateStandbyIDs(standbyIDs)
		if err != nil {
			return nil, err
		}
		return names, nil
	}

	return nil, nil
}

// ApplyAddOperation adds new standbys to the standby list (idempotent)
func ApplyAddOperation(currentStandbys, newStandbys []ReplicaID) []ReplicaID {
	updatedStandbys := append([]ReplicaID{}, currentStandbys...)
	existingMap := make(map[string]bool, len(currentStandbys))
	for _, standby := range currentStandbys {
		existingMap[standby.appName] = true
	}
	for _, newStandby := range newStandbys {
		if !existingMap[newStandby.appName] {
			updatedStandbys = append(updatedStandbys, newStandby)
		}
	}
	return updatedStandbys
}

// ApplyRemoveOperation removes standby names from the standby list (idempotent)
func ApplyRemoveOperation(currentStandbys, standbysToRemove []ReplicaID) []ReplicaID {
	removeMap := make(map[string]bool, len(standbysToRemove))
	for _, standby := range standbysToRemove {
		removeMap[standby.appName] = true
	}
	var updatedStandbys []ReplicaID
	for _, standby := range currentStandbys {
		if !removeMap[standby.appName] {
			updatedStandbys = append(updatedStandbys, standby)
		}
	}
	return updatedStandbys
}

// ReloadPostgresConfig reloads PostgreSQL configuration to apply changes made via
// ALTER SYSTEM, and waits for postmaster to finish re-reading the config files
// before returning.
//
// pg_reload_conf() returns immediately after sending SIGHUP to postmaster, well
// before any of that work has happened. We use pg_conf_load_time() — the
// timestamp of postmaster's most recent successful config load — as the
// completion signal: once a follow-up query observes it advance past the
// pre-reload value, postmaster has re-read postgresql.auto.conf and signalled
// its child processes.
//
// Caveat: this guarantees postmaster has processed the reload, not that every
// child process has. Backends (the walreceiver, individual query backends)
// each pick up SIGHUP at their own pace — typically within milliseconds, but
// not synchronously. Callers that need to observe a child's reaction (e.g.
// polling pg_stat_wal_receiver for the walreceiver to disconnect after
// clearing primary_conninfo) should still poll, but they can do so knowing
// the new config is loaded server-side rather than racing with postmaster's
// signal handler.
func ReloadPostgresConfig(ctx context.Context, logger *slog.Logger, qs executor.InternalQueryService) error {
	if qs == nil {
		return errors.New("internal query service not available")
	}

	loadTimeCtx, loadTimeCancel := context.WithTimeout(ctx, timeouts.PostgresConfigTimeout)
	defer loadTimeCancel()
	loadTimeBefore, err := readConfLoadTime(loadTimeCtx, qs)
	if err != nil {
		return mterrors.Wrap(err, "failed to read pg_conf_load_time before reload")
	}

	logger.InfoContext(ctx, "reloading Postgres configuration")
	reloadCtx, reloadCancel := context.WithTimeout(ctx, timeouts.PostgresConfigTimeout)
	defer reloadCancel()
	if _, err := qs.Query(reloadCtx, "SELECT pg_reload_conf()"); err != nil {
		logger.ErrorContext(ctx, "failed to reload configuration", "error", err)
		return mterrors.Wrap(err, "failed to reload PostgreSQL configuration")
	}

	// Confirm the reload landed by waiting for pg_conf_load_time() to change
	// from the value read before the reload.
	if _, err := WaitForConfigReload(ctx, qs, func(loadTime time.Time) bool {
		return !loadTime.Equal(loadTimeBefore)
	}); err != nil {
		return err
	}
	return nil
}

// WaitForConfigReload polls PostgreSQL's config load time until isReloaded
// reports that the observed value reflects a completed reload, and returns that
// load time. It backs off between polls — retry.New uses "do work, then back
// off" semantics, so the backoff timer starts after the previous query finishes
// and a slow query under load does not cause back-to-back hammering — and
// returns DEADLINE_EXCEEDED if no reload is observed within the budget.
//
// The isReloaded predicate lets each caller define "reloaded" against its own
// baseline: a SQL pg_reload_conf() caller (ReloadPostgresConfig) waits for the
// load time to differ from the value it read beforehand, while a SIGHUP caller
// waits for it to reach a wall-clock moment captured before the signal.
func WaitForConfigReload(ctx context.Context, qs executor.InternalQueryService, isReloaded func(loadTime time.Time) bool) (time.Time, error) {
	if qs == nil {
		return time.Time{}, errors.New("internal query service not available")
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	r := retry.New(1*time.Millisecond, 20*time.Millisecond)
	for _, attemptErr := range r.Attempts(waitCtx) {
		if attemptErr != nil {
			return time.Time{}, mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED,
				"timeout waiting for pg_conf_load_time to advance after reload")
		}
		queryCtx, queryCancel := context.WithTimeout(waitCtx, timeouts.PostgresConfigTimeout)
		loadTime, err := readConfLoadTime(queryCtx, qs)
		queryCancel()
		if err != nil {
			return time.Time{}, err
		}
		if isReloaded(loadTime) {
			return loadTime, nil
		}
	}
	// Unreachable: r.Attempts only exits via the ctx-cancelled branch above.
	return time.Time{}, mterrors.New(mtrpcpb.Code_INTERNAL, "reload polling loop exited unexpectedly")
}

// readConfLoadTime reads PostgreSQL's configuration load time. It selects the
// value as a Unix epoch (extract(epoch from ...)) rather than the default text
// rendering so the result is timezone-independent: pg_conf_load_time() text is
// formatted in the session's TimeZone, whose offset can carry minutes (e.g.
// +05:30) that the text-timestamp scanner does not parse. The epoch is an
// absolute instant, safe to compare across callers and sessions.
func readConfLoadTime(ctx context.Context, qs executor.InternalQueryService) (time.Time, error) {
	result, err := qs.Query(ctx, "SELECT extract(epoch from pg_conf_load_time())")
	if err != nil {
		return time.Time{}, mterrors.Wrap(err, "failed to read pg_conf_load_time")
	}
	var epoch float64
	if err := executor.ScanSingleRow(result, &epoch); err != nil {
		return time.Time{}, mterrors.Wrap(err, "failed to scan pg_conf_load_time")
	}
	sec := int64(epoch)
	nsec := int64((epoch - float64(sec)) * 1e9)
	return time.Unix(sec, nsec).UTC(), nil
}
