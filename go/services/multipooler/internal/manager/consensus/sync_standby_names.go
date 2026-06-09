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

	loadTimeCtx, loadTimeCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer loadTimeCancel()
	result, err := qs.Query(loadTimeCtx, "SELECT pg_conf_load_time()")
	if err != nil {
		return mterrors.Wrap(err, "failed to read pg_conf_load_time before reload")
	}
	var loadTimeBefore string
	if err := executor.ScanSingleRow(result, &loadTimeBefore); err != nil {
		return mterrors.Wrap(err, "failed to scan pg_conf_load_time before reload")
	}

	logger.InfoContext(ctx, "Reloading PostgreSQL configuration")
	reloadCtx, reloadCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer reloadCancel()
	if _, err := qs.Query(reloadCtx, "SELECT pg_reload_conf()"); err != nil {
		logger.ErrorContext(ctx, "Failed to reload configuration", "error", err)
		return mterrors.Wrap(err, "failed to reload PostgreSQL configuration")
	}

	// Poll pg_conf_load_time() until it advances. retry.New uses "do work, then
	// back off" semantics, so the backoff timer starts after the previous query
	// finishes — a slow query under load doesn't cause back-to-back hammering.
	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	r := retry.New(1*time.Millisecond, 20*time.Millisecond)
	for _, attemptErr := range r.Attempts(waitCtx) {
		if attemptErr != nil {
			return mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED,
				"timeout waiting for pg_conf_load_time to advance after pg_reload_conf")
		}
		queryCtx, queryCancel := context.WithTimeout(waitCtx, 500*time.Millisecond)
		result, err := qs.Query(queryCtx, "SELECT pg_conf_load_time()")
		queryCancel()
		if err != nil {
			return mterrors.Wrap(err, "failed to poll pg_conf_load_time after reload")
		}
		var loadTimeAfter string
		if err := executor.ScanSingleRow(result, &loadTimeAfter); err != nil {
			return mterrors.Wrap(err, "failed to scan pg_conf_load_time after reload")
		}
		if loadTimeAfter != loadTimeBefore {
			return nil
		}
	}
	// Unreachable: r.Attempts only exits via the ctx-cancelled branch above.
	return mterrors.New(mtrpcpb.Code_INTERNAL, "reload polling loop exited unexpectedly")
}
