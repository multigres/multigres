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

package eventlog

import "log/slog"

type NodeJoin struct{ NodeName string }

func (NodeJoin) EventType() string       { return "node.join" }
func (e NodeJoin) LogAttrs() []slog.Attr { return []slog.Attr{slog.String("node_name", e.NodeName)} }

type PrimaryPromotion struct {
	// NewPrimary is the elected leader. It is empty on the Started event because
	// the leader is not chosen until recruitment completes; it is set on the
	// terminal Success/Failed event.
	NewPrimary string
	// ProposedTerm is the term the coordinator is driving the cohort to. Known
	// before any RPC and stable through completion, it correlates a promotion's
	// Started and terminal events (and the term.begin event and rule-history row).
	ProposedTerm int64
	Reason       string // why the promotion was initiated, e.g. "ShardInit" or a failover trigger
	// RecruitMs and PromoteMs split the appointment latency (milliseconds,
	// monotonic clock) into its two phases: RecruitMs from Run start until a
	// leader is selected (recruiting a quorum), PromoteMs from leader selection
	// until the proposal commits. They are set only on terminal events and nil
	// where not applicable — both on Started, and PromoteMs on a recruitment
	// failure that never reached the promote phase. Draining late recruits
	// overlaps the promote phase but is off the commit critical path, so the
	// split reflects time-to-select-leader vs. time-to-commit.
	RecruitMs *int64
	PromoteMs *int64
}

const (
	attrRecruitMs = "recruit_ms"
	attrPromoteMs = "promote_ms"
)

func (PrimaryPromotion) EventType() string { return "primary.promotion" }
func (e PrimaryPromotion) LogAttrs() []slog.Attr {
	attrs := []slog.Attr{
		slog.Int64("proposed_term", e.ProposedTerm),
		slog.String("reason", e.Reason),
	}
	// Omitted on Started, where the leader is not yet known.
	if e.NewPrimary != "" {
		attrs = append(attrs, slog.String("new_primary", e.NewPrimary))
	}
	if e.RecruitMs != nil {
		attrs = append(attrs, slog.Int64(attrRecruitMs, *e.RecruitMs))
	}
	if e.PromoteMs != nil {
		attrs = append(attrs, slog.Int64(attrPromoteMs, *e.PromoteMs))
	}
	return attrs
}

type BackupAttempt struct{ BackupName string }

func (BackupAttempt) EventType() string { return "backup.attempt" }
func (e BackupAttempt) LogAttrs() []slog.Attr {
	return []slog.Attr{slog.String("backup_name", e.BackupName)}
}

type RestoreAttempt struct {
	BackupName string
}

func (RestoreAttempt) EventType() string { return "restore.attempt" }
func (e RestoreAttempt) LogAttrs() []slog.Attr {
	return []slog.Attr{slog.String("backup_name", e.BackupName)}
}

type PrimaryDemotion struct {
	NodeName string
	Reason   string // "stale" | "emergency"
}

func (PrimaryDemotion) EventType() string { return "primary.demotion" }
func (e PrimaryDemotion) LogAttrs() []slog.Attr {
	return []slog.Attr{slog.String("node_name", e.NodeName), slog.String("reason", e.Reason)}
}

type NodeDrain struct {
	NodeName string
	Reason   string // e.g. "rewind_not_feasible"
}

func (NodeDrain) EventType() string { return "node.drain" }
func (e NodeDrain) LogAttrs() []slog.Attr {
	return []slog.Attr{slog.String("node_name", e.NodeName), slog.String("reason", e.Reason)}
}

type BackupLeaseStolen struct {
	Stealer string
}

func (BackupLeaseStolen) EventType() string { return "backup.lease.stolen" }
func (e BackupLeaseStolen) LogAttrs() []slog.Attr {
	return []slog.Attr{slog.String("stealer", e.Stealer)}
}

type BackupLeaseLost struct {
	Holder string
}

func (BackupLeaseLost) EventType() string { return "backup.lease.lost" }
func (e BackupLeaseLost) LogAttrs() []slog.Attr {
	return []slog.Attr{slog.String("holder", e.Holder)}
}

type TermBegin struct {
	NewTerm      int64
	PreviousTerm int64
	RevokedRole  string // "primary" | "standby" | "" (empty = no revoke)
}

func (TermBegin) EventType() string { return "term.begin" }
func (e TermBegin) LogAttrs() []slog.Attr {
	return []slog.Attr{
		slog.Int64("new_term", e.NewTerm),
		slog.Int64("previous_term", e.PreviousTerm),
		slog.String("revoked_role", e.RevokedRole),
	}
}
