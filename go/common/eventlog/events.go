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

type PrimaryPromotion struct{ NewPrimary string }

func (PrimaryPromotion) EventType() string { return "primary.promotion" }
func (e PrimaryPromotion) LogAttrs() []slog.Attr {
	return []slog.Attr{slog.String("new_primary", e.NewPrimary)}
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
