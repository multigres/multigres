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

type PrimaryInit struct{ PoolerName string }

func (PrimaryInit) EventType() string { return "primary.init" }
func (e PrimaryInit) LogAttrs() []slog.Attr {
	return []slog.Attr{slog.String("pooler_name", e.PoolerName)}
}

type StandbyInit struct{ PoolerName string }

func (StandbyInit) EventType() string { return "standby.init" }
func (e StandbyInit) LogAttrs() []slog.Attr {
	return []slog.Attr{slog.String("pooler_name", e.PoolerName)}
}

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
