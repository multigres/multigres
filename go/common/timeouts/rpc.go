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

// Package timeouts defines common timeout constants used across multigres components.
package timeouts

import "time"

// RemoteOperationTimeout is the default timeout for remote operations such as
// RPC calls, etcd data fetches, and synchronous replication health checks.
const RemoteOperationTimeout = 15 * time.Second

// DefaultHealthStreamStalenessTimeout is the default staleness watchdog timeout for
// ManagerHealthStream connections. If no message is received within this window
// the client reconnects and the server advertises this value in the start response.
const DefaultHealthStreamStalenessTimeout = 90 * time.Second

// DefaultHealthHeartbeatInterval is the interval between periodic health
// broadcasts when no state changes occur.
const DefaultHealthHeartbeatInterval = 30 * time.Second

// DefaultSnapshotInterval is the proactive snapshot rate used when the
// orchestrator does not specify snapshot_interval in the start message.
// This guarantees that postgres state changes (e.g. process death) reach the
// orchestrator within this interval even when the local monitor is disabled.
const DefaultSnapshotInterval = 5 * time.Second
