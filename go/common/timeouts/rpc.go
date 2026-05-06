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

// PollResponseWait is the maximum time pollAndWaitForNewSnapshots spends
// waiting for poll responses from connected poolers. A connected pooler should
// respond within one network round-trip; this cap covers slow nodes and avoids
// blocking TriggerRecoveryNow indefinitely.
//
// This value was picked to be long enough to allow a round-trip to a slow node,
// but is shorter than the snapshot interval to ensure that we don't wait too
// long for a response before triggering recovery. In practice, this should be
// sufficient for most network conditions and allows for some variability in
// response times without causing undue delays in recovery.
const PollResponseWait = 500 * time.Millisecond

// readyDialTimeout bounds each probe dial. Local dials complete in microseconds,
// 500ms stays well within Kubernetes' default probe timeoutSeconds: 1, leaving
// headroom for HTTP overhead. It is a safety net for a kernel under extreme load.
const ReadyDialTimeout = 500 * time.Millisecond

// ReadyTopoCheckTimeout bounds the etcd connectivity probe in /ready handlers.
// etcd round-trips are network calls. 4s fits within a probe "timeoutSeconds: 5"
// while still detecting a genuinely unreachable etcd quickly.
const ReadyTopoCheckTimeout = 4 * time.Second
