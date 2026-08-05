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

package constants

// LogicalReplicationConnAppNamePrefix tags a logical-replication connection's
// Postgres session (the `application_name` startup parameter) so the
// replicationstats poller can correlate pg_stat_replication rows back to a
// specific reserved-pool connection. Set at connect time by
// reserved.Pool.NewLogicalReplicationConn; read by replicationstats.Poller.
// Kept as one shared constant, rather than a copy in each package, so the
// writer and reader can't drift out of sync.
const LogicalReplicationConnAppNamePrefix = "mg-replconn-"
