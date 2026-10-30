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

package heartbeat

// CreateTableSQL creates the table the Writer and Reader share. It is defined here, next
// to the queries that use it, so the multipooler's schema setup and the tests build
// exactly the same table.
const CreateTableSQL = `CREATE TABLE multigres.heartbeat (
		shard_id BYTEA PRIMARY KEY,
		leader_id TEXT NOT NULL,
		ts BIGINT NOT NULL,
		quorum_commit_lsn pg_lsn,
		quorum_commit_ts TIMESTAMPTZ
	)`
