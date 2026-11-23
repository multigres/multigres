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

// ProbeRole is the role an external availability probe logs in as.
const ProbeRole = "multigres_probe"

// GrantProbeReadSQL lets ProbeRole read the heartbeat table. It does nothing
// if the role or the table is missing, or the grants are already there.
//
// The schema check reads the ACL because has_schema_privilege is also true
// through PUBLIC, and the probe should not depend on that grant.
const GrantProbeReadSQL = `DO $$ BEGIN
	IF to_regclass('multigres.heartbeat') IS NULL
		OR NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '` + ProbeRole + `') THEN
		RETURN;
	END IF;
	IF NOT EXISTS (
		SELECT FROM pg_namespace n, aclexplode(n.nspacl) a
		WHERE n.nspname = 'multigres'
			AND a.grantee = '` + ProbeRole + `'::regrole
			AND a.privilege_type = 'USAGE'
	) THEN
		GRANT USAGE ON SCHEMA multigres TO ` + ProbeRole + `;
	END IF;
	IF NOT has_table_privilege('` + ProbeRole + `', 'multigres.heartbeat', 'SELECT') THEN
		GRANT SELECT ON TABLE multigres.heartbeat TO ` + ProbeRole + `;
	END IF;
END $$`
