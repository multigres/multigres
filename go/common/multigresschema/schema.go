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

// Package multigresschema defines the `multigres` PostgreSQL schema:
// multigres-owned metadata tables that live inside the managed database
// itself (as opposed to cluster topology, which lives in etcd). Every DDL
// statement here must be idempotent (IF NOT EXISTS) — it can be applied by
// any component at any time, on a fresh or already-provisioned database.
package multigresschema

// Name is the PostgreSQL schema that holds multigres metadata tables.
const Name = "multigres"

// BackendVpidTable is the fully qualified name of the vpid mapping table.
const BackendVpidTable = "multigres.backend_vpid"

// BackendVpidDDL creates the multigres schema and the backend_vpid table,
// which maps a live PostgreSQL backend pid to the multigateway virtual pid
// (the pid the proxied client sees) it is currently serving. The multipooler
// upserts a row whenever it hands a backend to a gateway session; the
// pgregress isolation harness reads the table to translate virtual pids into
// real pids for lock-wait probes.
//
// The table is UNLOGGED: it describes live backend processes, which do not
// survive a crash either, and standbys (which cannot be written to anyway)
// never need it. The GRANTs let pool connections of any user upsert their
// own mapping row.
const BackendVpidDDL = `CREATE SCHEMA IF NOT EXISTS multigres;
CREATE UNLOGGED TABLE IF NOT EXISTS multigres.backend_vpid (
	backend_pid integer PRIMARY KEY,
	vpid bigint NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now()
);
GRANT USAGE ON SCHEMA multigres TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON multigres.backend_vpid TO PUBLIC`
