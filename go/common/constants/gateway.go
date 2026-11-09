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

const (
	// ServerVersionVariable is the pseudo-variable that, qualified by the
	// product's version schema (servenv.VersionSchema), reports the running
	// multigateway's short release version: `SHOW multigres.server_version`, or
	// `SHOW minigres.server_version` in Minigres. It is not a real PostgreSQL
	// GUC: the gateway answers it locally and never forwards it to a backend.
	ServerVersionVariable = "server_version"

	// UnsafeConnectionParam is the connect-time option / GUC name that requests an
	// unsafe connection for a client connection (e.g. options=-c
	// multigres.unsafe_connection=on, or `SET multigres.unsafe_connection = on`).
	// It is a gateway-only property, never forwarded to a backend.
	UnsafeConnectionParam = "multigres.unsafe_connection"

	// DirectConnectionParam is the deprecated former name for
	// UnsafeConnectionParam. It is still recognized (at connect time and via SET)
	// so existing clients keep working, but new code and docs should use
	// UnsafeConnectionParam. Remove once all clients have migrated off
	// multigres.direct_connection.
	DirectConnectionParam = "multigres.direct_connection"

	// MultigresSchema is the schema used to namespace gateway-provided views
	// (e.g. `multigres.multipoolers`) so they do not shadow PostgreSQL's own
	// objects of the same name. The version surfaces use the product's schema
	// instead (servenv.VersionSchema), which is also "multigres" in Multigres.
	MultigresSchema = "multigres"

	// MultigresVersionFunction is the name of the gateway function
	// `multigres.version()` (`minigres.version()` in Minigres), which reports the
	// running multigateway's full build string. It doubles as the default output column label, matching
	// PostgreSQL's convention of labelling a bare function-call target with the
	// function name.
	MultigresVersionFunction = "version"

	// MultigresMultigatewaysView and MultigresMultipoolersView are the gateway
	// views `multigres.multigateways` and `multigres.multipoolers`. They have no
	// backend relation: the gateway folds each reference into a VALUES subquery
	// of its current cluster snapshot at execution time (see
	// engine/cluster_view.go). The `multigres` qualifier is required — the
	// gateway does not know a session's search_path.
	MultigresMultigatewaysView = "multigateways"
	MultigresMultipoolersView  = "multipoolers"

	// MaxBufferingRetries is the maximum number of times a query will be
	// retried after failover buffering completes.
	MaxBufferingRetries = 3

	// MaxStatementTimeoutMS is PostgreSQL's upper bound for statement_timeout:
	// the max of the underlying integer GUC, measured in its base unit
	// (milliseconds). Values outside 0 .. MaxStatementTimeoutMS are rejected,
	// matching PostgreSQL.
	MaxStatementTimeoutMS = 2147483647
)
