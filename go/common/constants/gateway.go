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
	// MultigresServerVersionVariable is the pseudo-variable name that, via
	// `SHOW multigres.server_version`, reports the running multigateway's short release
	// version. It is not a real PostgreSQL GUC: the gateway answers it locally
	// and never forwards it to a backend.
	MultigresServerVersionVariable = "multigres.server_version"

	// MultigresSchema is the schema used to namespace gateway-provided functions
	// (e.g. `multigres.version()`) so they do not shadow PostgreSQL's own
	// built-ins of the same name.
	MultigresSchema = "multigres"

	// MultigresVersionFunction is the name of the gateway function
	// `multigres.version()`, which reports the running multigateway's full build
	// string. It doubles as the default output column label, matching
	// PostgreSQL's convention of labelling a bare function-call target with the
	// function name.
	MultigresVersionFunction = "version"

	// MaxBufferingRetries is the maximum number of times a query will be
	// retried after failover buffering completes.
	MaxBufferingRetries = 3

	// MaxStatementTimeoutMS is PostgreSQL's upper bound for statement_timeout:
	// the max of the underlying integer GUC, measured in its base unit
	// (milliseconds). Values outside 0 .. MaxStatementTimeoutMS are rejected,
	// matching PostgreSQL.
	MaxStatementTimeoutMS = 2147483647
)
