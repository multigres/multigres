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

package constants

import "time"

// Constants used by the local provisioner during `multigres cluster start`.
const (
	// LocalBootstrapWaitTimeout bounds how long `multigres cluster start`
	// waits for every multigateway to serve queries end-to-end before
	// giving up.
	LocalBootstrapWaitTimeout = 2 * time.Minute

	// LocalGatewayBootstrapPollInterval is the delay between consecutive
	// readiness probes against a single gateway. The retry package
	// applies full jitter, so actual delays range from 0 to this value.
	LocalGatewayBootstrapPollInterval = 1 * time.Second

	// LocalGatewayBootstrapProbeTimeout bounds a single readiness probe
	// (a `SELECT 1` through multigateway) so a hung connection cannot
	// stall the wait loop.
	LocalGatewayBootstrapProbeTimeout = 5 * time.Second

	// MinPgBackrestVersion is the minimum supported pgBackRest version, in
	// semver form (with a leading "v") so it can be compared directly with
	// golang.org/x/mod/semver. Keep in sync with docs/building.md.
	MinPgBackrestVersion = "v2.57.0"

	// RequiredPostgresMajor is the only supported PostgreSQL major version.
	// Keep in sync with docs/building.md.
	RequiredPostgresMajor = 17
)
