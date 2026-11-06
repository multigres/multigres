// Copyright 2025 Supabase, Inc.
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

// Service names for telemetry, logging, and service registration.
// Use these constants instead of string literals to enable type-safe refactoring.
const (
	// ServiceMultigateway is the name of the multigateway service.
	ServiceMultigateway = "multigateway"

	// ServiceMultipooler is the name of the multipooler service.
	ServiceMultipooler = "multipooler"

	// ServiceMultiorch is the name of the multiorch service.
	ServiceMultiorch = "multiorch"

	// ServicePgctld is the name of the pgctld service.
	ServicePgctld = "pgctld"

	// ServiceMultiadmin is the name of the multiadmin service.
	ServiceMultiadmin = "multiadmin"

	// ServicePgbackrest is the name of the pgbackrest service.
	ServicePgbackrest = "pgbackrest"

	// ServiceMinigres is the name of the minigres service, which runs a
	// multigateway and a multipooler in one process.
	ServiceMinigres = "minigres"
)

// PgServingServices lists the service names that accept client PostgreSQL
// wire-protocol connections directly. Tooling that needs to find a cluster's
// Postgres entry point should check membership in this list instead of
// hardcoding a single service name, so it keeps working regardless of
// which of these topologies a given cell is configured with.
var PgServingServices = []string{ServiceMultigateway, ServiceMinigres}
