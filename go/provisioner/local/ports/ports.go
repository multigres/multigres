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

// Package ports defines default port constants for Multigres services.
package ports

const (
	// Global Services
	MultiadminHTTP = 15000
	MultiadminGRPC = 15990
	LocalproxyHTTP = 15800

	// Cell Services - HTTP
	MultigatewayHTTP = 15001
	MultipoolerHTTP  = 15001
	MultiorchHTTP    = 15301

	// Cell Services - gRPC
	MultigatewayGRPC = 15991
	MultipoolerGRPC  = 16001
	MultiorchGRPC    = 16000
	PgctldGRPC       = 17000

	// PostgreSQL
	PostgresPort = 5432
)
