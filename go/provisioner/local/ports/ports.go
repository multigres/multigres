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
	DefaultMultiadminHTTP = 15000
	DefaultMultiadminGRPC = 15990
	DefaultEtcdPort       = 2379

	// Cell Services - HTTP
	DefaultMultigatewayHTTP = 15001
	DefaultMultipoolerHTTP  = 15001
	DefaultMultiorchHTTP    = 15301

	// Cell Services - gRPC
	DefaultMultigatewayGRPC = 15991
	DefaultMultipoolerGRPC  = 16001
	DefaultMultiorchGRPC    = 16000
	DefaultPgctldGRPC       = 17000

	// PostgreSQL
	DefaultPostgresPort   = 5432
	DefaultMultigatewayPG = 15432
)
