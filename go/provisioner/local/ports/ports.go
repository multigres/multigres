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
// By convention, ports are usually in the from 15xxx with HTTP ports
// at 15x00 and the corresponding gRPC port at 15x70.
package ports

const (
	// Standard ports (unchanged)
	DefaultEtcdPort     = 2379
	DefaultPostgresPort = 5432

	// Multiadmin
	DefaultMultiadminHTTP = 15000
	DefaultMultiadminGRPC = 15070

	// Multigateway
	DefaultMultigatewayHTTP = 15100
	DefaultMultigatewayGRPC = 15170
	DefaultMultigatewayPG   = 15432

	// Multipooler
	DefaultMultipoolerHTTP = 15200
	DefaultMultipoolerGRPC = 15270

	// Multiorch
	DefaultMultiorchHTTP = 15300
	DefaultMultiorchGRPC = 15370

	// Pgctld
	DefaultPgctldGRPC = 15470

	// pgBackRest server
	DefaultPgbackRestPort = 18432

	// Local Provisioner Defaults
	DefaultLocalPostgresPort = 25432
)
