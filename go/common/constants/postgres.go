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

// PostgreSQL default values - semantically separate concepts.
// These are distinct constants despite having the same string value because
// they represent different concepts that could diverge in the future.
const (
	// DefaultPostgresUser is the default PostgreSQL superuser name.
	// This is the administrative user that owns the database cluster.
	DefaultPostgresUser = "postgres"

	// DefaultPostgresDatabase is the default database that always exists in PostgreSQL.
	// This database is created during cluster initialization.
	DefaultPostgresDatabase = "postgres"

	// PostgresExecutable is the name of the PostgreSQL server binary.
	PostgresExecutable = "postgres"
)
