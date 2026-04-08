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

import "time"

// Schema tracking constants used by the multipooler's schema tracker and the
// DDL event trigger installed in the multigres sidecar schema.
const (
	// SchemaChangedChannel is the PostgreSQL NOTIFY channel name used by the
	// DDL event trigger installed in the multigres sidecar schema.
	SchemaChangedChannel = "multigres.schema_changed"

	// DefaultSchemaTrackingInterval is the default polling interval for the
	// schema tracker. Used as the flag default for --schema-tracking-interval.
	DefaultSchemaTrackingInterval = 5 * time.Minute
)
