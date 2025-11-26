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

package handler

import querypb "github.com/multigres/multigres/go/pb/query"

// We need ExecuteOptions here to be different from
// querypb.ExecuteOptions because that deals with the ExecuteOptions sent for a single shard
// Here we are sending ExecuteOptions that has information for all the shards that could be required.
type ExecuteOptions struct {
	SessionSettings map[string]string

	// prepared_statement is the prepared statement that should be
	// available on the connection for this query execution.
	PreparedStatement *querypb.PreparedStatement

	// portal is the portal (bound prepared statement) that should be
	// available on the connection for this query execution.
	Portal *querypb.Portal

	// max_rows is the maximum number of rows to return from Execute.
	MaxRows uint64

	// ShardStates is the information per shard that needs to be maintained.
	// It keeps track of any reserved connections on each Shard currently open.
	ShardStates []*ShardState
}
