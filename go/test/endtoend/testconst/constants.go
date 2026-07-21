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

// Package testconst holds shared constants for the endtoend test suite.
package testconst

import "time"

const (
	// ManagerStartTimeout is the maximum time to wait for a multipooler manager
	// to become ready.
	ManagerStartTimeout = 40 * time.Second

	// ShardBootstrapTimeout is the maximum time waitForShardBootstrap will wait
	// for all poolers to report a primary and complete initialization.
	ShardBootstrapTimeout = 60 * time.Second
)
