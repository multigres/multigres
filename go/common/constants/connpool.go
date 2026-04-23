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

// Connection-pool retry constants. Used by both the regular pool
// (retryOnConnectionError around individual query ops) and the reserved
// pool (acquire+validate retry on NewConn). Kept together so the two
// retry regimes stay in lockstep.
const (
	// MaxConnPoolRetryAttempts is the maximum number of attempts for
	// retrying a connection-pool operation that hit a connection-class
	// error. On each retry, the underlying socket is replaced.
	MaxConnPoolRetryAttempts = 3

	// ConnPoolRetryBackoff is the delay between retry attempts. The
	// short pause gives PostgreSQL time to finish starting up when the
	// connection error is due to a restart.
	ConnPoolRetryBackoff = 100 * time.Millisecond
)
