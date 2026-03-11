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

// Health stream constants used by the multigateway's pooler connection.
const (
	// DefaultHealthRetryDelay is the initial delay before retrying a failed health stream.
	DefaultHealthRetryDelay = 5 * time.Second

	// DefaultHealthCheckTimeout is the timeout for detecting a stale health stream.
	// If no message is received within this duration, the connection is marked unhealthy.
	DefaultHealthCheckTimeout = 1 * time.Minute
)
