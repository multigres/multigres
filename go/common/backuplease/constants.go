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

package backuplease

import "time"

const (
	// SIGTERMTimeout is how long to wait after sending SIGTERM before
	// escalating to SIGKILL.
	SIGTERMTimeout = 5 * time.Second

	// StealGracePeriod is how long a stealer waits after revoking the old
	// holder's lease before proceeding. This gives the old holder time to
	// kill its pgbackrest process gracefully.
	StealGracePeriod = SIGTERMTimeout

	// CheckInterval is how often the lease monitor polls Check() to detect
	// lease loss.
	CheckInterval = 10 * time.Second
)
