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

// HeartbeatWriteInterval is the cadence at which the primary writes heartbeats
// to the multigres.heartbeat table. Used by the multipooler heartbeat writer
// and by multiorch to determine heartbeat staleness.
const HeartbeatWriteInterval = 1 * time.Second
