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

package constants

import "time"

// StatementCancelDrainGrace bounds how long the multipooler waits for a
// backend to actually stop after it has asked PostgreSQL to cancel the
// statement (statement_timeout or an explicit cancel). pg_cancel_backend only
// reports that the signal was delivered; PostgreSQL acts on it at its next
// CHECK_FOR_INTERRUPTS point, which for some operations (GEOS buffering, for
// example) is hundreds of milliseconds later. Past this grace the pooler
// force-closes the socket so the caller unwinds.
//
// The multigateway keeps its RPC to the pooler open for this long past the
// statement deadline so it receives the pooler's report that the backend has
// stopped, instead of returning the timeout to the client while the backend is
// still running.
const StatementCancelDrainGrace = 2 * time.Second
