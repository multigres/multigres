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

package topoclient

// ConnRecovery is implemented by connections that can report when their link
// to the topology server was lost and then regained.
//
// A component that holds state in the topology uses it to check that state
// once after an outage, which covers a topology server that came back empty.
// Reporting only transitions keeps steady-state load on the server at zero,
// unlike a periodic check.
type ConnRecovery interface {
	// Recoveries returns a channel that receives a value each time the
	// connection is regained after a loss. It holds at most one pending value,
	// so several recoveries before the consumer reads coalesce into one. The
	// channel is closed when the connection is closed.
	//
	// It supports a single consumer, and every call returns the same channel.
	Recoveries() <-chan struct{}
}

// notifyRecovery performs a non-blocking send on a size-1 recovery channel.
func notifyRecovery(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
