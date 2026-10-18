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

package poolerwatch

import (
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// Feed delivers pooler events to handlers from somewhere other than the
// topology, until stop is called. Handlers may be called before Feed returns.
type Feed func(handlers Handlers) (stop func())

// RecordFeed follows one pooler's record through watch, which calls its
// argument with the record right away and after every change. The first
// record is delivered as a snapshot of its cell and each change as an upsert,
// as the topology watch does. watch must deliver one record at a time.
func RecordFeed(watch func(fn func(*clustermetadatapb.Multipooler)) (stop func())) Feed {
	return func(handlers Handlers) func() {
		first := true
		return watch(func(record *clustermetadatapb.Multipooler) {
			if first {
				first = false
				handlers.OnSnapshot(record.GetId().GetCell(), []*clustermetadatapb.Multipooler{record})
				return
			}
			handlers.OnUpsert(record)
		})
	}
}
