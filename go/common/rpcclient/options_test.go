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

package rpcclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestPoolerSpanAttributes(t *testing.T) {
	poolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "0",
	}

	assert.Equal(t, []attribute.KeyValue{
		attribute.String("peer.service", "multipooler"),
		attribute.String("multigres.pooler.id", "multipooler-zone1-0"),
	}, PoolerSpanAttributes(poolerID))
}
