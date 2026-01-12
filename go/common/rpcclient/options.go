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

package rpcclient

import (
	"go.opentelemetry.io/otel/attribute"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// PoolerSpanAttributes returns OpenTelemetry span attributes for gRPC clients
// connecting to multipooler instances. It sets:
// - multigres.pooler.id: the global pooler identifier (e.g., "multipooler-zone1-0")
//
// Use with grpccommon.WithAttributes() when creating gRPC clients:
//
//	grpccommon.NewClient(addr, grpccommon.WithAttributes(rpcclient.PoolerSpanAttributes(poolerID)...))
//
// TODO: Add peer.service="multipooler" to match OTel semantic conventions where
// peer.service should match the remote service's service.name resource attribute.
func PoolerSpanAttributes(poolerID *clustermetadatapb.ID) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("multigres.pooler.id", topoclient.MultiPoolerIDString(poolerID)),
	}
}
