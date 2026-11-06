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
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// A caller that cannot resolve or reach pooler addresses passes its own dialer;
// every connection to a pooler must go through it with the raw host:port, even
// for a hostname the local resolver cannot resolve.
func TestWithContextDialer_RoutesUnresolvableHostThroughTheDialer(t *testing.T) {
	var (
		mu    sync.Mutex
		dials []string
	)
	dial := func(_ context.Context, addr string) (net.Conn, error) {
		mu.Lock()
		dials = append(dials, addr)
		mu.Unlock()
		return nil, errors.New("recording dialer refuses every dial")
	}
	c := NewMultipoolerClient(1, grpc.WithTransportCredentials(insecure.NewCredentials()), WithContextDialer(dial))
	t.Cleanup(c.Close)

	pooler := &clustermetadatapb.Multipooler{
		Hostname: "pooler-0.example.internal",
		PortMap:  map[string]int32{"grpc": 15200},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err := c.Status(ctx, pooler, &multipoolermanagerdatapb.StatusRequest{})
	require.Error(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, dials, "the dialer was never used")
	require.Equal(t, "pooler-0.example.internal:15200", dials[0])
}
