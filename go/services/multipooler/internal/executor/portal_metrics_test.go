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

package executor

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/protoutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// The extended protocol (Parse/Bind/Execute) is what drivers use, so the
// portal path carries most production traffic. These tests pin that it feeds
// the same mg.pooler.query.* metrics as the simple-query path, on every
// branch: an existing reservation, a reservation created for the portal, and
// a regular pooled connection. Without them the pooler's only per-statement
// signal for that traffic was the otelgrpc rpc.* histogram.

// newMetricsExecutor returns an executor whose queryStats report to an
// in-memory reader, plus the reader.
func newMetricsExecutor(t *testing.T, pm *stubPoolManager) (*Executor, *sdkmetric.ManualReader) {
	t.Helper()
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	e := NewExecutor(slog.Default(), pm, &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, false)
	e.metrics = newQueryStats()
	return e, setup.MetricReader
}

// newReservedTestPool builds a reserved pool against server. Callers must
// `defer pool.Close()` themselves, after their `defer server.Close()`, so the
// pool shuts down before the server does; closing it afterwards makes the
// pool wait out its drain timeout against a dead server.
func newReservedTestPool(t *testing.T, server *fakepgserver.Server) *reserved.Pool {
	t.Helper()
	return reserved.NewPool(context.Background(), &reserved.PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
}

func hasMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) bool {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, mm := range sm.Metrics {
			if mm.Name == name {
				return true
			}
		}
	}
	return false
}

// singleDurationPoint asserts mg.pooler.query.duration has exactly one data
// point and returns it.
func singleDurationPoint(t *testing.T, reader *sdkmetric.ManualReader) metricdata.HistogramDataPoint[float64] {
	t.Helper()
	dur := findMetric(t, reader, "mg.pooler.query.duration")
	hist, ok := dur.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, hist.DataPoints, 1, "one portal execution, one duration sample")
	return hist.DataPoints[0]
}

func TestPortalStreamExecute_ExistingReservationRecordsQueryMetrics(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newReservedTestPool(t, server)
	defer pool.Close()
	rconn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)

	e, reader := newMetricsExecutor(t, &stubPoolManager{reservedConn: rconn, reservedConnOK: true})

	_, err = e.PortalStreamExecute(context.Background(), &query.Target{},
		&query.PreparedStatement{Name: "stmt0", Query: "SELECT 1"},
		&query.Portal{Name: "p0"},
		&query.ExecuteOptions{User: "postgres", ReservedConnectionId: uint64(rconn.ConnID())},
		nil, nil, noopCallback)
	require.NoError(t, err)

	dp := singleDurationPoint(t, reader)
	assert.Equal(t, poolTypeReserved, attrValue(t, dp.Attributes, "pool_type"))
	assert.Equal(t, "ok", attrValue(t, dp.Attributes, "status"))
	assert.True(t, hasMetric(t, reader, "mg.pooler.query.rows"))
	assert.False(t, hasMetric(t, reader, "mg.pooler.query.errors"), "no error metric on success")
	assert.False(t, hasMetric(t, reader, "mg.pooler.query.pool_acquire.duration"),
		"reusing an existing reservation is a lookup, not a pool acquire")
}

func TestPortalStreamExecute_ExistingReservationRecordsErrorMetrics(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	server.AddRejectedQuery("select 1/0", mterrors.NewPgError("ERROR", "22012", "division by zero", ""))

	pool := newReservedTestPool(t, server)
	defer pool.Close()
	rconn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)

	e, reader := newMetricsExecutor(t, &stubPoolManager{reservedConn: rconn, reservedConnOK: true})

	_, err = e.PortalStreamExecute(context.Background(), &query.Target{},
		&query.PreparedStatement{Name: "stmt0", Query: "SELECT 1/0"},
		&query.Portal{Name: "p0"},
		&query.ExecuteOptions{User: "postgres", ReservedConnectionId: uint64(rconn.ConnID())},
		nil, nil, noopCallback)
	require.Error(t, err)

	dp := singleDurationPoint(t, reader)
	assert.Equal(t, poolTypeReserved, attrValue(t, dp.Attributes, "pool_type"))
	assert.Equal(t, "error", attrValue(t, dp.Attributes, "status"))

	errs := findMetric(t, reader, "mg.pooler.query.errors")
	sum, ok := errs.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	assert.Equal(t, int64(1), sum.DataPoints[0].Value)
	assert.Equal(t, "22012", attrValue(t, sum.DataPoints[0].Attributes, "sqlstate"))
	assert.Equal(t, poolTypeReserved, attrValue(t, sum.DataPoints[0].Attributes, "pool_type"))
}

func TestPortalStreamExecute_NewReservationRecordsPoolAcquire(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newReservedTestPool(t, server)
	defer pool.Close()
	e, reader := newMetricsExecutor(t, &stubPoolManager{newReservedPool: pool})

	// No ReservedConnectionId and a transaction reason: the portal reserves a
	// backend for itself, which is the pool acquire the metric must see.
	state, err := e.PortalStreamExecute(context.Background(), &query.Target{},
		&query.PreparedStatement{Name: "stmt0", Query: "SELECT 1"},
		&query.Portal{Name: "p0"},
		&query.ExecuteOptions{User: "postgres"},
		nil, &query.ReservationOptions{Reasons: protoutil.ReasonTransaction}, noopCallback)
	require.NoError(t, err)
	require.NotNil(t, state)

	dp := singleDurationPoint(t, reader)
	assert.Equal(t, poolTypeReserved, attrValue(t, dp.Attributes, "pool_type"))
	assert.Equal(t, "ok", attrValue(t, dp.Attributes, "status"))

	acq := findMetric(t, reader, "mg.pooler.query.pool_acquire.duration")
	acqHist, ok := acq.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, acqHist.DataPoints, 1)
	assert.Equal(t, poolTypeReserved, attrValue(t, acqHist.DataPoints[0].Attributes, "pool_type"))
	assert.Equal(t, "acquired", attrValue(t, acqHist.DataPoints[0].Attributes, "outcome"))
}

func TestPortalStreamExecute_RegularConnectionRecordsQueryAndAcquireMetrics(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	clientConn, err := client.Connect(context.Background(), context.Background(), server.ClientConfig())
	require.NoError(t, err)
	e, reader := newMetricsExecutor(t, &stubPoolManager{
		regularConn: &connpool.Pooled[*regular.Conn]{Conn: regular.NewConn(clientConn, nil)},
	})

	// No reservation, no MaxRows: the plain pooled-connection branch.
	state, err := e.PortalStreamExecute(context.Background(), &query.Target{},
		&query.PreparedStatement{Name: "stmt0", Query: "SELECT 1"},
		&query.Portal{Name: "p0"},
		&query.ExecuteOptions{User: "postgres"},
		nil, nil, noopCallback)
	require.NoError(t, err)
	require.Nil(t, state)

	dp := singleDurationPoint(t, reader)
	assert.Equal(t, poolTypeRegular, attrValue(t, dp.Attributes, "pool_type"))
	assert.Equal(t, "ok", attrValue(t, dp.Attributes, "status"))

	acq := findMetric(t, reader, "mg.pooler.query.pool_acquire.duration")
	acqHist, ok := acq.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, acqHist.DataPoints, 1)
	assert.Equal(t, poolTypeRegular, attrValue(t, acqHist.DataPoints[0].Attributes, "pool_type"))
	assert.Equal(t, "acquired", attrValue(t, acqHist.DataPoints[0].Attributes, "outcome"))
}
