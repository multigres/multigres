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

package multiadmin

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
)

// testMultiAdmin creates a MultiAdmin instance configured for testing
func testMultiAdmin(t *testing.T, cells ...string) *MultiAdmin {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, cells...)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ma := &MultiAdmin{
		adminServer: NewMultiAdminServer(ts, logger),
	}

	return ma
}

// --- Cell Endpoint Tests ---

func TestHTTPAPIGetCellNames(t *testing.T) {
	ma := testMultiAdmin(t)
	ctx := t.Context()

	t.Run("empty topology returns empty list", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/cells", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPICells(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetCellNamesResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Empty(t, resp.Names)
	})

	t.Run("returns all cell names", func(t *testing.T) {
		// Create test cells
		cells := []*clustermetadatapb.Cell{
			{Name: "cell1", ServerAddresses: []string{"localhost:2379"}, Root: "/multigres/cell1"},
			{Name: "cell2", ServerAddresses: []string{"localhost:2380"}, Root: "/multigres/cell2"},
		}

		for _, cell := range cells {
			err := ma.adminServer.ts.CreateCell(ctx, cell.Name, cell)
			require.NoError(t, err)
		}

		req := httptest.NewRequest(http.MethodGet, "/api/v1/cells", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPICells(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetCellNamesResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"cell1", "cell2"}, resp.Names)
	})

	t.Run("method not allowed for POST", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/cells", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPICells(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)

		var errResp ErrorResponse
		err := json.Unmarshal(w.Body.Bytes(), &errResp)
		require.NoError(t, err)
		assert.Equal(t, "InvalidArgument", errResp.Code)
	})
}

func TestHTTPAPIGetCell(t *testing.T) {
	ma := testMultiAdmin(t)
	ctx := t.Context()

	t.Run("non-existent cell returns NotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/cells/nonexistent", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPICellByName(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)

		var errResp ErrorResponse
		err := json.Unmarshal(w.Body.Bytes(), &errResp)
		require.NoError(t, err)
		assert.Equal(t, "NotFound", errResp.Code)
		assert.Contains(t, errResp.Error, "cell 'nonexistent' not found")
	})

	t.Run("existing cell returns cell data", func(t *testing.T) {
		testCell := &clustermetadatapb.Cell{
			Name:            "testcell",
			ServerAddresses: []string{"localhost:2379"},
			Root:            "/multigres/testcell",
		}
		err := ma.adminServer.ts.CreateCell(ctx, "testcell", testCell)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/cells/testcell", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPICellByName(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetCellResponse
		err = protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		require.NotNil(t, resp.Cell)
		assert.Equal(t, "testcell", resp.Cell.Name)
		assert.Equal(t, []string{"localhost:2379"}, resp.Cell.ServerAddresses)
	})
}

// --- Database Endpoint Tests ---

func TestHTTPAPIGetDatabaseNames(t *testing.T) {
	ma := testMultiAdmin(t)
	ctx := t.Context()

	t.Run("empty topology returns empty list", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/databases", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIDatabases(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetDatabaseNamesResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Empty(t, resp.Names)
	})

	t.Run("returns all database names", func(t *testing.T) {
		databases := []*clustermetadatapb.Database{
			{Name: "db1", DurabilityPolicy: "none", Cells: []string{"cell1"}},
			{Name: "db2", DurabilityPolicy: "none", Cells: []string{"cell1"}},
		}

		for _, db := range databases {
			err := ma.adminServer.ts.CreateDatabase(ctx, db.Name, db)
			require.NoError(t, err)
		}

		req := httptest.NewRequest(http.MethodGet, "/api/v1/databases", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIDatabases(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetDatabaseNamesResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"db1", "db2"}, resp.Names)
	})
}

func TestHTTPAPIGetDatabase(t *testing.T) {
	ma := testMultiAdmin(t)
	ctx := t.Context()

	t.Run("non-existent database returns NotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/databases/nonexistent", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIDatabaseByName(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)

		var errResp ErrorResponse
		err := json.Unmarshal(w.Body.Bytes(), &errResp)
		require.NoError(t, err)
		assert.Equal(t, "NotFound", errResp.Code)
		assert.Contains(t, errResp.Error, "database 'nonexistent' not found")
	})

	t.Run("existing database returns database data", func(t *testing.T) {
		testDatabase := &clustermetadatapb.Database{
			Name:             "testdb",
			BackupLocation:   "s3://backup-bucket/testdb",
			DurabilityPolicy: "none",
			Cells:            []string{"cell1", "cell2"},
		}
		err := ma.adminServer.ts.CreateDatabase(ctx, "testdb", testDatabase)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/databases/testdb", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIDatabaseByName(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetDatabaseResponse
		err = protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		require.NotNil(t, resp.Database)
		assert.Equal(t, "testdb", resp.Database.Name)
		assert.Equal(t, "s3://backup-bucket/testdb", resp.Database.BackupLocation)
	})
}

// --- Service Discovery Endpoint Tests ---

func TestHTTPAPIGetGateways(t *testing.T) {
	ma := testMultiAdmin(t)
	ctx := t.Context()

	t.Run("empty topology returns empty list", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/gateways", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIGateways(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetGatewaysResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Empty(t, resp.Gateways)
	})
}

func TestHTTPAPIGetGatewaysMultiCell(t *testing.T) {
	ma := testMultiAdmin(t, "cell1", "cell2")
	ctx := t.Context()

	require.NoError(t, memorytopo.SetupMultiCellTestData(ctx, ma.adminServer.ts))

	t.Run("get all gateways across all cells", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/gateways", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIGateways(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetGatewaysResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Len(t, resp.Gateways, 3) // gw1, gw2, gw3
	})

	t.Run("get gateways filtered by single cell", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/gateways?cells=cell1", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIGateways(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetGatewaysResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Len(t, resp.Gateways, 2) // gw1, gw2
		for _, gw := range resp.Gateways {
			assert.Equal(t, "cell1", gw.Id.Cell)
		}
	})

	t.Run("get gateways filtered by multiple cells", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/gateways?cells=cell1,cell2", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIGateways(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetGatewaysResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Len(t, resp.Gateways, 3) // gw1, gw2, gw3
	})
}

func TestHTTPAPIGetPoolers(t *testing.T) {
	ma := testMultiAdmin(t)
	ctx := t.Context()

	t.Run("empty topology returns empty list", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/poolers", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIPoolers(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetPoolersResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Empty(t, resp.Poolers)
	})
}

func TestHTTPAPIGetPoolersMultiCell(t *testing.T) {
	ma := testMultiAdmin(t, "cell1", "cell2")
	ctx := t.Context()

	require.NoError(t, memorytopo.SetupMultiCellTestData(ctx, ma.adminServer.ts))

	t.Run("get all poolers across all cells", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/poolers", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIPoolers(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetPoolersResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Len(t, resp.Poolers, 3) // pool1, pool2, pool3
	})

	t.Run("get poolers filtered by single cell", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/poolers?cells=cell2", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIPoolers(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetPoolersResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Len(t, resp.Poolers, 2) // pool2, pool3
		for _, pooler := range resp.Poolers {
			assert.Equal(t, "cell2", pooler.Id.Cell)
		}
	})

	t.Run("get poolers filtered by database", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/poolers?database=db1", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIPoolers(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetPoolersResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Len(t, resp.Poolers, 2) // pool1 and pool2
		for _, pooler := range resp.Poolers {
			assert.Equal(t, "db1", pooler.Database)
		}
	})

	t.Run("get poolers filtered by cell and database", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/poolers?cells=cell2&database=db1", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIPoolers(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetPoolersResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Len(t, resp.Poolers, 1) // only pool2
		assert.Equal(t, "cell2", resp.Poolers[0].Id.Cell)
		assert.Equal(t, "db1", resp.Poolers[0].Database)
	})
}

func TestHTTPAPIGetOrchestrators(t *testing.T) {
	ma := testMultiAdmin(t)
	ctx := t.Context()

	t.Run("empty topology returns empty list", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/orchs", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIOrchestrators(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetOrchsResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Empty(t, resp.Orchs)
	})
}

func TestHTTPAPIGetOrchestratorsMultiCell(t *testing.T) {
	ma := testMultiAdmin(t, "cell1", "cell2")
	ctx := t.Context()

	require.NoError(t, memorytopo.SetupMultiCellTestData(ctx, ma.adminServer.ts))

	t.Run("get all orchestrators across all cells", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/orchs", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIOrchestrators(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetOrchsResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Len(t, resp.Orchs, 2) // orch1, orch2
	})

	t.Run("get orchestrators filtered by single cell", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/orchs?cells=cell1", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIOrchestrators(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp multiadminpb.GetOrchsResponse
		err := protojson.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Len(t, resp.Orchs, 1) // orch1
		assert.Equal(t, "cell1", resp.Orchs[0].Id.Cell)
		assert.Equal(t, "orch1", resp.Orchs[0].Id.Name)
	})
}

// --- Backup Endpoint Tests ---

func TestHTTPAPIGetBackups(t *testing.T) {
	ma := testMultiAdmin(t, "cell1")
	ctx := t.Context()

	t.Run("missing required database returns error", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/backups", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIBackups(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)

		var errResp ErrorResponse
		err := json.Unmarshal(w.Body.Bytes(), &errResp)
		require.NoError(t, err)
		assert.Contains(t, errResp.Error, "database cannot be empty")
	})

	t.Run("missing table_group returns error", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/backups?database=db1", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIBackups(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)

		var errResp ErrorResponse
		err := json.Unmarshal(w.Body.Bytes(), &errResp)
		require.NoError(t, err)
		assert.Contains(t, errResp.Error, "table_group cannot be empty")
	})

	t.Run("invalid limit returns error", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/backups?database=db1&table_group=default&limit=invalid", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIBackups(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)

		var errResp ErrorResponse
		err := json.Unmarshal(w.Body.Bytes(), &errResp)
		require.NoError(t, err)
		assert.Equal(t, "InvalidArgument", errResp.Code)
		assert.Contains(t, errResp.Error, "invalid limit parameter")
	})
}

func TestHTTPAPICreateBackup(t *testing.T) {
	ma := testMultiAdmin(t, "cell1")
	ctx := t.Context()

	t.Run("missing required fields returns error", func(t *testing.T) {
		body := bytes.NewBufferString(`{}`)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/backups", body)
		req = req.WithContext(ctx)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		ma.handleAPIBackups(w, req)

		// Should fail because no poolers are available for the backup
		assert.NotEqual(t, http.StatusOK, w.Code)
	})

	t.Run("invalid JSON returns error", func(t *testing.T) {
		body := bytes.NewBufferString(`{invalid}`)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/backups", body)
		req = req.WithContext(ctx)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		ma.handleAPIBackups(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)

		var errResp ErrorResponse
		err := json.Unmarshal(w.Body.Bytes(), &errResp)
		require.NoError(t, err)
		assert.Equal(t, "InvalidArgument", errResp.Code)
		assert.Contains(t, errResp.Error, "invalid request body")
	})
}

func TestHTTPAPIRestores(t *testing.T) {
	ma := testMultiAdmin(t, "cell1")
	ctx := t.Context()

	t.Run("method not allowed for GET", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/restores", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIRestores(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})

	t.Run("invalid JSON returns error", func(t *testing.T) {
		body := bytes.NewBufferString(`{invalid}`)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/restores", body)
		req = req.WithContext(ctx)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		ma.handleAPIRestores(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)

		var errResp ErrorResponse
		err := json.Unmarshal(w.Body.Bytes(), &errResp)
		require.NoError(t, err)
		assert.Equal(t, "InvalidArgument", errResp.Code)
	})
}

func TestHTTPAPIJobStatus(t *testing.T) {
	ma := testMultiAdmin(t, "cell1")
	ctx := t.Context()

	t.Run("method not allowed for POST", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/job123", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIJobStatus(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})

	t.Run("non-existent job returns not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/nonexistent", nil)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()

		ma.handleAPIJobStatus(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)

		var errResp ErrorResponse
		err := json.Unmarshal(w.Body.Bytes(), &errResp)
		require.NoError(t, err)
		assert.Equal(t, "NotFound", errResp.Code)
	})
}

// --- Helper Function Tests ---

func TestExtractPathParam(t *testing.T) {
	tests := []struct {
		path     string
		prefix   string
		expected string
	}{
		{"/api/v1/cells/zone1", "/api/v1/cells/", "zone1"},
		{"/api/v1/databases/mydb", "/api/v1/databases/", "mydb"},
		{"/api/v1/jobs/abc123", "/api/v1/jobs/", "abc123"},
		{"/api/v1/cells/", "/api/v1/cells/", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := extractPathParam(tt.path, tt.prefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseCellsParam(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected []string
	}{
		{"empty", "/api/v1/gateways", nil},
		{"single cell", "/api/v1/gateways?cells=cell1", []string{"cell1"}},
		{"multiple cells", "/api/v1/gateways?cells=cell1,cell2,cell3", []string{"cell1", "cell2", "cell3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			result := parseCellsParam(req)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGrpcCodeToHTTP(t *testing.T) {
	tests := []struct {
		name     string
		code     int
		expected int
	}{
		{"OK", 0, http.StatusOK},
		{"InvalidArgument", 3, http.StatusBadRequest},
		{"NotFound", 5, http.StatusNotFound},
		{"AlreadyExists", 6, http.StatusConflict},
		{"PermissionDenied", 7, http.StatusForbidden},
		{"Unauthenticated", 16, http.StatusUnauthorized},
		{"ResourceExhausted", 8, http.StatusTooManyRequests},
		{"Unimplemented", 12, http.StatusNotImplemented},
		{"Unavailable", 14, http.StatusServiceUnavailable},
		{"Internal", 13, http.StatusInternalServerError},
		{"Unknown", 2, http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Import codes package to use proper enum
			result := grpcCodeToHTTP(codes.Code(tt.code))
			assert.Equal(t, tt.expected, result)
		})
	}
}
