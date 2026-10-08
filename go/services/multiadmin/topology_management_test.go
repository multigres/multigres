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

package multiadmin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"connectrpc.com/vanguard"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/servenv/servenvtest"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/pb/multiadmin/multiadminconnect"
	"github.com/multigres/multigres/go/test/utils/openapitest"
)

func managementServer(t *testing.T, store topoclient.Store) *MultiadminServer {
	t.Helper()
	return NewMultiadminServer(store, slog.New(slog.DiscardHandler), grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func initialCell() *clustermetadatapb.Cell {
	return &clustermetadatapb.Cell{Name: "zone1", ServerAddresses: []string{"localhost:2379"}, Root: "/cells/zone1", Metadata: `{"region":"test"}`}
}

func initialDatabase() *clustermetadatapb.Database {
	return &clustermetadatapb.Database{Name: "postgres", Cells: []string{"zone1"}, BootstrapDurabilityPolicy: topoclient.AtLeastN(2), BackupLocation: &clustermetadatapb.BackupLocation{Location: &clustermetadatapb.BackupLocation_S3{S3: &clustermetadatapb.S3Backup{Bucket: "backups", Region: "us-east-1", UseEnvCredentials: true}}}}
}

func TestManagementCreateAdopt(t *testing.T) {
	ts := memorytopo.NewServer(t.Context())
	t.Cleanup(func() { require.NoError(t, ts.Close()) })
	srv := managementServer(t, ts)
	for range 2 {
		cell, err := srv.CreateCell(t.Context(), &multiadminpb.CreateCellRequest{Cell: initialCell()})
		require.NoError(t, err)
		require.True(t, proto.Equal(initialCell(), cell.Cell))
		database, err := srv.CreateDatabase(t.Context(), &multiadminpb.CreateDatabaseRequest{Database: initialDatabase()})
		require.NoError(t, err)
		require.True(t, proto.Equal(initialDatabase(), database.Database))
	}
	cell, err := srv.GetCell(t.Context(), &multiadminpb.GetCellRequest{Name: "zone1"})
	require.NoError(t, err)
	require.True(t, proto.Equal(initialCell(), cell.Cell))
	database, err := srv.GetDatabase(t.Context(), &multiadminpb.GetDatabaseRequest{Name: "postgres"})
	require.NoError(t, err)
	require.True(t, proto.Equal(initialDatabase(), database.Database))
	conflictCell := initialCell()
	conflictCell.Root = "/elsewhere"
	_, err = srv.CreateCell(t.Context(), &multiadminpb.CreateCellRequest{Cell: conflictCell})
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	for _, change := range []func(*clustermetadatapb.Database){
		func(db *clustermetadatapb.Database) { db.BootstrapDurabilityPolicy = topoclient.AtLeastN(3) },
		func(db *clustermetadatapb.Database) { db.BackupLocation.GetS3().Bucket = "other" },
	} {
		conflict := initialDatabase()
		change(conflict)
		_, err = srv.CreateDatabase(t.Context(), &multiadminpb.CreateDatabaseRequest{Database: conflict})
		require.Equal(t, codes.AlreadyExists, status.Code(err))
	}
	stored, err := ts.GetDatabase(t.Context(), "postgres")
	require.NoError(t, err)
	require.True(t, proto.Equal(initialDatabase(), stored))
}

func TestManagementConcurrentCreates(t *testing.T) {
	ts := memorytopo.NewServer(t.Context())
	t.Cleanup(func() { require.NoError(t, ts.Close()) })
	srv := managementServer(t, ts)
	var wg sync.WaitGroup
	results := make(chan error, 16)
	for range 16 {
		wg.Go(func() {
			_, err := srv.CreateCell(t.Context(), &multiadminpb.CreateCellRequest{Cell: initialCell()})
			results <- err
		})
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
}

func TestManagementInvalidConfiguration(t *testing.T) {
	ts := memorytopo.NewServer(t.Context())
	t.Cleanup(func() { require.NoError(t, ts.Close()) })
	srv := managementServer(t, ts)
	for _, name := range []string{"", "..", "../other", "a/b", "global"} {
		cell := initialCell()
		cell.Name = name
		_, err := srv.CreateCell(t.Context(), &multiadminpb.CreateCellRequest{Cell: cell})
		require.Equal(t, codes.InvalidArgument, status.Code(err), name)
	}
	_, err := srv.CreateDatabase(t.Context(), &multiadminpb.CreateDatabaseRequest{Database: initialDatabase()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = srv.CreateCell(t.Context(), &multiadminpb.CreateCellRequest{Cell: initialCell()})
	require.NoError(t, err)
	for _, change := range []func(*clustermetadatapb.Database){
		func(db *clustermetadatapb.Database) { db.Name = "../other" },
		func(db *clustermetadatapb.Database) { db.Cells = nil },
		func(db *clustermetadatapb.Database) { db.Cells = []string{"zone1", "zone1"} },
		func(db *clustermetadatapb.Database) { db.BootstrapDurabilityPolicy = nil },
		func(db *clustermetadatapb.Database) { db.BootstrapDurabilityPolicy = topoclient.AtLeastN(0) },
		func(db *clustermetadatapb.Database) { db.BootstrapDurabilityPolicy = topoclient.MultiCellAtLeastN(3) },
		func(db *clustermetadatapb.Database) { db.BackupLocation.GetS3().Bucket = "" },
		func(db *clustermetadatapb.Database) {
			db.BackupLocation.GetS3().Endpoint = "https://user:secret@example.test"
		},
		func(db *clustermetadatapb.Database) { db.BackupLocation.AuthoritativeGeneration = 2 },
	} {
		db := initialDatabase()
		change(db)
		_, err := srv.CreateDatabase(t.Context(), &multiadminpb.CreateDatabaseRequest{Database: db})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	for _, call := range []func() error{
		func() error { _, err := srv.CreateCell(t.Context(), nil); return err },
		func() error { _, err := srv.CreateDatabase(t.Context(), nil); return err },
		func() error { _, err := srv.RetirePooler(t.Context(), nil); return err },
		func() error { _, err := srv.GetPoolerRegistration(t.Context(), nil); return err },
	} {
		require.Equal(t, codes.InvalidArgument, status.Code(call()))
	}
}

func registrationFixture(t *testing.T) (topoclient.Store, *MultiadminServer, *multiadminpb.RetirePoolerRequest) {
	t.Helper()
	ts := memorytopo.NewServer(t.Context(), "zone1")
	t.Cleanup(func() { require.NoError(t, ts.Close()) })
	pooler := topoclient.NewMultipooler("member1", "zone1", "localhost")
	pooler.ShardKey = &clustermetadatapb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0-inf"}
	require.NoError(t, ts.CreateMultipooler(t.Context(), pooler))
	srv := managementServer(t, ts)
	observed, err := srv.GetPoolerRegistration(t.Context(), &multiadminpb.GetPoolerRegistrationRequest{PoolerId: pooler.Id})
	require.NoError(t, err)
	return ts, srv, &multiadminpb.RetirePoolerRequest{PoolerId: pooler.Id, ShardKey: pooler.ShardKey, IncarnationId: pooler.IncarnationId, Version: observed.Version, FencingAcknowledged: true}
}

func TestManagementRetirePreconditions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*multiadminpb.RetirePoolerRequest)
		code   codes.Code
	}{
		{"unfenced", func(r *multiadminpb.RetirePoolerRequest) { r.FencingAcknowledged = false }, codes.FailedPrecondition},
		{"no incarnation", func(r *multiadminpb.RetirePoolerRequest) { r.IncarnationId = "" }, codes.InvalidArgument},
		{"no version", func(r *multiadminpb.RetirePoolerRequest) { r.Version = "" }, codes.InvalidArgument},
		{"no shard", func(r *multiadminpb.RetirePoolerRequest) { r.ShardKey = nil }, codes.InvalidArgument},
		{"wrong component", func(r *multiadminpb.RetirePoolerRequest) { r.PoolerId.Component = clustermetadatapb.ID_MULTIGATEWAY }, codes.InvalidArgument},
		{"wrong shard", func(r *multiadminpb.RetirePoolerRequest) { r.ShardKey.Shard = "80-" }, codes.Aborted},
		{"stale incarnation", func(r *multiadminpb.RetirePoolerRequest) { r.IncarnationId = "old-process" }, codes.Aborted},
		{"stale version", func(r *multiadminpb.RetirePoolerRequest) { r.Version = "old-version" }, codes.Aborted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, srv, req := registrationFixture(t)
			originalID := proto.Clone(req.PoolerId).(*clustermetadatapb.ID)
			tc.change(req)
			_, err := srv.RetirePooler(t.Context(), req)
			require.Equal(t, tc.code, status.Code(err))
			_, err = ts.GetMultipooler(t.Context(), originalID)
			require.NoError(t, err)
		})
	}
}

func TestManagementRetireRepeatAndReplacement(t *testing.T) {
	ts, srv, req := registrationFixture(t)
	for range 2 {
		_, err := srv.RetirePooler(t.Context(), req)
		require.NoError(t, err)
	}
	_, err := srv.GetPoolerRegistration(t.Context(), &multiadminpb.GetPoolerRegistrationRequest{PoolerId: req.PoolerId})
	require.Equal(t, codes.NotFound, status.Code(err))
	replacement := topoclient.NewMultipooler(req.PoolerId.Name, req.PoolerId.Cell, "localhost")
	replacement.ShardKey = req.ShardKey
	require.NotEqual(t, req.IncarnationId, replacement.IncarnationId)
	require.NoError(t, ts.CreateMultipooler(t.Context(), replacement))
	_, err = srv.RetirePooler(t.Context(), req)
	require.Equal(t, codes.Aborted, status.Code(err))
	observed, err := srv.GetPoolerRegistration(t.Context(), &multiadminpb.GetPoolerRegistrationRequest{PoolerId: req.PoolerId})
	require.NoError(t, err)
	require.Equal(t, replacement.IncarnationId, observed.Pooler.IncarnationId)
}

func TestManagementRetireLegacyRegistration(t *testing.T) {
	ts, srv, req := registrationFixture(t)
	_, err := ts.UpdateMultipoolerFields(t.Context(), req.PoolerId, func(mp *clustermetadatapb.Multipooler) error { mp.IncarnationId = ""; return nil })
	require.NoError(t, err)
	_, err = srv.RetirePooler(t.Context(), req)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

type managementConnStore struct {
	topoclient.Store
	wrap func(topoclient.Conn) topoclient.Conn
	err  error
}

func (s *managementConnStore) ConnForCell(ctx context.Context, cell string) (topoclient.Conn, error) {
	if s.err != nil {
		return nil, s.err
	}
	conn, err := s.Store.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}
	return s.wrap(conn), nil
}

type retirementConn struct {
	topoclient.Conn
	before     func(context.Context, string)
	afterError error
}

func (c *retirementConn) Delete(ctx context.Context, p string, v topoclient.Version) error {
	if c.before != nil {
		c.before(ctx, p)
	}
	if err := c.Conn.Delete(ctx, p, v); err != nil {
		return err
	}
	return c.afterError
}

func TestManagementRetireConcurrentRegistration(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(map[bool]string{false: "update", true: "delete and recreate"}[recreate], func(t *testing.T) {
			ts, _, req := registrationFixture(t)
			wrapped := &managementConnStore{Store: ts, wrap: func(conn topoclient.Conn) topoclient.Conn {
				return &retirementConn{Conn: conn, before: func(ctx context.Context, p string) {
					data, _, err := conn.Get(ctx, p)
					require.NoError(t, err)
					if recreate {
						require.NoError(t, conn.Delete(ctx, p, nil))
						_, err = conn.Create(ctx, p, data)
					} else {
						_, err = conn.Update(ctx, p, data, nil)
					}
					require.NoError(t, err)
				}}
			}}
			_, err := managementServer(t, wrapped).RetirePooler(t.Context(), req)
			require.Equal(t, codes.Aborted, status.Code(err))
			_, err = ts.GetMultipooler(t.Context(), req.PoolerId)
			require.NoError(t, err)
		})
	}
}

func TestManagementRetireUncertainOutcome(t *testing.T) {
	ts, _, req := registrationFixture(t)
	wrapped := &managementConnStore{Store: ts, wrap: func(conn topoclient.Conn) topoclient.Conn {
		return &retirementConn{Conn: conn, afterError: context.DeadlineExceeded}
	}}
	srv := managementServer(t, wrapped)
	_, err := srv.RetirePooler(t.Context(), req)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	_, err = srv.GetPoolerRegistration(t.Context(), &multiadminpb.GetPoolerRegistrationRequest{PoolerId: req.PoolerId})
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = srv.RetirePooler(t.Context(), req)
	require.NoError(t, err)
	// Recovery does not depend on in-memory request state.
	_, err = managementServer(t, ts).RetirePooler(t.Context(), req)
	require.NoError(t, err)
}

func TestManagementRegistrationDependencyErrors(t *testing.T) {
	ts, _, req := registrationFixture(t)
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{errors.New("connection failed"), codes.Unavailable},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
		{context.Canceled, codes.Canceled},
		{topoclient.NewError(topoclient.NoNode, "zone1"), codes.FailedPrecondition},
	} {
		srv := managementServer(t, &managementConnStore{Store: ts, err: tc.err})
		_, err := srv.GetPoolerRegistration(t.Context(), &multiadminpb.GetPoolerRegistrationRequest{PoolerId: req.PoolerId})
		require.Equal(t, tc.code, status.Code(err))
		_, err = srv.RetirePooler(t.Context(), req)
		require.Equal(t, tc.code, status.Code(err))
	}
}

type createLostAckStore struct{ topoclient.Store }

func (s *createLostAckStore) CreateCell(ctx context.Context, name string, cell *clustermetadatapb.Cell) error {
	if err := s.Store.CreateCell(ctx, name, cell); err != nil {
		return err
	}
	return context.DeadlineExceeded
}

func (s *createLostAckStore) CreateDatabase(ctx context.Context, name string, db *clustermetadatapb.Database) error {
	if err := s.Store.CreateDatabase(ctx, name, db); err != nil {
		return err
	}
	return context.DeadlineExceeded
}

func TestManagementCreateUncertainOutcome(t *testing.T) {
	ts := memorytopo.NewServer(t.Context())
	t.Cleanup(func() { require.NoError(t, ts.Close()) })
	srv := managementServer(t, &createLostAckStore{Store: ts})
	_, err := srv.CreateCell(t.Context(), &multiadminpb.CreateCellRequest{Cell: initialCell()})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	_, err = srv.CreateCell(t.Context(), &multiadminpb.CreateCellRequest{Cell: initialCell()})
	require.NoError(t, err)
	_, err = srv.CreateDatabase(t.Context(), &multiadminpb.CreateDatabaseRequest{Database: initialDatabase()})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	_, err = srv.CreateDatabase(t.Context(), &multiadminpb.CreateDatabaseRequest{Database: initialDatabase()})
	require.NoError(t, err)
}

func TestManagementHTTPAndAuthentication(t *testing.T) {
	ts, srv, retire := registrationFixture(t)
	service, handler := newConnectHandler(srv, func() servenv.Authenticator { return &servenvtest.FakeTokenVerifier{ValidToken: "valid-token"} })
	transcoder, err := vanguard.NewTranscoder([]*vanguard.Service{vanguard.NewService(service, handler)})
	require.NoError(t, err)
	mux := http.NewServeMux()
	mux.Handle(service, handler)
	mux.Handle("/api/", transcoder)
	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)
	contract := openapitest.Validator(t)
	call := func(method, route, body, token string, want int) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, httpServer.URL+route, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := httpServer.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		valid, failures := contract.ValidateHttpResponse(req, resp)
		require.True(t, valid, "%+v", failures)
		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, want, resp.StatusCode, string(data))
		return data
	}
	retireJSON, err := protojson.Marshal(retire)
	require.NoError(t, err)
	cellJSON := `{"name":"newzone","serverAddresses":["localhost:2379"],"root":"/cells/newzone"}`
	db := initialDatabase()
	db.Name = "newdb"
	dbJSON, err := protojson.Marshal(db)
	require.NoError(t, err)
	registrationRoute := "/api/v1/poolers/zone1/member1/registration"
	retireRoute := "/api/v1/poolers/zone1/member1/retire"
	for _, tc := range []struct{ method, route, body string }{
		{"POST", "/api/v1/cells", cellJSON},
		{"POST", "/api/v1/databases", string(dbJSON)},
		{"GET", registrationRoute, ""},
		{"POST", retireRoute, string(retireJSON)},
	} {
		for _, token := range []string{"", "invalid"} {
			call(tc.method, tc.route, tc.body, token, http.StatusUnauthorized)
		}
	}
	_, err = ts.GetMultipooler(t.Context(), retire.PoolerId)
	require.NoError(t, err)
	client := multiadminconnect.NewMultiadminServiceClient(httpServer.Client(), httpServer.URL)
	_, err = client.CreateCell(t.Context(), connect.NewRequest(&multiadminpb.CreateCellRequest{Cell: initialCell()}))
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
	_, err = client.RetirePooler(t.Context(), connect.NewRequest(retire))
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
	for range 2 {
		call("POST", "/api/v1/cells", cellJSON, "valid-token", http.StatusOK)
		call("POST", "/api/v1/databases", string(dbJSON), "valid-token", http.StatusOK)
	}
	call("POST", "/api/v1/cells", `{"name":"newzone","serverAddresses":["localhost:2379"],"root":"/changed"}`, "valid-token", http.StatusConflict)
	read := call("GET", registrationRoute, "", "valid-token", http.StatusOK)
	var observed multiadminpb.GetPoolerRegistrationResponse
	require.NoError(t, protojson.Unmarshal(read, &observed))
	require.Equal(t, retire.IncarnationId, observed.Pooler.IncarnationId)
	stale := proto.Clone(retire).(*multiadminpb.RetirePoolerRequest)
	stale.Version = "stale"
	staleJSON, err := protojson.Marshal(stale)
	require.NoError(t, err)
	call("POST", retireRoute, string(staleJSON), "valid-token", http.StatusConflict)
	for range 2 {
		call("POST", retireRoute, string(retireJSON), "valid-token", http.StatusOK)
	}
	call("GET", registrationRoute, "", "valid-token", http.StatusNotFound)
}

func TestManagementAdoptsLegacyNames(t *testing.T) {
	ts := memorytopo.NewServer(t.Context())
	t.Cleanup(func() { require.NoError(t, ts.Close()) })
	cell := initialCell()
	cell.Name = ""
	require.NoError(t, ts.CreateCell(t.Context(), "zone1", cell))
	db := initialDatabase()
	db.Name = ""
	require.NoError(t, ts.CreateDatabase(t.Context(), "postgres", db))
	srv := managementServer(t, ts)
	_, err := srv.CreateCell(t.Context(), &multiadminpb.CreateCellRequest{Cell: initialCell()})
	require.NoError(t, err)
	_, err = srv.CreateDatabase(t.Context(), &multiadminpb.CreateDatabaseRequest{Database: initialDatabase()})
	require.NoError(t, err)
	gotCell, err := srv.GetCell(t.Context(), &multiadminpb.GetCellRequest{Name: "zone1"})
	require.NoError(t, err)
	require.True(t, proto.Equal(initialCell(), gotCell.Cell))
	gotDB, err := srv.GetDatabase(t.Context(), &multiadminpb.GetDatabaseRequest{Name: "postgres"})
	require.NoError(t, err)
	require.True(t, proto.Equal(initialDatabase(), gotDB.Database))
	stored, err := ts.GetCell(t.Context(), "zone1")
	require.NoError(t, err)
	require.Empty(t, stored.Name, "adoption must not rewrite the record")
}

func TestManagementLostHTTPResponse(t *testing.T) {
	_, srv, retire := registrationFixture(t)
	service, handler := newConnectHandler(srv, func() servenv.Authenticator { return nil })
	transcoder, err := vanguard.NewTranscoder([]*vanguard.Service{vanguard.NewService(service, handler)})
	require.NoError(t, err)
	var drop sync.Once
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := httptest.NewRecorder()
		transcoder.ServeHTTP(response, r)
		discard := false
		drop.Do(func() { discard = true })
		if discard {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		maps.Copy(w.Header(), response.Header())
		w.WriteHeader(response.Code)
		_, _ = w.Write(response.Body.Bytes())
	}))
	t.Cleanup(httpServer.Close)
	body, err := protojson.Marshal(retire)
	require.NoError(t, err)
	url := httpServer.URL + "/api/v1/poolers/zone1/member1/retire"
	send := func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader(string(body)))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		return httpServer.Client().Do(req)
	}
	lostResponse, err := send()
	if lostResponse != nil {
		_ = lostResponse.Body.Close()
	}
	require.Error(t, err)
	_, err = srv.GetPoolerRegistration(t.Context(), &multiadminpb.GetPoolerRegistrationRequest{PoolerId: retire.PoolerId})
	require.Equal(t, codes.NotFound, status.Code(err))
	response, err := send()
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
}

func TestManagementRequestSchemas(t *testing.T) {
	doc := openapitest.Load(t)
	var spec any
	require.NoError(t, yaml.Unmarshal(*doc.GetSpecInfo().SpecBytes, &spec))
	compiler := jsonschema.NewCompiler()
	require.NoError(t, compiler.AddResource("https://multigres.test/openapi", spec))
	for _, tc := range []struct{ name, ref, valid, invalid string }{
		{"cell", "#/paths/~1api~1v1~1cells/post/requestBody/content/application~1json/schema", `{"name":"zone1","serverAddresses":["localhost:2379"],"root":"/cells/zone1"}`, `{"name":"zone1"}`},
		{"database", "#/paths/~1api~1v1~1databases/post/requestBody/content/application~1json/schema", `{"name":"postgres","cells":["zone1"],"bootstrapDurabilityPolicy":{"quorumType":"QUORUM_TYPE_AT_LEAST_N","requiredCount":2}}`, `{"name":"postgres","cells":[]}`},
		{"retirement", "#/paths/~1api~1v1~1poolers~1{pooler_id.cell}~1{pooler_id.name}~1retire/post/requestBody/content/application~1json/schema", `{"shardKey":{"database":"postgres","tableGroup":"default","shard":"0-inf"},"incarnationId":"process-uuid","version":"42","fencingAcknowledged":true}`, `{"shardKey":{"database":"postgres"},"incarnationId":"","version":"42","fencingAcknowledged":false}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := compiler.Compile("https://multigres.test/openapi" + tc.ref)
			require.NoError(t, err)
			var valid, invalid any
			require.NoError(t, json.Unmarshal([]byte(tc.valid), &valid))
			require.NoError(t, json.Unmarshal([]byte(tc.invalid), &invalid))
			require.NoError(t, schema.Validate(valid))
			require.Error(t, schema.Validate(invalid))
		})
	}
}
