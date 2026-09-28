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
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"

	"github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/test/utils/openapitest"
)

// TestOpenAPIRoutes compares the spec with HTTP annotations in the service descriptor.
func TestOpenAPIRoutes(t *testing.T) {
	doc := openapitest.Load(t)
	model, err := doc.BuildV3Model()
	require.NoError(t, err)
	actual := map[string]string{}
	for path, item := range model.Model.Paths.PathItems.FromOldest() {
		for method, operation := range item.GetOperations().FromOldest() {
			actual[strings.ToUpper(method)+" "+path] = operation.OperationId
			require.Len(t, operation.Tags, 1)
		}
	}
	expected := map[string]string{}
	var addRule func(*annotations.HttpRule, string)
	addRule = func(rule *annotations.HttpRule, operation string) {
		var method, path string
		switch pattern := rule.GetPattern().(type) {
		case *annotations.HttpRule_Get:
			method, path = "GET", pattern.Get
		case *annotations.HttpRule_Post:
			method, path = "POST", pattern.Post
		case *annotations.HttpRule_Put:
			method, path = "PUT", pattern.Put
		case *annotations.HttpRule_Patch:
			method, path = "PATCH", pattern.Patch
		case *annotations.HttpRule_Delete:
			method, path = "DELETE", pattern.Delete
		default:
			t.Fatalf("unsupported HTTP annotation: %T", pattern)
		}
		expected[method+" "+path] = operation
		for _, binding := range rule.AdditionalBindings {
			addRule(binding, operation)
		}
	}
	methods := multiadminpb.File_multiadminservice_proto.Services().ByName("MultiadminService").Methods()
	for i := 0; i < methods.Len(); i++ {
		method := methods.Get(i)
		if proto.HasExtension(method.Options(), annotations.E_Http) {
			rule := proto.GetExtension(method.Options(), annotations.E_Http).(*annotations.HttpRule)
			addRule(rule, string(method.FullName()))
		}
	}
	require.NotEmpty(t, expected)
	assert.Equal(t, expected, actual, "REST routes must exactly match google.api.http annotations")
	t.Logf("all %d annotated REST operations match; no extra operations", len(expected))
	scheme, ok := model.Model.Components.SecuritySchemes.Get("BearerAuth")
	require.True(t, ok)
	assert.Equal(t, "http", scheme.Type)
	assert.Equal(t, "bearer", scheme.Scheme)
	assert.Equal(t, "JWT", scheme.BearerFormat)
	require.Len(t, model.Model.Security, 1)
	_, secured := model.Model.Security[0].Requirements.Get("BearerAuth")
	require.True(t, secured)
}

func TestOpenAPIWireSchemas(t *testing.T) {
	doc := openapitest.Load(t)
	var spec any
	require.NoError(t, yaml.Unmarshal(*doc.GetSpecInfo().SpecBytes, &spec))
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	const schemaURL = "https://multigres.test/openapi"
	require.NoError(t, compiler.AddResource(schemaURL, spec))
	validate := func(t *testing.T, name string, value any, valid bool) {
		t.Helper()
		ref := name
		if !strings.HasPrefix(ref, "#/") {
			ref = "#/components/schemas/" + name
		}
		schema, err := compiler.Compile(schemaURL + ref)
		require.NoError(t, err)
		err = schema.Validate(value)
		if valid {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
	marshaled := func(t *testing.T, msg proto.Message) any {
		t.Helper()
		data, err := protojson.Marshal(msg)
		require.NoError(t, err)
		var value any
		require.NoError(t, json.Unmarshal(data, &value))
		return value
	}
	t.Run("canonical backup JSON", func(t *testing.T) {
		backup := &multiadminpb.BackupInfo{
			BackupSizeBytes: 18446744073709551615,
			Status:          multiadminpb.BackupStatus_BACKUP_STATUS_COMPLETE,
			StartTimestamp:  timestamppb.Now(),
		}
		value := marshaled(t, backup).(map[string]any)
		validate(t, "multiadmin.BackupInfo", value, true)
		value["backupSizeBytes"] = float64(1)
		validate(t, "multiadmin.BackupInfo", value, false)
		value["backupSizeBytes"] = "-1"
		validate(t, "multiadmin.BackupInfo", value, false)
		value["backupSizeBytes"] = "1"
		value["status"] = float64(2)
		validate(t, "multiadmin.BackupInfo", value, false)
		delete(value, "status")
		value["backup_size_bytes"] = "1"
		validate(t, "multiadmin.BackupInfo", value, false)
	})
	t.Run("signed 64-bit integer", func(t *testing.T) {
		validate(t, "clustermetadata.RuleNumber", marshaled(t, &clustermetadata.RuleNumber{CoordinatorTerm: -9223372036854775808, LeaderSubterm: 9223372036854775807}), true)
		validate(t, "clustermetadata.RuleNumber", map[string]any{"coordinatorTerm": float64(1)}, false)
	})
	t.Run("timestamp", func(t *testing.T) {
		validate(t, "google.protobuf.Timestamp", "2000-01-01T00:00:00.123456789Z", true)
		validate(t, "google.protobuf.Timestamp", "yesterday", false)
	})
	t.Run("protobuf duration", func(t *testing.T) {
		validate(t, "google.protobuf.Duration", marshaled(t, &durationpb.Duration{Seconds: 1, Nanos: 500000000}), true)
		validate(t, "google.protobuf.Duration", "PT1S", false)
	})
	t.Run("optional oneof", func(t *testing.T) {
		validate(t, "clustermetadata.BackupLocation", marshaled(t, &clustermetadata.BackupLocation{}), true)
		validate(t, "clustermetadata.BackupLocation", map[string]any{"filesystem": map[string]any{}}, true)
		validate(t, "clustermetadata.BackupLocation", map[string]any{"filesystem": map[string]any{}, "s3": map[string]any{}}, false)
	})
	t.Run("required certificate choice", func(t *testing.T) {
		path := "/api/v1/shards/{shard_key.database}/{shard_key.table_group}/{shard_key.shard}/rule-change"
		ref := "#/paths/" + strings.ReplaceAll(path, "/", "~1") + "/post/requestBody/content/application~1json/schema"
		for _, field := range []string{"cert", "unsafeDeriveCert"} {
			validate(t, ref, map[string]any{field: map[string]any{}, "reason": "test"}, true)
		}
		validate(t, ref, map[string]any{}, false)
		validate(t, ref, map[string]any{"cert": map[string]any{}, "unsafeDeriveCert": map[string]any{}}, false)
	})
	t.Run("required backup selectors", func(t *testing.T) {
		for _, name := range []string{"multiadmin.ExpireBackupsRequest", "multiadmin.VerifyBackupsRequest"} {
			value := map[string]any{"database": "postgres", "tableGroup": "default", "shard": "0"}
			validate(t, name, value, true)
			value["database"] = ""
			validate(t, name, value, false)
			delete(value, "database")
			validate(t, name, value, false)
		}
	})
}

func TestOpenAPIErrorSchema(t *testing.T) {
	v := openapitest.Validator(t)
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{`{"code":5,"message":"cell not found","details":[]}`, true},
		{`{"code":"not_found","message":"cell not found"}`, false},
	} {
		t.Run(strconv.FormatBool(tc.valid), func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://localhost/api/v1/cells/missing", nil)
			require.NoError(t, err)
			resp := &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(tc.body))}
			valid, failures := v.ValidateHttpResponse(req, resp)
			assert.Equal(t, tc.valid, valid, "%+v", failures)
		})
	}
}
