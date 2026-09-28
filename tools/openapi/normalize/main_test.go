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

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestNormalizeWireValues(t *testing.T) {
	for _, tc := range []struct {
		name, input, ref string
		valid, invalid   []any
	}{
		{"signed integer", `{"type":"integer","format":"int64"}`, "", []any{"-9223372036854775808", "9223372036854775807"}, []any{float64(1), "text"}},
		{"unsigned integer", `{"description":"Count (proto uint64)","type":"integer","format":"int64"}`, "", []any{"0", "18446744073709551615"}, []any{"-1", float64(1)}},
		{"unsigned parameter", `{"description":"(proto uint64)","schema":{"type":"integer","format":"int64"}}`, "/schema", []any{"0", "18446744073709551615"}, []any{"-1", "text"}},
		{"unsigned array", `{"description":"(proto fixed64)","type":"array","items":{"type":"integer","format":"int64"}}`, "", []any{[]any{"0", "18446744073709551615"}}, []any{[]any{"-1"}}},
		{"unsigned map", `{"description":"(proto uint64)","type":"object","additionalProperties":{"type":"integer","format":"int64"}}`, "", []any{map[string]any{"count": "0"}}, []any{map[string]any{"count": "-1"}}},
		{"duration", `{"type":"string","format":"duration"}`, "", []any{"1.500s", "-0.000000001s"}, []any{"PT1S", "1.1234567890s"}},
		{"optional oneof", `{"type":"object","additionalProperties":false,"oneOf":[{"properties":{"a":{"type":"string"}},"required":["a"]},{"properties":{"b":{"type":"string"}},"required":["b"]}]}`, "", []any{map[string]any{}, map[string]any{"a": "x"}, map[string]any{"b": "y"}}, []any{map[string]any{"a": "x", "b": "y"}, map[string]any{"other": "x"}}},
		{"allOf fields", `{"type":"object","additionalProperties":false,"allOf":[{"properties":{"a":{"type":"string"}}},{"properties":{"b":{"type":"string"}}}]}`, "", []any{map[string]any{"a": "x", "b": "y"}}, []any{map[string]any{"other": "x"}}},
		{"ordinary oneof", `{"oneOf":[{"type":"string"},{"type":"number"}]}`, "", []any{"x", float64(1)}, []any{nil, map[string]any{}}},
		{"sibling type hints", `{"type":"object","properties":{"unsigned":{"description":"(proto uint64)","type":"integer","format":"int64"},"signed":{"type":"integer","format":"int64"}}}`, "", []any{map[string]any{"unsigned": "0", "signed": "-1"}}, []any{map[string]any{"unsigned": "-1", "signed": "0"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var schema any
			require.NoError(t, json.Unmarshal([]byte(tc.input), &schema))
			normalize(schema, "")
			compiler := jsonschema.NewCompiler()
			compiler.AssertFormat()
			require.NoError(t, compiler.AddResource("https://multigres.test/schema", schema))
			compiled, err := compiler.Compile("https://multigres.test/schema#" + tc.ref)
			require.NoError(t, err)
			for _, v := range tc.valid {
				require.NoError(t, compiled.Validate(v), "value: %#v", v)
			}
			for _, v := range tc.invalid {
				require.Error(t, compiled.Validate(v), "value: %#v", v)
			}
		})
	}
}

func TestRunReachableSchemas(t *testing.T) {
	dir := t.TempDir()
	input, output := filepath.Join(dir, "input.json"), filepath.Join(dir, "output.yaml")
	const source = `{
 "openapi":"3.1.0","info":{"title":"Test","version":"v1"},"paths":{},
 "x-extra":true,
 "components":{
  "responses":{"Error":{"content":{"application/json":{"schema":{"$ref":"#/components/schemas/A"}}}}},
  "schemas":{
   "A":{"type":"object","properties":{"nested":{"$ref":"#/components/schemas/B"}}},
   "B":{"type":"object","properties":{"parent":{"$ref":"#/components/schemas/A"},"count":{"description":"Count (proto uint64)","type":"integer","format":"int64"}}},
   "Unused":{"type":"string"}
  }
 }} `
	require.NoError(t, os.WriteFile(input, []byte(source), 0o600))
	require.NoError(t, run([]string{input, output}))
	first, err := os.ReadFile(output)
	require.NoError(t, err)
	var spec map[string]any
	require.NoError(t, yaml.Unmarshal(first, &spec))
	schemas := spec["components"].(map[string]any)["schemas"].(map[string]any)
	require.Len(t, schemas, 2)
	require.Contains(t, schemas, "A")
	require.Contains(t, schemas, "B")
	require.NotContains(t, schemas, "Unused")
	require.NotContains(t, string(first), "(proto uint64)")
	require.Contains(t, string(first), "description: Count")
	require.True(t, strings.Index(string(first), "openapi:") < strings.Index(string(first), "components:"))
	require.True(t, strings.Index(string(first), "components:") < strings.Index(string(first), "x-extra:"))
	require.NoError(t, run([]string{input, output}))
	second, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestRunErrors(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.json")
	require.ErrorContains(t, run(nil), "usage:")
	require.Error(t, run([]string{filepath.Join(dir, "missing.json"), filepath.Join(dir, "out.yaml")}))
	require.NoError(t, os.WriteFile(input, []byte(`{`), 0o600))
	require.Error(t, run([]string{input, filepath.Join(dir, "out.yaml")}))
	require.NoError(t, os.WriteFile(input, []byte(`{"components":{"schemas":{}}}`), 0o600))
	require.Error(t, run([]string{input, dir}))
}
