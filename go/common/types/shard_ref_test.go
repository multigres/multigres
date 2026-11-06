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

package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multiorchpb "github.com/multigres/multigres/go/pb/multiorch"
	querypb "github.com/multigres/multigres/go/pb/query"
)

func TestFormatShardRef(t *testing.T) {
	assert.Equal(t, ShardRefString(""), FormatShardRef(nil))
	assert.Equal(t, ShardRefString("db/tg/0"), FormatShardRef(&clustermetadatapb.ShardRef{Database: "db", TableGroup: "tg", Shard: "0"}))
}

// TestShardRefJSONName pins the protojson name of every field typed
// clustermetadata.ShardRef to "shardRef". HTTP gateways, multiadmin's OpenAPI
// spec and CLI JSON output expose it, so renaming it is an API change.
func TestShardRefJSONName(t *testing.T) {
	ref := &clustermetadatapb.ShardRef{Database: "db", TableGroup: "tg", Shard: "0"}
	tests := []struct {
		name string
		msg  proto.Message
	}{
		{"Multipooler", &clustermetadatapb.Multipooler{ShardRef: ref}},
		{"query.Target", &querypb.Target{ShardRef: ref}},
		{"multiorch.ShardStatusRequest", &multiorchpb.ShardStatusRequest{ShardRef: ref}},
		{"multiorch.DetectedProblem", &multiorchpb.DetectedProblem{ShardRef: ref}},
		{"multiorch.ApplyCertifiedRuleChangeRequest", &multiorchpb.ApplyCertifiedRuleChangeRequest{ShardRef: ref}},
		{"multiadmin.ApplyCertifiedRuleChangeRequest", &multiadminpb.ApplyCertifiedRuleChangeRequest{ShardRef: ref}},
		{"multiadmin.SwitchPrimaryRequest", &multiadminpb.SwitchPrimaryRequest{ShardRef: ref}},
	}
	const want = `{"shardRef":{"database":"db","tableGroup":"tg","shard":"0"}}`
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := protojson.Marshal(tt.msg)
			require.NoError(t, err)
			// protojson output whitespace is unstable; compare structurally.
			var gotAny, wantAny any
			require.NoError(t, json.Unmarshal(got, &gotAny))
			require.NoError(t, json.Unmarshal([]byte(want), &wantAny))
			assert.Equal(t, wantAny, gotAny)

			decoded := tt.msg.ProtoReflect().New().Interface()
			require.NoError(t, protojson.Unmarshal([]byte(want), decoded))
			assert.True(t, proto.Equal(tt.msg, decoded), "decoded %v", decoded)
		})
	}
}
