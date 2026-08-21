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

package cluster

import (
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

func getSwitchPrimaryCommand() *cobra.Command {
	clusterCmd := &cobra.Command{Use: "cluster"}
	AddSwitchPrimaryCommand(clusterCmd)
	cmd, _, _ := clusterCmd.Find([]string{"switch-primary"})
	return cmd
}

func TestSwitchPrimaryCommandFlags(t *testing.T) {
	cmd := getSwitchPrimaryCommand()
	require.NotNil(t, cmd)

	tests := []struct {
		flag     string
		defValue string
	}{
		{"database", "postgres"},
		{"table-group", constants.DefaultTableGroup},
		{"shard", constants.DefaultShard},
		{"reason", ""},
		{"yes", "false"},
		{"timeout", (120 * time.Second).String()},
		{"admin-server", ""},
	}
	for _, tt := range tests {
		t.Run(tt.flag, func(t *testing.T) {
			f := cmd.Flag(tt.flag)
			require.NotNil(t, f, "flag %q should exist", tt.flag)
			assert.Equal(t, tt.defValue, f.DefValue)
		})
	}
}

func TestConfirmSwitchPrimary_Success(t *testing.T) {
	cmd, out := makeConfirmCmd("0-inf\n")

	req := &multiadminpb.SwitchPrimaryRequest{
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "db1",
			TableGroup: "default",
			Shard:      "0-inf",
		},
		Reason: "maintenance",
	}
	err := confirmSwitchPrimary(cmd, req)
	require.NoError(t, err)

	// Summary should mention the fields the operator needs to see before
	// confirming.
	output := out.String()
	assert.Contains(t, output, "Shard:   db1/default/0-inf")
	assert.Contains(t, output, "Reason:  maintenance")
}

func TestConfirmSwitchPrimary_WrongShard(t *testing.T) {
	cmd, _ := makeConfirmCmd("wrong-shard\n")

	req := &multiadminpb.SwitchPrimaryRequest{
		ShardKey: &clustermetadatapb.ShardKey{Shard: "0-inf"},
	}
	err := confirmSwitchPrimary(cmd, req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "aborted")
}

func TestConfirmSwitchPrimary_EOF(t *testing.T) {
	cmd, _ := makeConfirmCmd("")

	req := &multiadminpb.SwitchPrimaryRequest{
		ShardKey: &clustermetadatapb.ShardKey{Shard: "0-inf"},
	}
	err := confirmSwitchPrimary(cmd, req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "aborted")
}

func TestConfirmSwitchPrimary_Whitespace(t *testing.T) {
	cmd, _ := makeConfirmCmd("  0-inf  \n")

	req := &multiadminpb.SwitchPrimaryRequest{
		ShardKey: &clustermetadatapb.ShardKey{Shard: "0-inf"},
	}
	err := confirmSwitchPrimary(cmd, req)
	require.NoError(t, err, "leading/trailing whitespace should be trimmed")
}
