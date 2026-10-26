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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/viperutil"
)

// preRunWithConfig runs preRun with yaml as the config file and args as flags.
func preRunWithConfig(t *testing.T, yaml string, args ...string) (*minigres, error) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "minigres.yaml")
	require.NoError(t, os.WriteFile(file, []byte(yaml), 0o600))
	m, cmd := newPreRunCommand(t, append(args, "--config-file="+file)...)
	return m, m.preRun(cmd)
}

func TestConfigFile_ConfiguresBothHalves(t *testing.T) {
	m, err := preRunWithConfig(t, "cell: zone1\nservice-id: mg-1\nmultigateway.pg-port: 15433\nmultipooler.database: appdb\n", "--service-id=mg-2")
	require.NoError(t, err)

	gateway, pooler := m.gateway.ServiceIdentity(), m.pooler.ServiceIdentity()
	assert.Equal(t, "zone1", gateway.Cell, "a shared setting reaches both halves")
	assert.Equal(t, "zone1", pooler.Cell)
	assert.Equal(t, "mg-2", gateway.ServiceInstanceID, "the flag wins over the file")
	assert.Equal(t, "mg-2", pooler.ServiceInstanceID)
	assert.Equal(t, "appdb", pooler.Database)
	assert.Equal(t, 15433, m.reg.Combined().GetInt("multigateway.pg-port"))
}

func TestConfigFile_Rejects(t *testing.T) {
	_, err := preRunWithConfig(t, "cell: zone1\nmultipooler.cell: zone1\n")
	require.NoError(t, err, "a shared setting in a section with the top-level value is accepted")

	for yaml, want := range map[string]string{
		"pg-port: 15432\n":                       "pg-port: unknown setting",
		"multigateway.pg-prot: 15432\n":          "multigateway.pg-prot: unknown setting",
		"multipooler.pg-port: 5432\n":            "multipooler.pg-port: not available in minigres",
		"cell: zone1\nmultipooler.cell: zone2\n": "multipooler.cell: cell is shared by both halves; set it once as cell",
	} {
		t.Run(yaml, func(t *testing.T) {
			_, err := preRunWithConfig(t, yaml)
			require.ErrorContains(t, err, want)
		})
	}
}

// TestConfigFile_ExampleIsValid keeps config/minigres.yaml in step with the
// settings minigres reads.
func TestConfigFile_ExampleIsValid(t *testing.T) {
	m, cmd := newPreRunCommand(t, "--config-file="+filepath.Join("..", "..", "..", "config", "minigres.yaml"))
	require.NoError(t, m.preRun(cmd))
}

func TestCheckEnvClashes(t *testing.T) {
	assert.NoError(t, checkEnvClashes(newMinigres().reg), "minigres's own settings do not clash")

	reg := viperutil.NewRegistry()
	gateway, pooler := reg.WithPrefix("multigateway"), reg.WithPrefix("multipooler")
	viperutil.Configure(gateway, "cell", viperutil.Options[string]{EnvVars: []string{"MT_CELL"}})
	viperutil.Configure(pooler, "cell", viperutil.Options[string]{EnvVars: []string{"MT_CELL"}})
	viperutil.Configure(gateway, "pg-port", viperutil.Options[int]{EnvVars: []string{"MT_PG_PORT"}})
	viperutil.Configure(pooler, "pg-port", viperutil.Options[int]{EnvVars: []string{"MT_PG_PORT"}})
	err := checkEnvClashes(reg)
	assert.ErrorContains(t, err, "environment variable MT_PG_PORT is read by multigateway.pg-port and multipooler.pg-port")
	assert.NotContains(t, err.Error(), "MT_CELL", "a shared setting may be read by both halves")
}
