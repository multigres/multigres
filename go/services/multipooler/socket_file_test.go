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

package multipooler

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/viperutil"
)

func TestResolveSocketFilePath(t *testing.T) {
	tests := []struct {
		name          string
		configured    string
		explicitlySet bool
		poolerDir     string
		pgPort        int
		want          string
	}{
		{
			name:      "unset flag with pooler-dir derives the socket path",
			poolerDir: "/data/pooler-1", pgPort: 5433,
			want: "/data/pooler-1/pg_sockets/.s.PGSQL.5433",
		},
		{
			name:       "explicit path wins over derivation",
			configured: "/custom/.s.PGSQL.5432", poolerDir: "/data/pooler-1", pgPort: 5432,
			want: "/custom/.s.PGSQL.5432",
		},
		{
			name:          "explicitly empty forces TCP",
			explicitlySet: true, poolerDir: "/data/pooler-1", pgPort: 5432,
			want: "",
		},
		{
			name:   "no pooler-dir keeps TCP",
			pgPort: 5432,
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveSocketFilePath(tt.configured, tt.explicitlySet, tt.poolerDir, tt.pgPort))
		})
	}
}

func TestFlagExplicitlySet(t *testing.T) {
	// No FlagSet registered (minimal test setups): never explicit.
	mp := &Multipooler{}
	assert.False(t, mp.flagExplicitlySet("socket-file"))

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.String("socket-file", "", "")
	mp.flagSet = fs

	assert.False(t, mp.flagExplicitlySet("socket-file"), "registered but untouched flag is not explicit")
	assert.False(t, mp.flagExplicitlySet("no-such-flag"))

	require.NoError(t, fs.Set("socket-file", ""))
	assert.True(t, mp.flagExplicitlySet("socket-file"), "explicitly set to empty is still explicit")
}

func TestFlagExplicitlySet_ConfigFile(t *testing.T) {
	reg := viperutil.NewRegistry()
	mp := &Multipooler{reg: reg}
	viperutil.Configure(reg, "socket-file", viperutil.Options[string]{Default: "", FlagName: "socket-file"})
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.String("socket-file", "", "")
	mp.flagSet = fs

	assert.False(t, mp.flagExplicitlySet("socket-file"), "no flag, no config file: not explicit")

	// A config file pinning socket-file to empty is as deliberate as
	// --socket-file='' and must equally count as explicit.
	path := filepath.Join(t.TempDir(), "mtconfig.yaml")
	require.NoError(t, os.WriteFile(path, []byte("socket-file: \"\"\n"), 0o644))
	vc := viperutil.NewViperConfig(reg)
	vc.RegisterFlags(fs)
	require.NoError(t, fs.Set("config-file", path))
	cancel, err := vc.LoadConfig(reg)
	require.NoError(t, err)
	t.Cleanup(cancel)

	assert.True(t, mp.flagExplicitlySet("socket-file"))
	// Through the resolver: the TCP dial is preserved despite a pooler-dir.
	assert.Equal(t, "", resolveSocketFilePath("", mp.flagExplicitlySet("socket-file"), "/data/pooler-1", 5432))
}
