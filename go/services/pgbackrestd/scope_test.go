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

package pgbackrestd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

func scopedTo(tableGroup, shard string, id int64) func(*Config) {
	return func(c *Config) { c.TableGroup, c.Shard, c.TableGroupID = tableGroup, shard, id }
}

func repoPathLine(t *testing.T, conf string) string {
	t.Helper()
	for line := range strings.SplitSeq(conf, "\n") {
		if path, ok := strings.CutPrefix(line, "repo1-path="); ok {
			return path
		}
	}
	require.FailNow(t, "no repo1-path in the rendered conf", conf)
	return ""
}

func TestRepositoryScope_ByShardShape(t *testing.T) {
	tests := []struct {
		name       string
		tableGroup string
		shard      string
		id         int64
		want       string
	}{
		{"catalog shard keeps the base path", constants.DefaultTableGroup, constants.DefaultShard, 0, ""},
		{"dynamic tablegroup with its catalog id", "commerce", "0-80", 7, "tablegroups/commerce/7/0-80"},
		{"tablegroup whose id is unknown", "commerce", "0-80", 0, "tablegroups/commerce/0-80"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newControlHarness(t, scopedTo(tc.tableGroup, tc.shard, tc.id))
			got, err := h.s.cfg.repositoryScope()
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)

			resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
			require.NoError(t, err)
			assert.Equal(t, tc.want, resp.Msg.RepositoryScope, "Status reports the scope the flags select")

			require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
			path := repoPathLine(t, readFile(t, h.s.cfg.stateSub(fileClientConf)))
			base := h.s.cfg.FSRepoPath
			if tc.want == "" {
				assert.Equal(t, base, path)
			} else {
				assert.Equal(t, filepath.Join(base, tc.want), path)
			}
		})
	}
}

func TestRepositoryScope_TwoShardsRenderDifferentRepositories(t *testing.T) {
	shared := t.TempDir()
	render := func(shard string) string {
		h := newControlHarness(t, func(c *Config) {
			c.FSRepoPath = shared
			scopedTo("commerce", shard, 7)(c)
		})
		require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
		return repoPathLine(t, readFile(t, h.s.cfg.stateSub(fileClientConf)))
	}
	a, b := render("0-80"), render("80-inf")
	assert.NotEqual(t, a, b)
	assert.Equal(t, filepath.Join(shared, "tablegroups/commerce/7/0-80"), a)
	assert.Equal(t, filepath.Join(shared, "tablegroups/commerce/7/80-inf"), b)
}

func TestConfigValidate_RequiresAScope(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"no table group":    scopedTo("", constants.DefaultShard, 0),
		"no shard":          scopedTo(constants.DefaultTableGroup, "", 0),
		"negative id":       scopedTo("commerce", "0-80", -1),
		"neither table nor": scopedTo("", "", 0),
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t)
			mutate(&cfg)
			assert.Error(t, cfg.Validate())
		})
	}
}

// The first Configure pins the state directory to its repository. A start
// whose flags select another one stops before it serves anything.
func TestRepositoryScope_PinStopsAChangedScope(t *testing.T) {
	h := newControlHarness(t, scopedTo("commerce", "0-80", 7))
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	assert.Equal(t, "tablegroups/commerce/7/0-80", readFile(t, h.s.cfg.stateSub(fileRepositoryScope)))
	cfg := h.s.cfg
	require.NoError(t, h.s.Close())

	for name, mutate := range map[string]func(*Config){
		"another shard":      scopedTo("commerce", "80-inf", 7),
		"another tablegroup": scopedTo("orders", "0-80", 7),
		"another id":         scopedTo("commerce", "0-80", 8),
		"the id forgotten":   scopedTo("commerce", "0-80", 0),
		"the catalog shard":  scopedTo(constants.DefaultTableGroup, constants.DefaultShard, 0),
	} {
		t.Run(name, func(t *testing.T) {
			changed := cfg
			mutate(&changed)
			s, err := newForTest(changed)
			if s != nil {
				s.Close()
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), fileRepositoryScope)
			assert.Contains(t, err.Error(), "tablegroups/commerce/7/0-80")
		})
	}

	s, err := newForTest(cfg)
	require.NoError(t, err, "the scope that was pinned still starts")
	require.NoError(t, s.Close())
}

// The pin survives a generations file that no longer loads, which is set
// aside and leaves the sidecar unconfigured.
func TestRepositoryScope_PinSurvivesASetAsideState(t *testing.T) {
	h := newControlHarness(t, scopedTo("commerce", "0-80", 7))
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	cfg := h.s.cfg
	require.NoError(t, h.s.Close())
	require.NoError(t, os.WriteFile(cfg.stateSub(fileGenerations), []byte("not json"), 0o600))

	s, err := newForTest(cfg)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	_, statErr := os.Stat(cfg.stateSub(fileGenerations))
	require.ErrorIs(t, statErr, os.ErrNotExist, "the state was set aside")

	changed := cfg
	scopedTo("commerce", "80-inf", 7)(&changed)
	s, err = newForTest(changed)
	if s != nil {
		s.Close()
	}
	require.Error(t, err)
}

// A state directory that holds a configuration but no pin was in use before
// repositories were scoped, so it used the base path.
func TestRepositoryScope_ConfigurationWithoutAPinIsTheBasePath(t *testing.T) {
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	cfg := h.s.cfg
	require.NoError(t, h.s.Close())
	require.NoError(t, os.Remove(cfg.stateSub(fileRepositoryScope)))

	scoped := cfg
	scopedTo("commerce", "0-80", 7)(&scoped)
	s, err := newForTest(scoped)
	if s != nil {
		s.Close()
	}
	require.Error(t, err)

	s, err = newForTest(cfg)
	require.NoError(t, err, "the catalog shard is the base path")
	require.NoError(t, s.Close())
}

// A directory that never configured a repository has nothing to protect: its
// first scope is accepted and then pinned.
func TestRepositoryScope_UnconfiguredDirectoryIsNotPinned(t *testing.T) {
	h := newControlHarness(t, scopedTo("commerce", "0-80", 7))
	_, err := os.Stat(h.s.cfg.stateSub(fileRepositoryScope))
	require.ErrorIs(t, err, os.ErrNotExist)
	cfg := h.s.cfg
	require.NoError(t, h.s.Close())

	changed := cfg
	scopedTo("commerce", "80-inf", 7)(&changed)
	s, err := newForTest(changed)
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

// Neither Configure nor Run carries a location, a path or a stanza, so no
// request can move the sidecar to another repository.
func TestRepositoryScope_NoRequestNamesARepository(t *testing.T) {
	for _, arg := range []string{
		"--repo1-path=/other", "--repo-path=/other", "--stanza=other", "--config=/tmp/c.conf",
		"--repo1-s3-bucket=other", "--repo1-type=posix", "--repo1-s3-key-prefix=/other",
	} {
		for _, cmd := range []pb.Command{pb.Command_COMMAND_BACKUP, pb.Command_COMMAND_INFO, pb.Command_COMMAND_EXPIRE, pb.Command_COMMAND_RESTORE, pb.Command_COMMAND_STANZA_CREATE} {
			assert.Error(t, checkArgs(cmd, []string{arg}), "%s %s", cmd, arg)
		}
	}
	_, hasScope := any(&pb.ConfigureRequest{}).(interface{ GetRepositoryScope() string })
	assert.False(t, hasScope, "Configure has no field that selects a repository")
}
