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

package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// Anything the sidecar's command set does not name is refused before a call is
// made: the API has no stanza, config file or free-form command to fall back on.
func TestNewSidecarCmd_RefusesWhatTheSidecarDoesNotOffer(t *testing.T) {
	for name, args := range map[string][]string{
		"an unknown subcommand": {"--stanza=multigres", "annotate"},
		"no subcommand":         {"--stanza=multigres", "--repo=1"},
		"a second positional":   {"backup", "extra"},
		"a command with a typo": {"backups"},
	} {
		t.Run(name, func(t *testing.T) {
			c := newSidecarCmd(args)
			require.Error(t, c.err)

			e := &Engine{}
			_, _, err := e.runSidecar(t.Context(), c)
			require.Error(t, err, "the error is returned without reaching the sidecar")
		})
	}
}

func TestNewSidecarCmd_StripsStanzaAndConfigAndKeepsOptions(t *testing.T) {
	c := newSidecarCmd([]string{"--stanza=multigres", "--config=/x.conf", "backup", "--type=full", "--repo=1"})
	require.NoError(t, c.err)
	assert.Equal(t, pb.Command_COMMAND_BACKUP, c.command)
	assert.Equal(t, []string{"--type=full", "--repo=1"}, c.args)
}
