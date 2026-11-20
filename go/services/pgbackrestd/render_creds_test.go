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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCredsCommandCarriesNonDefaultStateDir(t *testing.T) {
	pod := &Server{cfg: Config{StateDir: defaultStateDir}, self: "/multigres/bin/pgbackrestd"}
	assert.Equal(t, []string{"/multigres/bin/pgbackrestd", "creds"}, pod.credsCommand(),
		"the pod layout must not gain a flag")

	local := &Server{cfg: Config{StateDir: "/tmp/p1/pgbackrestd-data/state"}, self: "/multigres/bin/pgbackrestd"}
	assert.Equal(t, []string{"/multigres/bin/pgbackrestd", "creds", "--state-dir", "/tmp/p1/pgbackrestd-data/state"}, local.credsCommand(),
		"the helper finds its socket under the state dir, so a custom one must be passed on")
}

// pgBackRest reads the process command as a list, one argument per
// occurrence of the option. One value holding "prog creds" is a program named
// "prog creds", which it cannot execute.
func TestProcessCmdValueRendersOneOccurrencePerArgument(t *testing.T) {
	got := processCmdValue("repo1-s3-process-cmd", []string{"/bin/pgbackrestd", "creds", "--state-dir", "/s"})
	assert.Equal(t, "/bin/pgbackrestd\nrepo1-s3-process-cmd=creds\nrepo1-s3-process-cmd=--state-dir\nrepo1-s3-process-cmd=/s", got)
}
