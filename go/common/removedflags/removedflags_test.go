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

package removedflags

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheck(t *testing.T) {
	newFlags := func() *pflag.FlagSet {
		fs := pflag.NewFlagSet("multipooler", pflag.ContinueOnError)
		fs.String("pooler-dir", "", "")
		Register(fs, Multipooler)
		return fs
	}

	fs := newFlags()
	require.NoError(t, fs.Parse([]string{"--pooler-dir=/data"}))
	require.NoError(t, Check(fs, "multipooler", Multipooler), "a command line without a removed flag starts")

	for _, name := range Multipooler {
		fs := newFlags()
		assert.True(t, fs.Lookup(name).Hidden, "--%s must not be listed in --help", name)
		// Any value parses: the old flags were strings and an integer.
		require.NoError(t, fs.Parse([]string{"--" + name + "=8432"}))
		err := Check(fs, "multipooler", Multipooler)
		require.Error(t, err, "--%s must refuse startup", name)
		assert.Equal(t, "flag --"+name+" was removed: pgBackRest runs in the pgbackrestd sidecar, and this multipooler needs a multigres-operator that deploys that sidecar and does not pass the flag", err.Error())
	}

	// An empty value still counts: the flag was passed.
	fs = newFlags()
	require.NoError(t, fs.Parse([]string{"--pgbackrest-ca-file="}))
	require.Error(t, Check(fs, "multipooler", Multipooler))

	fs = newFlags()
	require.NoError(t, fs.Parse([]string{"--pgbackrest-port", "8432", "--pgbackrest-ca-file", "/certs/ca.crt"}))
	err := Check(fs, "multipooler", Multipooler)
	require.ErrorContains(t, err, "flags --pgbackrest-ca-file, --pgbackrest-port were removed")
}
