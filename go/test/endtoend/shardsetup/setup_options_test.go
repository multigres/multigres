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

package shardsetup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestWithPgInitdbArgs verifies the SetupOption stores the verbatim initdb
// args string on SetupConfig. The args are passed through to pgctld's
// --pg-initdb-args flag at process spawn time.
func TestWithPgInitdbArgs(t *testing.T) {
	c := &SetupConfig{}
	WithPgInitdbArgs("--no-locale --encoding=UTF8")(c)
	assert.Equal(t, "--no-locale --encoding=UTF8", c.PgInitdbArgs)
}

// TestWithPgInitdbExtraConfFiles verifies the SetupOption appends each path
// to SetupConfig.PgInitdbExtraConfFiles. Variadic + repeated invocations
// must accumulate rather than overwrite so callers can stack snippets.
func TestWithPgInitdbExtraConfFiles(t *testing.T) {
	t.Run("single path", func(t *testing.T) {
		c := &SetupConfig{}
		WithPgInitdbExtraConfFiles("/tmp/a.conf")(c)
		assert.Equal(t, []string{"/tmp/a.conf"}, c.PgInitdbExtraConfFiles)
	})

	t.Run("multiple paths in one call", func(t *testing.T) {
		c := &SetupConfig{}
		WithPgInitdbExtraConfFiles("/tmp/a.conf", "/tmp/b.conf")(c)
		assert.Equal(t, []string{"/tmp/a.conf", "/tmp/b.conf"}, c.PgInitdbExtraConfFiles)
	})

	t.Run("repeated calls accumulate", func(t *testing.T) {
		c := &SetupConfig{}
		WithPgInitdbExtraConfFiles("/tmp/a.conf")(c)
		WithPgInitdbExtraConfFiles("/tmp/b.conf", "/tmp/c.conf")(c)
		assert.Equal(t, []string{"/tmp/a.conf", "/tmp/b.conf", "/tmp/c.conf"}, c.PgInitdbExtraConfFiles)
	})
}

// TestAddPgInitdbExtraConfFiles verifies the post-setup mutator appends conf
// snippet paths to EVERY pgctld instance's existing list (it must not replace
// the base snippets applied at setup), and that the appended paths land in the
// argv pgctld is restarted with — which is how they take effect on the next
// ReinitializeCluster. pgregresstest relies on this to scope the external
// extension suite's server config (shared_preload_libraries) to the external
// phase's cluster only.
func TestAddPgInitdbExtraConfFiles(t *testing.T) {
	s := &ShardSetup{
		Multipoolers: map[string]*MultipoolerInstance{
			"pooler-1": {Name: "pooler-1", Pgctld: &ProcessInstance{PgInitdbExtraConfFiles: []string{"/tmp/base.conf"}}},
			"pooler-2": {Name: "pooler-2", Pgctld: &ProcessInstance{PgInitdbExtraConfFiles: []string{"/tmp/base.conf"}}},
		},
	}

	s.AddPgInitdbExtraConfFiles("/tmp/preload.conf", "/tmp/pg_cron.conf")

	for name, inst := range s.Multipoolers {
		assert.Equal(t,
			[]string{"/tmp/base.conf", "/tmp/preload.conf", "/tmp/pg_cron.conf"},
			inst.Pgctld.PgInitdbExtraConfFiles,
			"%s: appended snippets must follow the base ones (later snippets win per GUC)", name)

		// The snippets only matter if the restarted pgctld is launched with
		// them: assert the rebuilt argv carries one --pg-initdb-extra-conf per
		// path, in order.
		args := buildPgctldServerArgs(inst.Pgctld)
		var confArgs []string
		for i, a := range args {
			if a == "--pg-initdb-extra-conf" {
				confArgs = append(confArgs, args[i+1])
			}
		}
		assert.Equal(t, []string{"/tmp/base.conf", "/tmp/preload.conf", "/tmp/pg_cron.conf"}, confArgs, name)
	}
}

func TestMultipoolerExtraArgsMutators(t *testing.T) {
	const vpidFlag = "--backend-vpid-tracking-enabled=true"
	s := &ShardSetup{
		Multipoolers: map[string]*MultipoolerInstance{
			"pooler-1": {Name: "pooler-1", Multipooler: &ProcessInstance{ExtraArgs: []string{"--existing=1"}}},
			"pooler-2": {Name: "pooler-2", Multipooler: &ProcessInstance{ExtraArgs: []string{"--other=true", vpidFlag}}},
			"pooler-3": {Name: "pooler-3"}, // nil Multipooler is skipped safely.
		},
	}

	s.AddMultipoolerExtraArgs(vpidFlag, "--foo=bar", vpidFlag)

	assert.Equal(t, []string{"--existing=1", vpidFlag, "--foo=bar"}, s.Multipoolers["pooler-1"].Multipooler.ExtraArgs)
	assert.Equal(t, []string{"--other=true", vpidFlag, "--foo=bar"}, s.Multipoolers["pooler-2"].Multipooler.ExtraArgs)
	assert.Nil(t, s.Multipoolers["pooler-3"].Multipooler)

	s.RemoveMultipoolerExtraArgs(vpidFlag, "--missing=true")

	assert.Equal(t, []string{"--existing=1", "--foo=bar"}, s.Multipoolers["pooler-1"].Multipooler.ExtraArgs)
	assert.Equal(t, []string{"--other=true", "--foo=bar"}, s.Multipoolers["pooler-2"].Multipooler.ExtraArgs)
	assert.Nil(t, s.Multipoolers["pooler-3"].Multipooler)
}

// TestProcessInstancePgInitdbArgs pins the pgctld arg-construction path
// that forwards PgInitdbArgs as --pg-initdb-args. Bypasses the actual
// process spawn (which needs a real pgctld binary on PATH) by reading the
// argv that startPgctld would assemble.
func TestProcessInstancePgInitdbArgs(t *testing.T) {
	t.Run("empty arg omits flag", func(t *testing.T) {
		p := &ProcessInstance{PgInitdbArgs: ""}
		args := buildPgctldServerArgs(p)
		assert.NotContains(t, args, "--pg-initdb-args")
	})

	t.Run("non-empty arg appends flag and value", func(t *testing.T) {
		p := &ProcessInstance{PgInitdbArgs: "--no-locale --encoding=UTF8"}
		args := buildPgctldServerArgs(p)
		// flag and value are appended as two argv slots so exec preserves
		// the value as a single argument even with embedded spaces.
		idx := -1
		for i, a := range args {
			if a == "--pg-initdb-args" {
				idx = i
				break
			}
		}
		if assert.GreaterOrEqual(t, idx, 0, "--pg-initdb-args flag should be present") {
			assert.Equal(t, "--no-locale --encoding=UTF8", args[idx+1])
		}
	})
}
