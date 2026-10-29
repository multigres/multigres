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

package multigateway

import (
	"log/slog"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multigateway/executor"
	"github.com/multigres/multigres/go/services/multigateway/poolergateway"
	"github.com/multigres/multigres/go/tools/viperutil"
)

func newTestStaticRouting(t *testing.T, args ...string) *staticRouting {
	t.Helper()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	r := &staticRouting{}
	r.RegisterFlags(fs, viperutil.NewRegistry())
	require.NoError(t, fs.Parse(args))
	return r
}

// TestStaticRoutingIsOffUnlessConfigured: without its flags it changes nothing, so the gateway
// routes application SQL to the default tablegroup as it always did.
func TestStaticRoutingIsOffUnlessConfigured(t *testing.T) {
	require.NoError(t, newTestStaticRouting(t).apply(nil, nil, slog.Default()), "no flags: nothing to apply")
	var unregistered *staticRouting
	require.NoError(t, unregistered.apply(nil, nil, slog.Default()), "flags never registered")
}

// TestStaticRoutingReadsItsFlagsAndApplies: each gateway reads the values of its own registry,
// and with both set the routing is applied before anything is served.
func TestStaticRoutingReadsItsFlagsAndApplies(t *testing.T) {
	one := newTestStaticRouting(t, "--routing-database=one", "--routing-tablegroup=oneTG")
	two := newTestStaticRouting(t, "--routing-database=two", "--routing-tablegroup=twoTG")
	require.Equal(t, "one", one.database.Get())
	require.Equal(t, "oneTG", one.tablegroup.Get())
	require.Equal(t, "two", two.database.Get())
	require.Equal(t, "twoTG", two.tablegroup.Get())

	ex := executor.NewExecutor(nil, slog.Default(), 0)
	require.NoError(t, one.apply(ex, &poolergateway.PoolerGateway{}, slog.Default()))
}

// TestStaticRoutingNeedsBothFlags: one of the two alone is a mistake, not a default.
func TestStaticRoutingNeedsBothFlags(t *testing.T) {
	for _, args := range [][]string{{"--routing-database=app"}, {"--routing-tablegroup=migrateTG"}} {
		require.ErrorContains(t, newTestStaticRouting(t, args...).apply(nil, nil, slog.Default()), "set together")
	}
}
