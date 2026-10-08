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
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/tools/viperutil"
)

func TestQueryStreamReuseFlag(t *testing.T) {
	reg := viperutil.NewRegistry()
	mg := NewMultigateway(reg, servenv.ProcessResources{
		ServEnv:    servenv.NewServEnv(reg),
		GrpcServer: servenv.NewGrpcServer(reg),
	}, "/")
	require.True(t, mg.queryStreamReuse.Default())
	fs := pflag.NewFlagSet("query-stream-test", pflag.ContinueOnError)
	mg.RegisterFlags(fs)
	require.NoError(t, fs.Parse([]string{"--query-stream-reuse=false"}))
	require.False(t, mg.queryStreamReuse.Get())
}
