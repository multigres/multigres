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

package servenv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/tools/viperutil"
)

func TestProcessResourcesValidate(t *testing.T) {
	reg := viperutil.NewRegistry()
	complete := ProcessResources{
		ServEnv:    NewServEnv(reg),
		GrpcServer: NewGrpcServer(reg),
		TopoStore:  func() topoclient.Store { return nil },
	}
	require.NoError(t, complete.Validate())

	err := ProcessResources{}.Validate()
	require.Error(t, err)
	for _, missing := range []string{"ServEnv", "GrpcServer", "TopoStore"} {
		assert.Contains(t, err.Error(), missing)
	}
}
