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

package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpireBackups(t *testing.T) {
	// ExpireBackups requires a running pgbackrest with a valid stanza.
	// The actual invocation is tested by integration tests (endtoend/multipooler).
	// This test validates the precondition checks.

	t.Run("fails when not ready", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		err := pm.ExpireBackups(context.Background(), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "manager is in unknown state")
	})
}
