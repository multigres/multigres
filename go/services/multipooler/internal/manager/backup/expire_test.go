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

package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
)

func TestExpire_RequiresActionLock(t *testing.T) {
	e, _ := newTestEngine(t, t.TempDir(), "tg1", "0", "/tmp/backups")
	_, err := e.Expire(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context does not hold an action lock")
}

// TestExpire_NoExpirations drives Expire with a stub whose backup list is
// unchanged across the run, so no IDs are reported as expired.
func TestExpire_NoExpirations(t *testing.T) {
	json := `[{"backup":[{"label":"20250104-100000F","type":"full","annotation":{"table_group":"tg1","shard":"0","job_id":"j1"}}]}]`
	stubPgbackrest(t, pgbackrestInfoStub(json))
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	lock := actionlock.NewActionLock()
	ctx, err := lock.Acquire(context.Background(), "expire-test")
	require.NoError(t, err)
	defer lock.Release(ctx)

	expired, err := e.Expire(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, expired, "backup list is unchanged, so nothing should be reported expired")
}
