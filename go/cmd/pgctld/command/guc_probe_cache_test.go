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

package command

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
)

// initFakeDataDir satisfies pgctld.IsDataDirInitialized (which checks for
// PG_VERSION under PGDATA) so gucProbeCache.get reaches the probe.
func initFakeDataDir(t *testing.T) {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "pg_data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("17\n"), 0o644))
	t.Setenv(constants.PgDataDirEnvVar, dataDir)
}

func TestGucProbeCache(t *testing.T) {
	ctx := context.Background()
	initFakeDataDir(t)

	t.Run("caches successes and failures per GUC", func(t *testing.T) {
		calls := map[string]int{}
		c := newGucProbeCache(slog.Default())
		c.probe = func(_ context.Context, name string) (int32, error) {
			calls[name]++
			if name == "broken" {
				return 0, errors.New("boom")
			}
			return 110, nil
		}

		v, known := c.get(ctx, "max_connections")
		assert.True(t, known)
		assert.Equal(t, int32(110), v)
		v, known = c.get(ctx, "max_connections")
		assert.True(t, known)
		assert.Equal(t, int32(110), v)
		assert.Equal(t, 1, calls["max_connections"], "successes are cached")

		v, known = c.get(ctx, "broken")
		assert.False(t, known)
		assert.Zero(t, v)
		_, known = c.get(ctx, "broken")
		assert.False(t, known)
		assert.Equal(t, 1, calls["broken"], "failures are cached too — no re-fork per poll")
	})

	t.Run("invalidate clears values and re-probes", func(t *testing.T) {
		val := int32(100)
		c := newGucProbeCache(slog.Default())
		c.probe = func(_ context.Context, _ string) (int32, error) { return val, nil }

		v, _ := c.get(ctx, "max_connections")
		assert.Equal(t, int32(100), v)
		val = 200
		v, _ = c.get(ctx, "max_connections")
		assert.Equal(t, int32(100), v, "cached until invalidated")
		c.invalidate()
		v, _ = c.get(ctx, "max_connections")
		assert.Equal(t, int32(200), v)
	})

	t.Run("zero is a valid known value, distinct from failure", func(t *testing.T) {
		calls := 0
		c := newGucProbeCache(slog.Default())
		c.probe = func(_ context.Context, _ string) (int32, error) { calls++; return 0, nil }

		v, known := c.get(ctx, "reserved_connections")
		assert.True(t, known, "a configured 0 is known, not a failure")
		assert.Zero(t, v)
		_, known = c.get(ctx, "reserved_connections")
		assert.True(t, known)
		assert.Equal(t, 1, calls, "valid zero is cached")
	})

	t.Run("probe overlapping an invalidation is discarded", func(t *testing.T) {
		c := newGucProbeCache(slog.Default())
		c.probe = func(_ context.Context, _ string) (int32, error) {
			// The config mutation completes — and invalidates — while this
			// probe is in flight (e.g. a Restart that changes the value).
			c.invalidate()
			return 100, nil
		}
		v, _ := c.get(ctx, "max_connections")
		assert.Equal(t, int32(100), v, "in-flight caller still gets its result")

		// The stale result must NOT have been published: the next get
		// re-probes and sees the post-mutation value.
		c.probe = func(_ context.Context, _ string) (int32, error) { return 20, nil }
		v, _ = c.get(ctx, "max_connections")
		assert.Equal(t, int32(20), v)
	})
}
