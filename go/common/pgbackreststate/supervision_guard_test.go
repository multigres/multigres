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

package pgbackreststate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Startup and cleanup both refuse anything that is not a regular instance-lock
// file: a directory or a missing handle is evidence that cannot be trusted.
func TestSupervisionRefusesAHandleThatIsNotARegularLock(t *testing.T) {
	dir := t.TempDir()
	asDir, err := os.Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { asDir.Close() })

	assert.ErrorIs(t, ValidateServingProtocol(nil), ErrUnverifiedState)
	assert.ErrorIs(t, ValidateServingProtocol(asDir), ErrUnverifiedState)
	assert.ErrorIs(t, RecordSupervision(dir, nil), ErrUnverifiedState)
	assert.ErrorIs(t, RecordSupervision(dir, asDir), ErrUnverifiedState)
}

func TestValidateServingProtocol_AcceptsEmptyAndTheKnownStampOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), InstanceLockName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	require.NoError(t, ValidateServingProtocol(f), "uninitialized state is allowed at startup")

	_, err = f.WriteAt([]byte(protocolStamp), 0)
	require.NoError(t, err)
	require.NoError(t, ValidateServingProtocol(f))

	require.NoError(t, f.Truncate(0))
	_, err = f.WriteAt([]byte("some other protocol\n"), 0)
	require.NoError(t, err)
	assert.ErrorIs(t, ValidateServingProtocol(f), ErrUnverifiedState)
}

func TestRecordSupervision_RefusesAnUnknownStampAndAMissingStateDir(t *testing.T) {
	t.Run("an unknown nonempty stamp is not overwritten", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, InstanceLockName)
		require.NoError(t, os.WriteFile(path, []byte("legacy owner record"), 0o600))
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		t.Cleanup(func() { f.Close() })

		require.ErrorIs(t, RecordSupervision(dir, f), ErrUnverifiedState)
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "legacy owner record", string(got))
	})

	t.Run("a stamp of the right length with other content", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, InstanceLockName)
		other := make([]byte, len(protocolStamp))
		for i := range other {
			other[i] = 'x'
		}
		require.NoError(t, os.WriteFile(path, other, 0o600))
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		t.Cleanup(func() { f.Close() })
		require.ErrorIs(t, RecordSupervision(dir, f), ErrUnverifiedState)
	})

	t.Run("a state directory that cannot be synced", func(t *testing.T) {
		dir := t.TempDir()
		f, err := os.OpenFile(filepath.Join(dir, InstanceLockName), os.O_CREATE|os.O_RDWR, 0o600)
		require.NoError(t, err)
		t.Cleanup(func() { f.Close() })
		require.Error(t, RecordSupervision(filepath.Join(dir, "missing"), f))
	})
}

func TestCleanupGuard_RequiresAStateDirectoryAndClosesNil(t *testing.T) {
	guard, err := AcquireCleanupGuard("")
	require.Nil(t, guard)
	require.ErrorIs(t, err, ErrUnverifiedState)

	var none *CleanupGuard
	assert.NoError(t, none.Close())
}

func TestCleanupGuard_ResolvesARelativeStateDirectory(t *testing.T) {
	dir, _ := stampedOwner(t)
	// The owner holds the exclusive lock, so the relative spelling resolves to
	// the same directory and is refused as in use rather than as unverified.
	wd, err := os.Getwd()
	require.NoError(t, err)
	rel, err := filepath.Rel(wd, dir)
	require.NoError(t, err)
	guard, err := AcquireCleanupGuard(rel)
	require.Nil(t, guard)
	require.ErrorIs(t, err, ErrStateInUse)
}
