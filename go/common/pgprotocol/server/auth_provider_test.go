// Copyright 2025 Supabase, Inc.
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

package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/scram"
)

func TestConstantHashProvider(t *testing.T) {
	t.Run("returns hash for any user", func(t *testing.T) {
		provider := NewConstantHashProvider("testpassword")

		// Should return the same hash for any user/database combination.
		hash1, err := provider.GetPasswordHash(context.Background(), "user1", "db1")
		require.NoError(t, err)
		require.NotNil(t, hash1)

		hash2, err := provider.GetPasswordHash(context.Background(), "user2", "db2")
		require.NoError(t, err)
		require.NotNil(t, hash2)

		// Both should be the same hash.
		assert.Equal(t, hash1.Salt, hash2.Salt)
		assert.Equal(t, hash1.Iterations, hash2.Iterations)
		assert.Equal(t, hash1.StoredKey, hash2.StoredKey)
		assert.Equal(t, hash1.ServerKey, hash2.ServerKey)
	})

	t.Run("hash verifies correct password", func(t *testing.T) {
		password := "mysecretpassword"
		provider := NewConstantHashProvider(password)

		hash, err := provider.GetPasswordHash(context.Background(), "anyuser", "anydb")
		require.NoError(t, err)

		// Verify the hash was computed correctly by recomputing it.
		saltedPassword := scram.ComputeSaltedPassword(password, hash.Salt, hash.Iterations)
		clientKey := scram.ComputeClientKey(saltedPassword)
		storedKey := scram.ComputeStoredKey(clientKey)
		serverKey := scram.ComputeServerKey(saltedPassword)

		assert.Equal(t, storedKey, hash.StoredKey, "StoredKey should match")
		assert.Equal(t, serverKey, hash.ServerKey, "ServerKey should match")
	})

	t.Run("different passwords produce different hashes", func(t *testing.T) {
		provider1 := NewConstantHashProvider("password1")
		provider2 := NewConstantHashProvider("password2")

		hash1, err := provider1.GetPasswordHash(context.Background(), "user", "db")
		require.NoError(t, err)

		hash2, err := provider2.GetPasswordHash(context.Background(), "user", "db")
		require.NoError(t, err)

		// The stored keys should be different.
		assert.NotEqual(t, hash1.StoredKey, hash2.StoredKey)
		assert.NotEqual(t, hash1.ServerKey, hash2.ServerKey)
	})

	t.Run("hash has expected iterations", func(t *testing.T) {
		provider := NewConstantHashProvider("password")

		hash, err := provider.GetPasswordHash(context.Background(), "user", "db")
		require.NoError(t, err)

		// The implementation uses 4096 iterations.
		assert.Equal(t, 4096, hash.Iterations)
	})
}
