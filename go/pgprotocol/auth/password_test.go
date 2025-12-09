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

package auth

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseScramSHA256Hash(t *testing.T) {
	// Test case from PostgreSQL documentation and real PostgreSQL output.
	// Generated with: CREATE ROLE testuser WITH LOGIN PASSWORD 'testpassword';
	// SELECT rolpassword FROM pg_authid WHERE rolname = 'testuser';

	t.Run("valid SCRAM-SHA-256 hash", func(t *testing.T) {
		// This is a real PostgreSQL SCRAM-SHA-256 hash for password "pencil"
		// with salt "W22ZaJ0SNY7soEsUEjb6gQ==" and 4096 iterations.
		hash := "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$WG5d8oPm3OtcPnkdi4Oln6rNiYzlYY42lUpMtdJ7U90=:HKZfkuYXDxJboM9DFNR0yFNHpRx/rbdVdNOTk/V0v0Q="

		parsed, err := ParseScramSHA256Hash(hash)
		require.NoError(t, err)

		assert.Equal(t, 4096, parsed.Iterations)

		// Decode expected salt
		expectedSalt, err := base64.StdEncoding.DecodeString("W22ZaJ0SNY7soEsUEjb6gQ==")
		require.NoError(t, err)
		assert.Equal(t, expectedSalt, parsed.Salt)

		// Decode expected StoredKey
		expectedStoredKey, err := base64.StdEncoding.DecodeString("WG5d8oPm3OtcPnkdi4Oln6rNiYzlYY42lUpMtdJ7U90=")
		require.NoError(t, err)
		assert.Equal(t, expectedStoredKey, parsed.StoredKey)

		// Decode expected ServerKey
		expectedServerKey, err := base64.StdEncoding.DecodeString("HKZfkuYXDxJboM9DFNR0yFNHpRx/rbdVdNOTk/V0v0Q=")
		require.NoError(t, err)
		assert.Equal(t, expectedServerKey, parsed.ServerKey)
	})

	t.Run("valid hash with different iterations", func(t *testing.T) {
		// Hash with 8192 iterations (non-default)
		hash := "SCRAM-SHA-256$8192:c2FsdA==$c3RvcmVka2V5:c2VydmVya2V5"

		parsed, err := ParseScramSHA256Hash(hash)
		require.NoError(t, err)

		assert.Equal(t, 8192, parsed.Iterations)
		assert.Equal(t, []byte("salt"), parsed.Salt)
		assert.Equal(t, []byte("storedkey"), parsed.StoredKey)
		assert.Equal(t, []byte("serverkey"), parsed.ServerKey)
	})

	t.Run("invalid prefix", func(t *testing.T) {
		hash := "MD5$4096:c2FsdA==$c3RvcmVka2V5:c2VydmVya2V5"
		_, err := ParseScramSHA256Hash(hash)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid SCRAM-SHA-256 hash prefix")
	})

	t.Run("missing parts", func(t *testing.T) {
		hash := "SCRAM-SHA-256$4096:c2FsdA=="
		_, err := ParseScramSHA256Hash(hash)
		assert.Error(t, err)
	})

	t.Run("invalid iterations - not a number", func(t *testing.T) {
		hash := "SCRAM-SHA-256$abc:c2FsdA==$c3RvcmVka2V5:c2VydmVya2V5"
		_, err := ParseScramSHA256Hash(hash)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "iterations")
	})

	t.Run("invalid iterations - negative", func(t *testing.T) {
		hash := "SCRAM-SHA-256$-1:c2FsdA==$c3RvcmVka2V5:c2VydmVya2V5"
		_, err := ParseScramSHA256Hash(hash)
		assert.Error(t, err)
	})

	t.Run("invalid iterations - zero", func(t *testing.T) {
		hash := "SCRAM-SHA-256$0:c2FsdA==$c3RvcmVka2V5:c2VydmVya2V5"
		_, err := ParseScramSHA256Hash(hash)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "iterations must be positive")
	})

	t.Run("invalid salt - not base64", func(t *testing.T) {
		hash := "SCRAM-SHA-256$4096:not-valid-base64!!!$c3RvcmVka2V5:c2VydmVya2V5"
		_, err := ParseScramSHA256Hash(hash)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "salt")
	})

	t.Run("invalid StoredKey - not base64", func(t *testing.T) {
		hash := "SCRAM-SHA-256$4096:c2FsdA==$not-valid-base64!!!:c2VydmVya2V5"
		_, err := ParseScramSHA256Hash(hash)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "StoredKey")
	})

	t.Run("invalid ServerKey - not base64", func(t *testing.T) {
		hash := "SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVka2V5:not-valid-base64!!!"
		_, err := ParseScramSHA256Hash(hash)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ServerKey")
	})

	t.Run("empty hash", func(t *testing.T) {
		_, err := ParseScramSHA256Hash("")
		assert.Error(t, err)
	})

	t.Run("missing keys section", func(t *testing.T) {
		hash := "SCRAM-SHA-256$4096:c2FsdA=="
		_, err := ParseScramSHA256Hash(hash)
		assert.Error(t, err)
	})

	t.Run("missing ServerKey", func(t *testing.T) {
		hash := "SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVka2V5"
		_, err := ParseScramSHA256Hash(hash)
		assert.Error(t, err)
	})
}

func TestScramHashStruct(t *testing.T) {
	t.Run("ScramHash fields are correct types", func(t *testing.T) {
		hash := ScramHash{
			Iterations: 4096,
			Salt:       []byte("salt"),
			StoredKey:  []byte("storedkey"),
			ServerKey:  []byte("serverkey"),
		}

		assert.Equal(t, 4096, hash.Iterations)
		assert.Equal(t, []byte("salt"), hash.Salt)
		assert.Equal(t, []byte("storedkey"), hash.StoredKey)
		assert.Equal(t, []byte("serverkey"), hash.ServerKey)
	})
}

func TestIsScramSHA256Hash(t *testing.T) {
	t.Run("valid SCRAM-SHA-256 prefix", func(t *testing.T) {
		assert.True(t, IsScramSHA256Hash("SCRAM-SHA-256$4096:salt$storedkey:serverkey"))
	})

	t.Run("MD5 hash", func(t *testing.T) {
		assert.False(t, IsScramSHA256Hash("md5abc123"))
	})

	t.Run("plaintext password", func(t *testing.T) {
		assert.False(t, IsScramSHA256Hash("mypassword"))
	})

	t.Run("empty string", func(t *testing.T) {
		assert.False(t, IsScramSHA256Hash(""))
	})

	t.Run("SCRAM-SHA-256 prefix only", func(t *testing.T) {
		// Should return true for prefix check, actual parsing would fail
		assert.True(t, IsScramSHA256Hash("SCRAM-SHA-256"))
	})
}
