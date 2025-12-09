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

package scram

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

const (
	// ScramSHA256Prefix is the prefix for SCRAM-SHA-256 password hashes in PostgreSQL.
	ScramSHA256Prefix = "SCRAM-SHA-256"

	// MinIterationCount is the minimum PBKDF2 iteration count accepted for security.
	// RFC 5802 recommends a minimum of 4096 iterations to make brute-force attacks harder.
	MinIterationCount = 4096

	// MinSaltLength is the minimum salt length in bytes accepted for security.
	MinSaltLength = 8
)

// ScramHash contains the parsed components of a PostgreSQL SCRAM-SHA-256 password hash.
// The hash format is: SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>
// where salt, StoredKey, and ServerKey are base64-encoded.
type ScramHash struct {
	// Iterations is the PBKDF2 iteration count used to derive the salted password.
	Iterations int

	// Salt is the random salt used in PBKDF2 key derivation.
	Salt []byte

	// StoredKey is H(ClientKey) where H is SHA-256 and ClientKey = HMAC(SaltedPassword, "Client Key").
	// Used to verify the client's proof.
	StoredKey []byte

	// ServerKey is HMAC(SaltedPassword, "Server Key").
	// Used to generate the server's signature for mutual authentication.
	ServerKey []byte
}

// ParseScramSHA256Hash parses a PostgreSQL SCRAM-SHA-256 password hash string.
// The expected format is: SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>
//
// Example: SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$WG5d8oPm3OtcPnkdi4Oln6rNiYzlYY42lUpMtdJ7U90=:HKZfkuYXDxJboM9DFNR0yFNHpRx/rbdVdNOTk/V0v0Q=
func ParseScramSHA256Hash(hash string) (*ScramHash, error) {
	if hash == "" {
		return nil, fmt.Errorf("empty hash string")
	}

	// Check prefix.
	if !strings.HasPrefix(hash, ScramSHA256Prefix+"$") {
		return nil, fmt.Errorf("invalid SCRAM-SHA-256 hash prefix: expected %q prefix", ScramSHA256Prefix)
	}

	// Remove prefix.
	remainder := strings.TrimPrefix(hash, ScramSHA256Prefix+"$")

	// Split into iterations:salt and StoredKey:ServerKey parts.
	parts := strings.Split(remainder, "$")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid SCRAM-SHA-256 hash format: expected 2 parts separated by '$', got %d", len(parts))
	}

	iterationsAndSalt := parts[0]
	keys := parts[1]

	// Parse iterations and salt.
	iterSaltParts := strings.SplitN(iterationsAndSalt, ":", 2)
	if len(iterSaltParts) != 2 {
		return nil, fmt.Errorf("invalid SCRAM-SHA-256 hash format: expected iterations:salt")
	}

	iterations, err := strconv.Atoi(iterSaltParts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid iterations value: %w", err)
	}
	if iterations <= 0 {
		return nil, fmt.Errorf("iterations must be positive, got %d", iterations)
	}
	if iterations < MinIterationCount {
		return nil, fmt.Errorf("iteration count %d below minimum %d (insecure)", iterations, MinIterationCount)
	}

	salt, err := base64.StdEncoding.DecodeString(iterSaltParts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid salt (base64 decode failed): %w", err)
	}
	if len(salt) < MinSaltLength {
		return nil, fmt.Errorf("salt length %d below minimum %d bytes (insecure)", len(salt), MinSaltLength)
	}

	// Parse StoredKey and ServerKey.
	keyParts := strings.SplitN(keys, ":", 2)
	if len(keyParts) != 2 {
		return nil, fmt.Errorf("invalid SCRAM-SHA-256 hash format: expected StoredKey:ServerKey")
	}

	storedKey, err := base64.StdEncoding.DecodeString(keyParts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid StoredKey (base64 decode failed): %w", err)
	}

	serverKey, err := base64.StdEncoding.DecodeString(keyParts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid ServerKey (base64 decode failed): %w", err)
	}

	return &ScramHash{
		Iterations: iterations,
		Salt:       salt,
		StoredKey:  storedKey,
		ServerKey:  serverKey,
	}, nil
}

// IsScramSHA256Hash returns true if the hash string appears to be a SCRAM-SHA-256 hash.
// This is a quick check based on the prefix; it does not validate the entire format.
func IsScramSHA256Hash(hash string) bool {
	return strings.HasPrefix(hash, ScramSHA256Prefix)
}
