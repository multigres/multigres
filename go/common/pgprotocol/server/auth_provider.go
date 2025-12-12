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

	"github.com/multigres/multigres/go/common/pgprotocol/scram"
)

// TODO(dweitzman): Delete this file once real credential fetching is implemented.
// This is a temporary placeholder that accepts a single hardcoded password
// for all users, used to test SCRAM protocol integration before the
// GetAuthCredentials gRPC endpoint exists.
//
// Replace with a real PasswordHashProvider that fetches hashes from
// multipooler via gRPC (which queries pg_authid).

// ConstantHashProvider implements scram.PasswordHashProvider by returning
// the same pre-computed hash for any user. This accepts a single hardcoded
// password for all users - NOT FOR PRODUCTION USE.
type ConstantHashProvider struct {
	hash *scram.ScramHash
}

// NewConstantHashProvider creates a provider that accepts the given password
// for any user. The hash is computed once at creation time.
//
// Example: NewConstantHashProvider("postgres") accepts password "postgres"
// for any username.
func NewConstantHashProvider(password string) *ConstantHashProvider {
	// Use a fixed salt for reproducibility in testing.
	// In production, each user would have a unique random salt.
	salt := []byte("multigres-test-salt!")
	iterations := 4096

	saltedPassword := scram.ComputeSaltedPassword(password, salt, iterations)
	clientKey := scram.ComputeClientKey(saltedPassword)

	return &ConstantHashProvider{
		hash: &scram.ScramHash{
			Iterations: iterations,
			Salt:       salt,
			StoredKey:  scram.ComputeStoredKey(clientKey),
			ServerKey:  scram.ComputeServerKey(saltedPassword),
		},
	}
}

// GetPasswordHash returns the pre-computed hash for any user.
// This implements scram.PasswordHashProvider.
func (p *ConstantHashProvider) GetPasswordHash(_ context.Context, _, _ string) (*scram.ScramHash, error) {
	return p.hash, nil
}
