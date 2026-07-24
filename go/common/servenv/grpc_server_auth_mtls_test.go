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
)

func TestMtlsAuthPluginInitializer(t *testing.T) {
	tests := []struct {
		name       string
		substrings string
		wantErr    bool
	}{
		{name: "empty allow-list rejected", substrings: "", wantErr: true},
		{name: "empty token rejected", substrings: "client-a::client-b", wantErr: true},
		{name: "trailing empty token rejected", substrings: "client-a:", wantErr: true},
		{name: "valid single entry", substrings: "client-a", wantErr: false},
		{name: "valid multiple entries", substrings: "client-a:client-b", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := clientCertSubstrings
			t.Cleanup(func() { clientCertSubstrings = orig })
			clientCertSubstrings = tt.substrings

			auth, err := mtlsAuthPluginInitializer()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "grpc-auth-mtls-allowed-substrings")
			} else {
				require.NoError(t, err)
				require.NotNil(t, auth)
			}
		})
	}
}
