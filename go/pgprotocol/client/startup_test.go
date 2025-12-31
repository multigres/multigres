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

package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pgprotocol/protocol"
)

func TestHandleAuthenticationRequest_AuthOk(t *testing.T) {
	w := NewMessageWriter()
	w.WriteInt32(protocol.AuthOk)

	conn := &Conn{}
	err := conn.handleAuthenticationRequest(w.Bytes())
	require.NoError(t, err)
}

func TestHandleAuthenticationRequest_RejectsCleartextPassword(t *testing.T) {
	w := NewMessageWriter()
	w.WriteInt32(protocol.AuthCleartextPassword)

	conn := &Conn{}
	err := conn.handleAuthenticationRequest(w.Bytes())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cleartext password authentication")
	assert.Contains(t, err.Error(), "not supported")
	assert.Contains(t, err.Error(), "security")
}

func TestHandleAuthenticationRequest_RejectsMD5Password(t *testing.T) {
	w := NewMessageWriter()
	w.WriteInt32(protocol.AuthMD5Password)
	// MD5 auth includes a 4-byte salt, but we reject before reading it
	w.WriteBytes([]byte{0x01, 0x02, 0x03, 0x04})

	conn := &Conn{}
	err := conn.handleAuthenticationRequest(w.Bytes())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "MD5 password authentication")
	assert.Contains(t, err.Error(), "not supported")
	assert.Contains(t, err.Error(), "security")
}

func TestHandleAuthenticationRequest_RejectsUnsupportedMethod(t *testing.T) {
	w := NewMessageWriter()
	w.WriteInt32(99) // Unknown auth type

	conn := &Conn{}
	err := conn.handleAuthenticationRequest(w.Bytes())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported authentication method")
}

func TestHandleAuthenticationRequest_MessageTooShort(t *testing.T) {
	conn := &Conn{}
	err := conn.handleAuthenticationRequest([]byte{0x00, 0x00}) // Only 2 bytes, need 4

	require.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}
