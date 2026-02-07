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

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
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

func TestHandleParameterStatus(t *testing.T) {
	conn := &Conn{
		serverParams: make(map[string]string),
	}

	// Build a ParameterStatus message body: name\0value\0
	w := NewMessageWriter()
	w.WriteString("TimeZone")
	w.WriteString("UTC")

	err := conn.handleParameterStatus(w.Bytes())
	require.NoError(t, err)

	// Verify serverParams is updated.
	assert.Equal(t, "UTC", conn.serverParams["TimeZone"])

	// Verify parameterStatus is populated.
	ps := conn.GetParameterStatus()
	assert.Equal(t, "UTC", ps["TimeZone"])

	// Verify GetParameterStatus clears the buffer.
	ps2 := conn.GetParameterStatus()
	assert.Nil(t, ps2)
}

func TestHandleParameterStatus_MultipleParams(t *testing.T) {
	conn := &Conn{
		serverParams: make(map[string]string),
	}

	// Simulate multiple ParameterStatus messages.
	params := map[string]string{
		"TimeZone":        "America/New_York",
		"client_encoding": "UTF8",
		"DateStyle":       "ISO, MDY",
	}
	for name, value := range params {
		w := NewMessageWriter()
		w.WriteString(name)
		w.WriteString(value)
		err := conn.handleParameterStatus(w.Bytes())
		require.NoError(t, err)
	}

	// All params should be in serverParams.
	for name, value := range params {
		assert.Equal(t, value, conn.serverParams[name])
	}

	// All params should be in parameterStatus.
	ps := conn.GetParameterStatus()
	require.NotNil(t, ps)
	for name, value := range params {
		assert.Equal(t, value, ps[name])
	}
}

func TestHandleParameterStatus_OverwritesPreviousValue(t *testing.T) {
	conn := &Conn{
		serverParams: make(map[string]string),
	}

	// Set initial value.
	w := NewMessageWriter()
	w.WriteString("TimeZone")
	w.WriteString("UTC")
	err := conn.handleParameterStatus(w.Bytes())
	require.NoError(t, err)

	// Overwrite with new value.
	w2 := NewMessageWriter()
	w2.WriteString("TimeZone")
	w2.WriteString("US/Pacific")
	err = conn.handleParameterStatus(w2.Bytes())
	require.NoError(t, err)

	// Both maps should have the latest value.
	assert.Equal(t, "US/Pacific", conn.serverParams["TimeZone"])
	ps := conn.GetParameterStatus()
	assert.Equal(t, "US/Pacific", ps["TimeZone"])
}

func TestGetParameterStatus_ClearsAfterRead(t *testing.T) {
	conn := &Conn{
		serverParams: make(map[string]string),
	}

	// Set a parameter.
	w := NewMessageWriter()
	w.WriteString("work_mem")
	w.WriteString("64MB")
	err := conn.handleParameterStatus(w.Bytes())
	require.NoError(t, err)

	// First read returns the value.
	ps := conn.GetParameterStatus()
	require.NotNil(t, ps)
	assert.Equal(t, "64MB", ps["work_mem"])

	// Second read returns nil (cleared).
	ps2 := conn.GetParameterStatus()
	assert.Nil(t, ps2)

	// serverParams still has the value.
	assert.Equal(t, "64MB", conn.serverParams["work_mem"])

	// New parameter status after clear works correctly.
	w2 := NewMessageWriter()
	w2.WriteString("work_mem")
	w2.WriteString("128MB")
	err = conn.handleParameterStatus(w2.Bytes())
	require.NoError(t, err)

	ps3 := conn.GetParameterStatus()
	require.NotNil(t, ps3)
	assert.Equal(t, "128MB", ps3["work_mem"])
}
