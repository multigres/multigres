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

package server

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
)

// TestWriteRawMessage verifies the opaque passthrough write path emits the
// pre-framed DataRow block to the client verbatim, through both the buffered
// writer (the normal path) and the direct connection fallback.
func TestWriteRawMessage(t *testing.T) {
	// block is two already-framed DataRow messages, as the multipooler forwards
	// them; WriteRawMessage must write the bytes unchanged.
	block := []byte("D\x00\x00\x00\x0bhelloD\x00\x00\x00\x0bworld")

	t.Run("buffered writer", func(t *testing.T) {
		var buf bytes.Buffer
		c := &Conn{bufferedWriter: bufio.NewWriter(&buf)}
		require.NoError(t, c.WriteRawMessage(block))
		require.NoError(t, c.bufferedWriter.Flush())
		assert.Equal(t, block, buf.Bytes())
	})

	t.Run("direct connection", func(t *testing.T) {
		var buf bytes.Buffer
		c := &Conn{conn: &mockNetConn{buf: &buf}} // bufferedWriter nil -> conn.Write
		require.NoError(t, c.WriteRawMessage(block))
		assert.Equal(t, block, buf.Bytes())
	})
}

// TestWriteResultRows verifies the send path chooses the opaque block verbatim
// when PassthroughBlock is present and falls back to per-row structured frames otherwise.
func TestWriteResultRows(t *testing.T) {
	t.Run("opaque forwards the block verbatim", func(t *testing.T) {
		block := []byte("D\x00\x00\x00\x0bhelloD\x00\x00\x00\x0bworld")
		var buf bytes.Buffer
		c := &Conn{bufferedWriter: bufio.NewWriter(&buf)}
		require.NoError(t, c.writeResultRows(&sqltypes.Result{PassthroughBlock: block, PassthroughRowCount: 2}))
		require.NoError(t, c.bufferedWriter.Flush())
		assert.Equal(t, block, buf.Bytes())
	})

	t.Run("structured writes one DataRow per row", func(t *testing.T) {
		var buf bytes.Buffer
		c := &Conn{bufferedWriter: bufio.NewWriter(&buf)}
		result := &sqltypes.Result{Rows: []*sqltypes.Row{
			{Values: []sqltypes.Value{[]byte("a")}},
			{Values: []sqltypes.Value{[]byte("bb")}},
		}}
		require.NoError(t, c.writeResultRows(result))
		require.NoError(t, c.bufferedWriter.Flush())
		// Two DataRow frames, each starting with the 'D' type byte.
		assert.Equal(t, byte('D'), buf.Bytes()[0])
		assert.Equal(t, 2, bytes.Count(buf.Bytes(), []byte{'D'}), "one frame per row")
	})

	t.Run("opaque write error is wrapped", func(t *testing.T) {
		// bufferedWriter nil -> conn.Write, which fails here.
		c := &Conn{conn: errWriteConn{&mockNetConn{buf: &bytes.Buffer{}}}}
		err := c.writeResultRows(&sqltypes.Result{PassthroughBlock: []byte("Draw")})
		assert.ErrorContains(t, err, "writing raw data block")
	})

	t.Run("structured write error is wrapped", func(t *testing.T) {
		c := &Conn{conn: errWriteConn{&mockNetConn{buf: &bytes.Buffer{}}}}
		result := &sqltypes.Result{Rows: []*sqltypes.Row{{Values: []sqltypes.Value{[]byte("a")}}}}
		err := c.writeResultRows(result)
		assert.ErrorContains(t, err, "writing data row")
	})
}

// errWriteConn is a net.Conn whose Write always fails, to exercise write-error
// paths.
type errWriteConn struct{ *mockNetConn }

func (errWriteConn) Write([]byte) (int, error) { return 0, errWrite }

var errWrite = errString("write failed")

type errString string

func (e errString) Error() string { return string(e) }
