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
	"bufio"
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// TestBindAndExecute_FatalErrorWithoutReadyForQuery_PreservesDiagnostic
// exercises the extended query protocol equivalent of the simple-query fix
// in query_test.go: Postgrex (and other clients using the extended
// protocol) hits this exact path for a self pg_terminate_backend(pg_backend_pid()).
// PostgreSQL sends BindComplete, then a FATAL ErrorResponse, then closes the
// connection without ever sending ReadyForQuery. processBindAndExecuteResponses
// must return that diagnostic instead of the read failure that follows it.
func TestBindAndExecute_FatalErrorWithoutReadyForQuery_PreservesDiagnostic(t *testing.T) {
	var input bytes.Buffer
	writeRawMessage(&input, protocol.MsgBindComplete, nil)
	input.Write(buildErrorResponseWithSeverity("FATAL", "57P01", "terminating connection due to administrator command"))
	// No ReadyForQuery follows: the input ends here, exactly like PostgreSQL
	// closing the socket right after a FATAL.

	c := newTestReadOnlyConn(input.Bytes())
	_, err := c.processBindAndExecuteResponses(context.Background(), nil)

	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "expected a *mterrors.PgDiagnostic, got %T: %v", err, err)
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, "57P01", diag.Code)
}

// TestWaitForParseComplete_FatalErrorWithoutReadyForQuery_PreservesDiagnostic
// covers waitForParseComplete's identical FATAL/PANIC handling to
// processBindAndExecuteResponses above: PostgreSQL sends a FATAL
// ErrorResponse in place of ParseComplete and closes the connection without
// a trailing ReadyForQuery.
func TestWaitForParseComplete_FatalErrorWithoutReadyForQuery_PreservesDiagnostic(t *testing.T) {
	input := buildErrorResponseWithSeverity("FATAL", "57P01", "terminating connection due to administrator command")

	c := newTestReadOnlyConn(input)
	err := c.waitForParseComplete(context.Background())

	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "expected a *mterrors.PgDiagnostic, got %T: %v", err, err)
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, "57P01", diag.Code)
}

// TestWaitForCloseComplete_FatalErrorWithoutReadyForQuery_PreservesDiagnostic
// covers waitForCloseComplete's identical FATAL/PANIC handling.
func TestWaitForCloseComplete_FatalErrorWithoutReadyForQuery_PreservesDiagnostic(t *testing.T) {
	input := buildErrorResponseWithSeverity("FATAL", "57P01", "terminating connection due to administrator command")

	c := newTestReadOnlyConn(input)
	err := c.waitForCloseComplete(context.Background())

	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "expected a *mterrors.PgDiagnostic, got %T: %v", err, err)
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, "57P01", diag.Code)
}

// TestWaitForReadyForQuery_FatalErrorWithoutReadyForQuery_PreservesDiagnostic
// covers waitForReadyForQuery's identical FATAL/PANIC handling.
func TestWaitForReadyForQuery_FatalErrorWithoutReadyForQuery_PreservesDiagnostic(t *testing.T) {
	input := buildErrorResponseWithSeverity("FATAL", "57P01", "terminating connection due to administrator command")

	c := newTestReadOnlyConn(input)
	err := c.waitForReadyForQuery(context.Background())

	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "expected a *mterrors.PgDiagnostic, got %T: %v", err, err)
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, "57P01", diag.Code)
}

// TestProcessExecuteResponses_FatalErrorWithoutReadyForQuery_PreservesDiagnostic
// covers processExecuteResponses' identical FATAL/PANIC handling.
func TestProcessExecuteResponses_FatalErrorWithoutReadyForQuery_PreservesDiagnostic(t *testing.T) {
	input := buildErrorResponseWithSeverity("FATAL", "57P01", "terminating connection due to administrator command")

	c := newTestReadOnlyConn(input)
	_, err := c.processExecuteResponses(context.Background(), nil)

	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "expected a *mterrors.PgDiagnostic, got %T: %v", err, err)
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, "57P01", diag.Code)
}

// TestProcessDescribeResponses_FatalErrorWithoutReadyForQuery_PreservesDiagnostic
// covers processDescribeResponses' identical FATAL/PANIC handling.
func TestProcessDescribeResponses_FatalErrorWithoutReadyForQuery_PreservesDiagnostic(t *testing.T) {
	input := buildErrorResponseWithSeverity("FATAL", "57P01", "terminating connection due to administrator command")

	c := newTestReadOnlyConn(input)
	_, err := c.processDescribeResponses(context.Background())

	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "expected a *mterrors.PgDiagnostic, got %T: %v", err, err)
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, "57P01", diag.Code)
}

// TestProcessBindAndDescribeResponses_FatalErrorWithoutReadyForQuery_PreservesDiagnostic
// covers processBindAndDescribeResponses' identical FATAL/PANIC handling.
func TestProcessBindAndDescribeResponses_FatalErrorWithoutReadyForQuery_PreservesDiagnostic(t *testing.T) {
	input := buildErrorResponseWithSeverity("FATAL", "57P01", "terminating connection due to administrator command")

	c := newTestReadOnlyConn(input)
	_, err := c.processBindAndDescribeResponses(context.Background())

	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "expected a *mterrors.PgDiagnostic, got %T: %v", err, err)
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, "57P01", diag.Code)
}

// TestProcessPrepareAndExecuteResponses_FatalErrorWithoutReadyForQuery_PreservesDiagnostic
// covers processPrepareAndExecuteResponses' identical FATAL/PANIC handling.
func TestProcessPrepareAndExecuteResponses_FatalErrorWithoutReadyForQuery_PreservesDiagnostic(t *testing.T) {
	input := buildErrorResponseWithSeverity("FATAL", "57P01", "terminating connection due to administrator command")

	c := newTestReadOnlyConn(input)
	err := c.processPrepareAndExecuteResponses(context.Background(), nil)

	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "expected a *mterrors.PgDiagnostic, got %T: %v", err, err)
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, "57P01", diag.Code)
}

// TestProcessBindDescribeAndExecuteResponses_FatalErrorWithoutReadyForQuery_PreservesDiagnostic
// covers processBindDescribeAndExecuteResponses' identical FATAL/PANIC handling.
func TestProcessBindDescribeAndExecuteResponses_FatalErrorWithoutReadyForQuery_PreservesDiagnostic(t *testing.T) {
	input := buildErrorResponseWithSeverity("FATAL", "57P01", "terminating connection due to administrator command")

	c := newTestReadOnlyConn(input)
	_, err := c.processBindDescribeAndExecuteResponses(context.Background(), nil)

	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "expected a *mterrors.PgDiagnostic, got %T: %v", err, err)
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, "57P01", diag.Code)
}

// TestExtendedResponseLoops_ReadFailureWrapsErrorWhenNothingCaptured covers
// readLoopErr's fallback branch at every one of the nine extended-protocol
// response loops' call sites: when the socket fails before any diagnostic
// was ever parsed, the loop must return a wrapped read error, not a nil or
// swallowed failure.
func TestExtendedResponseLoops_ReadFailureWrapsErrorWhenNothingCaptured(t *testing.T) {
	cases := []struct {
		name string
		call func(c *Conn) error
	}{
		{"waitForParseComplete", func(c *Conn) error {
			return c.waitForParseComplete(context.Background())
		}},
		{"waitForCloseComplete", func(c *Conn) error {
			return c.waitForCloseComplete(context.Background())
		}},
		{"waitForReadyForQuery", func(c *Conn) error {
			return c.waitForReadyForQuery(context.Background())
		}},
		{"processDescribeResponses", func(c *Conn) error {
			_, err := c.processDescribeResponses(context.Background())
			return err
		}},
		{"processExecuteResponses", func(c *Conn) error {
			_, err := c.processExecuteResponses(context.Background(), nil)
			return err
		}},
		{"processBindAndExecuteResponses", func(c *Conn) error {
			_, err := c.processBindAndExecuteResponses(context.Background(), nil)
			return err
		}},
		{"processBindAndDescribeResponses", func(c *Conn) error {
			_, err := c.processBindAndDescribeResponses(context.Background())
			return err
		}},
		{"processPrepareAndExecuteResponses", func(c *Conn) error {
			return c.processPrepareAndExecuteResponses(context.Background(), nil)
		}},
		{"processBindDescribeAndExecuteResponses", func(c *Conn) error {
			_, err := c.processBindDescribeAndExecuteResponses(context.Background(), nil)
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestReadOnlyConn(nil) // empty input: the first read fails immediately
			err := tc.call(c)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "failed to read message")
		})
	}
}

// TestWaitForParseComplete_NonFatalErrorThenReadFailure_PreservesFirstError
// covers readLoopErr's other branch: a non-fatal ErrorResponse leaves the
// loop waiting for ReadyForQuery, and if the socket then fails (e.g. the
// server crashed after sending the error but before the trailing RFQ), the
// loop must still return the diagnostic already captured rather than
// masking it with the read failure that follows.
func TestWaitForParseComplete_NonFatalErrorThenReadFailure_PreservesFirstError(t *testing.T) {
	input := buildErrorResponse("42601", "syntax error at or near \"nonsense\"")

	c := newTestReadOnlyConn(input)
	err := c.waitForParseComplete(context.Background())

	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "expected a *mterrors.PgDiagnostic, got %T: %v", err, err)
	assert.Equal(t, "42601", diag.Code)
}

func TestWriteParse(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeParse("stmt1", "SELECT $1, $2", []uint32{23, 25})
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgParse), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)

	body, err := conn.readMessageBody(length)
	require.NoError(t, err)

	r := NewMessageReader(body)

	// Statement name.
	name, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "stmt1", name)

	// Query string.
	queryStr, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "SELECT $1, $2", queryStr)

	// Parameter count.
	paramCount, err := r.ReadInt16()
	require.NoError(t, err)
	assert.Equal(t, int16(2), paramCount)

	// Parameter types.
	oid1, err := r.ReadUint32()
	require.NoError(t, err)
	assert.Equal(t, uint32(23), oid1)

	oid2, err := r.ReadUint32()
	require.NoError(t, err)
	assert.Equal(t, uint32(25), oid2)
}

func TestWriteBind(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	params := [][]byte{[]byte("42"), []byte("hello")}
	paramFormats := []int16{0, 0}
	resultFormats := []int16{0}

	err := conn.writeBind("portal1", "stmt1", params, paramFormats, resultFormats)
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgBind), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)

	body, err := conn.readMessageBody(length)
	require.NoError(t, err)

	r := NewMessageReader(body)

	// Portal name.
	portalName, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "portal1", portalName)

	// Statement name.
	stmtName, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "stmt1", stmtName)

	// Parameter format count.
	formatCount, err := r.ReadInt16()
	require.NoError(t, err)
	assert.Equal(t, int16(2), formatCount)

	// Parameter formats.
	for range 2 {
		format, err := r.ReadInt16()
		require.NoError(t, err)
		assert.Equal(t, int16(0), format)
	}

	// Parameter count.
	paramCount, err := r.ReadInt16()
	require.NoError(t, err)
	assert.Equal(t, int16(2), paramCount)

	// Parameters.
	p1, err := r.ReadByteString()
	require.NoError(t, err)
	assert.Equal(t, []byte("42"), p1)

	p2, err := r.ReadByteString()
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), p2)

	// Result format count.
	resultFormatCount, err := r.ReadInt16()
	require.NoError(t, err)
	assert.Equal(t, int16(1), resultFormatCount)

	// Result format.
	resultFormat, err := r.ReadInt16()
	require.NoError(t, err)
	assert.Equal(t, int16(0), resultFormat)
}

func TestWriteExecute(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeExecute("portal1", 100)
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgExecute), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)

	body, err := conn.readMessageBody(length)
	require.NoError(t, err)

	r := NewMessageReader(body)

	// Portal name.
	portalName, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "portal1", portalName)

	// Max rows.
	maxRows, err := r.ReadInt32()
	require.NoError(t, err)
	assert.Equal(t, int32(100), maxRows)
}

func TestWriteDescribe(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeDescribe('S', "stmt1")
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgDescribe), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)

	body, err := conn.readMessageBody(length)
	require.NoError(t, err)

	r := NewMessageReader(body)

	// Describe type.
	descType, err := r.ReadByte()
	require.NoError(t, err)
	assert.Equal(t, byte('S'), descType)

	// Name.
	name, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "stmt1", name)
}

func TestWriteClose(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeClose('P', "portal1")
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgClose), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)

	body, err := conn.readMessageBody(length)
	require.NoError(t, err)

	r := NewMessageReader(body)

	// Close type.
	closeType, err := r.ReadByte()
	require.NoError(t, err)
	assert.Equal(t, byte('P'), closeType)

	// Name.
	name, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "portal1", name)
}

func TestWriteSync(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeSync()
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgSync), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)
	assert.Equal(t, 0, length) // Sync has no body.
}

func TestWriteFlush(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeFlush()
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgFlush), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)
	assert.Equal(t, 0, length) // Flush has no body.
}

func TestWriteTerminate(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeTerminate()
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgTerminate), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)
	assert.Equal(t, 0, length) // Terminate has no body.
}

func TestWriteBindWithNullParams(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	// Include a NULL parameter.
	params := [][]byte{[]byte("42"), nil, []byte("hello")}

	err := conn.writeBind("", "", params, nil, nil)
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgBind), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)

	body, err := conn.readMessageBody(length)
	require.NoError(t, err)

	r := NewMessageReader(body)

	// Skip portal and statement names.
	_, _ = r.ReadString()
	_, _ = r.ReadString()

	// Skip parameter format count (0).
	_, _ = r.ReadInt16()

	// Parameter count.
	paramCount, err := r.ReadInt16()
	require.NoError(t, err)
	assert.Equal(t, int16(3), paramCount)

	// Parameters.
	p1, err := r.ReadByteString()
	require.NoError(t, err)
	assert.Equal(t, []byte("42"), p1)

	p2, err := r.ReadByteString()
	require.NoError(t, err)
	assert.Nil(t, p2) // NULL

	p3, err := r.ReadByteString()
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), p3)
}

func TestEncodeStringArray(t *testing.T) {
	t.Run("empty slice", func(t *testing.T) {
		assert.Equal(t, "{}", encodeStringArray([]string{}))
	})

	t.Run("single plain element", func(t *testing.T) {
		assert.Equal(t, `{"foo"}`, encodeStringArray([]string{"foo"}))
	})

	t.Run("multiple plain elements", func(t *testing.T) {
		assert.Equal(t, `{"foo","bar","baz"}`, encodeStringArray([]string{"foo", "bar", "baz"}))
	})

	t.Run("element with space is quoted", func(t *testing.T) {
		assert.Equal(t, `{"foo bar"}`, encodeStringArray([]string{"foo bar"}))
	})

	t.Run("element with comma is quoted", func(t *testing.T) {
		assert.Equal(t, `{"foo,bar"}`, encodeStringArray([]string{"foo,bar"}))
	})

	t.Run("element with double quote is quoted and escaped", func(t *testing.T) {
		assert.Equal(t, `{"foo\"bar"}`, encodeStringArray([]string{`foo"bar`}))
	})

	t.Run("element with backslash is quoted and escaped", func(t *testing.T) {
		assert.Equal(t, `{"foo\\bar"}`, encodeStringArray([]string{`foo\bar`}))
	})

	t.Run("pooler ID format round-trips through ParseTextArray", func(t *testing.T) {
		elems := []string{"zone1_pooler-1", "zone1_pooler-2", "zone1_pooler-3"}
		encoded := encodeStringArray(elems)
		assert.Equal(t, `{"zone1_pooler-1","zone1_pooler-2","zone1_pooler-3"}`, encoded)
	})

	t.Run("unicode elements are encoded correctly", func(t *testing.T) {
		assert.Equal(t, `{"ᚼᛅᛁᛚ","ᚼᛅᛁᛗᚱ"}`, encodeStringArray([]string{"ᚼᛅᛁᛚ", "ᚼᛅᛁᛗᚱ"}))
	})

	t.Run("null string is quoted to avoid SQL NULL interpretation", func(t *testing.T) {
		assert.Equal(t, `{"NULL"}`, encodeStringArray([]string{"NULL"}))
	})
}
