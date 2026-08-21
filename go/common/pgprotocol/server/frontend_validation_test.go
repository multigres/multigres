// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"bytes"
	"context"
	"testing"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFrontendBody(buf *bytes.Buffer, body []byte) {
	writeTestInt32(buf, int32(len(body)+4))
	buf.Write(body)
}

func requireProtocolViolation(t *testing.T, writeBuf *bytes.Buffer) {
	t.Helper()
	assert.Equal(t, mterrors.PgSSProtocolViolation, errorResponseSQLSTATE(t, writeBuf))
}

func TestStrictFrontendMessageValidation(t *testing.T) {
	parseBody := []byte("s\x00SELECT 1\x00\x00\x00")
	bindBody := []byte("p\x00s\x00\x00\x00\x00\x00\x00\x00")
	executeBody := []byte("p\x00\x00\x00\x00\x00")

	tests := []struct {
		name   string
		body   []byte
		handle func(*Conn) error
	}{
		{"Parse missing statement name", nil, func(c *Conn) error { return c.handleParse() }},
		{"Parse unterminated statement name", []byte("s"), func(c *Conn) error { return c.handleParse() }},
		{"Parse missing query", []byte("s\x00"), func(c *Conn) error { return c.handleParse() }},
		{"Parse unterminated query", []byte("s\x00SELECT 1"), func(c *Conn) error { return c.handleParse() }},
		{"Parse missing parameter count", []byte("s\x00SELECT 1\x00"), func(c *Conn) error { return c.handleParse() }},
		{"Parse trailing byte", append(append([]byte(nil), parseBody...), 'x'), func(c *Conn) error { return c.handleParse() }},
		{"Parse unsigned count without OIDs", []byte("s\x00SELECT 1\x00\x80\x00"), func(c *Conn) error { return c.handleParse() }},
		{"Bind missing portal name", nil, func(c *Conn) error { return c.handleBind() }},
		{"Bind unterminated portal name", []byte("p"), func(c *Conn) error { return c.handleBind() }},
		{"Bind missing statement name", []byte("p\x00"), func(c *Conn) error { return c.handleBind() }},
		{"Bind unterminated statement name", []byte("p\x00s"), func(c *Conn) error { return c.handleBind() }},
		{"Bind missing parameter format count", []byte("p\x00s\x00"), func(c *Conn) error { return c.handleBind() }},
		{"Bind parameter format count exceeds body", []byte("p\x00s\x00\x00\x01"), func(c *Conn) error { return c.handleBind() }},
		{"Bind missing parameter count", []byte("p\x00s\x00\x00\x00"), func(c *Conn) error { return c.handleBind() }},
		{"Bind parameter count exceeds body", []byte("p\x00s\x00\x00\x00\x00\x01"), func(c *Conn) error { return c.handleBind() }},
		{"Bind truncated parameter value", []byte("p\x00s\x00\x00\x00\x00\x01\x00\x00\x00\x01"), func(c *Conn) error { return c.handleBind() }},
		{"Bind missing result format count", []byte("p\x00s\x00\x00\x00\x00\x00"), func(c *Conn) error { return c.handleBind() }},
		{"Bind result format count exceeds body", []byte("p\x00s\x00\x00\x00\x00\x00\x00\x01"), func(c *Conn) error { return c.handleBind() }},
		{"Bind trailing byte", append(append([]byte(nil), bindBody...), 'x'), func(c *Conn) error { return c.handleBind() }},
		{"Execute missing portal name", nil, func(c *Conn) error { return c.handleExecute() }},
		{"Execute unterminated portal name", []byte("p"), func(c *Conn) error { return c.handleExecute() }},
		{"Execute missing max rows", []byte("p\x00"), func(c *Conn) error { return c.handleExecute() }},
		{"Execute trailing byte", append(append([]byte(nil), executeBody...), 'x'), func(c *Conn) error { return c.handleExecute() }},
		{"Describe missing target", nil, func(c *Conn) error { return c.handleDescribe() }},
		{"Describe invalid target", []byte{'X', 0}, func(c *Conn) error { return c.handleDescribe() }},
		{"Describe unterminated name", []byte{'S', 's'}, func(c *Conn) error { return c.handleDescribe() }},
		{"Describe trailing byte", []byte{'S', 0, 'x'}, func(c *Conn) error { return c.handleDescribe() }},
		{"Close missing target", nil, func(c *Conn) error { return c.handleClose() }},
		{"Close invalid target", []byte{'X', 0}, func(c *Conn) error { return c.handleClose() }},
		{"Close unterminated name", []byte{'S', 's'}, func(c *Conn) error { return c.handleClose() }},
		{"Close trailing byte", []byte{'S', 0, 'x'}, func(c *Conn) error { return c.handleClose() }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var readBuf, writeBuf bytes.Buffer
			conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, &testHandler{})
			writeFrontendBody(&readBuf, tc.body)

			require.NoError(t, tc.handle(conn))
			requireProtocolViolation(t, &writeBuf)
			assert.True(t, conn.discardingUntilSync)
		})
	}
}

func TestBindRejectsInvalidFormatAtBind(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{
			name: "parameter format",
			body: []byte{
				'p', 0, 's', 0,
				0, 1, 0, 2,
				0, 0,
				0, 0,
			},
		},
		{
			name: "result format",
			body: []byte{
				'p', 0, 's', 0,
				0, 0,
				0, 0,
				0, 1, 0, 2,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var readBuf, writeBuf bytes.Buffer
			called := false
			conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, &testHandler{
				bindFunc: func(_ context.Context, _ *Conn, _, _ string, _ [][]byte, _, _ []int16) error {
					called = true
					return nil
				},
			})
			writeFrontendBody(&readBuf, tc.body)

			require.NoError(t, conn.handleBind())
			assert.Equal(t, mterrors.PgSSInvalidParameterValue, errorResponseSQLSTATE(t, &writeBuf))
			assert.False(t, called)
		})
	}
}

func TestParseAcceptsUnsignedParameterCount(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	const count = 32768
	called := false
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, &testHandler{
		parseFunc: func(_ context.Context, _ *Conn, _, _ string, paramTypes []uint32) error {
			called = true
			require.Len(t, paramTypes, count)
			return nil
		},
	})
	body := make([]byte, 0, len("s\x00SELECT 1\x00")+2+count*4)
	body = append(body, []byte("s\x00SELECT 1\x00")...)
	body = append(body, 0x80, 0x00)
	body = append(body, make([]byte, count*4)...)
	writeFrontendBody(&readBuf, body)

	require.NoError(t, conn.handleParse())
	assert.True(t, called)
	msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgParseComplete), msgType)
}

func TestSyncAndFlushRequireEmptyBodies(t *testing.T) {
	t.Run("Sync", func(t *testing.T) {
		var readBuf, writeBuf bytes.Buffer
		conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, &testHandler{})
		writeFrontendBody(&readBuf, []byte{'x'})

		require.NoError(t, conn.handleSync())
		requireProtocolViolation(t, &writeBuf)
		msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
		assert.Equal(t, byte(protocol.MsgReadyForQuery), msgType)
		assert.Zero(t, readBuf.Len())
	})

	t.Run("Flush", func(t *testing.T) {
		var readBuf, writeBuf bytes.Buffer
		conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, &testHandler{})
		writeFrontendBody(&readBuf, []byte{'x'})

		require.NoError(t, conn.handleMessage(protocol.MsgFlush))
		requireProtocolViolation(t, &writeBuf)
		assert.True(t, conn.discardingUntilSync)
		assert.Zero(t, readBuf.Len())
	})
}
