// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package queryserving

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

func frontendPacket(typ byte, body []byte) []byte {
	packet := []byte{typ, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(packet[1:], uint32(len(body)+4))
	return append(packet, body...)
}

func frontendTrace(t *testing.T, ctx context.Context, port int, wire []byte) []string {
	t.Helper()
	dsn := shardsetup.GetTestUserDSN("localhost", port, "sslmode=disable", "connect_timeout=5")
	conn, err := pgconn.Connect(ctx, dsn)
	require.NoError(t, err)
	hijacked, err := conn.Hijack()
	require.NoError(t, err)
	defer hijacked.Conn.Close()
	require.NoError(t, hijacked.Conn.SetDeadline(time.Now().Add(3*time.Second)))
	_, err = hijacked.Conn.Write(wire)
	require.NoError(t, err)

	var trace []string
	for len(trace) < 20 {
		msg, err := hijacked.Frontend.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return append(trace, "EOF")
			}
			return append(trace, "ERR:"+err.Error())
		}
		switch msg := msg.(type) {
		case *pgproto3.ErrorResponse:
			trace = append(trace, fmt.Sprintf("Error(%s)", msg.Code))
		case *pgproto3.ReadyForQuery:
			return append(trace, fmt.Sprintf("Ready(%c)", msg.TxStatus))
		default:
			trace = append(trace, fmt.Sprintf("%T", msg))
		}
	}
	return trace
}

func TestFrontendValidationMatchesPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}
	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 2*time.Minute)

	parse := frontendPacket('P', []byte("s\x00SELECT 1\x00\x00\x00"))
	bind := frontendPacket('B', []byte("p\x00s\x00\x00\x00\x00\x00\x00\x00"))
	execute := frontendPacket('E', []byte("p\x00\x00\x00\x00\x00"))
	sync := frontendPacket('S', nil)

	tests := []struct {
		name string
		wire []byte
	}{
		{"Oversized frontend frame", func() []byte {
			packet := []byte{'Q', 0, 0, 0, 0}
			binary.BigEndian.PutUint32(packet[1:], 0x40000003)
			return packet
		}()},
		{"Parse high truncated count", bytes.Join([][]byte{frontendPacket('P', []byte("s\x00SELECT 1\x00\x80\x00")), sync}, nil)},
		{"Parse trailing byte", bytes.Join([][]byte{frontendPacket('P', []byte("s\x00SELECT 1\x00\x00\x00x")), sync}, nil)},
		{"Bind trailing byte", bytes.Join([][]byte{parse, frontendPacket('B', []byte("p\x00s\x00\x00\x00\x00\x00\x00\x00x")), sync}, nil)},
		{"Execute trailing byte", bytes.Join([][]byte{parse, bind, frontendPacket('E', []byte("p\x00\x00\x00\x00\x00x")), sync}, nil)},
		{"Describe invalid target", bytes.Join([][]byte{frontendPacket('D', []byte{'X', 0}), sync}, nil)},
		{"Close invalid target", bytes.Join([][]byte{frontendPacket('C', []byte{'X', 0}), sync}, nil)},
		{"Close trailing byte", bytes.Join([][]byte{frontendPacket('C', []byte{'S', 0, 'x'}), sync}, nil)},
		{"Sync non-empty body", bytes.Join([][]byte{frontendPacket('S', []byte{'x'}), sync}, nil)},
		{"Flush non-empty body", bytes.Join([][]byte{frontendPacket('H', []byte{'x'}), sync}, nil)},
		{"Bind invalid format", bytes.Join([][]byte{
			frontendPacket('P', []byte("s\x00SELECT $1::text\x00\x00\x00")),
			frontendPacket('B', []byte{
				'p', 0, 's', 0,
				0, 1, 0, 2,
				0, 1, 0, 0, 0, 1, '1',
				0, 0,
			}),
			execute, sync,
		}, nil)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var postgresTrace []string
			for _, target := range setup.GetComparisonTargets(t) {
				trace := frontendTrace(t, ctx, target.Port, tc.wire)
				if target.Name == "postgres" {
					postgresTrace = trace
					continue
				}
				require.Equal(t, postgresTrace, trace,
					"Multigateway frontend response must match PostgreSQL")
			}
		})
	}
}
