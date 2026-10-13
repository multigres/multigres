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

package pgeval

import (
	"bytes"
	"context"
	"math/rand/v2"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/funcs"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// INTERNAL arguments cannot be supplied from SQL. Send raw binary Bind values
// instead, bypassing pgx's encoders so malformed payloads reach PostgreSQL's
// actual typreceive function. The local side still invokes the builtin directly.
func compareBinaryReceive(t *testing.T, ctx context.Context, conn *pgx.Conn, proc *pgcatalog.Proc, info *fmgr.FmgrInfo, rng *rand.Rand) int {
	t.Helper()
	typ := pgcatalog.TypeByOid(proc.RetType)
	require.Equal(t, proc.Oid, typ.Receive)
	var receiveOID uint32
	require.NoError(t, conn.QueryRow(ctx, "SELECT typreceive::oid FROM pg_catalog.pg_type WHERE oid=$1::oid", uint32(typ.Oid)).Scan(&receiveOID))
	require.Equal(t, uint32(proc.Oid), receiveOID)
	width := int(typ.Len)
	require.Positive(t, width)

	payloads := [][]byte{nil, {}} // NULL and empty are distinct protocol values.
	for _, value := range scalarSamples(typ.Oid, false) {
		if value == nil {
			continue
		}
		data, err := conn.TypeMap().Encode(uint32(typ.Oid), pgtype.BinaryFormatCode, value, nil)
		require.NoError(t, err)
		payloads = append(payloads, data, append(bytes.Clone(data), 127))
		for n := range len(data) {
			payloads = append(payloads, data[:n])
		}
	}
	if typ.Oid == pgoid.BOOLOID {
		for n := 0; n <= 255; n++ {
			payloads = append(payloads, []byte{byte(n)})
		}
	}
	for range 100 {
		data := make([]byte, rng.IntN(width+4))
		for i := range data {
			data[i] = byte(rng.Uint32())
		}
		payloads = append(payloads, data)
	}

	comparisons := 0
	query := "SELECT $1::" + (pgx.Identifier{"pg_catalog", typ.Name}).Sanitize()
	exec := func(data []byte) *pgconn.Result {
		comparisons++
		return conn.PgConn().ExecParams(ctx, query, [][]byte{data}, []uint32{uint32(typ.Oid)},
			[]int16{pgtype.BinaryFormatCode}, []int16{pgtype.BinaryFormatCode}).Read()
	}
	for _, data := range payloads {
		fc := fmgr.NewFunctionCallInfo(info, 1, pgoid.InvalidOid)
		input := funcs.NewBinaryInput(data)
		fc.Args[0] = datum.NullableDatum{Value: input.Datum(), IsNull: data == nil}
		var local datum.Datum
		localErr := pgerror.Recover(func() { local = fmgr.CallFunction(fc) })
		reference := exec(data)
		if data != nil && len(data) > width {
			// recv must leave trailing bytes for a containing decoder. Bind
			// rejects those bytes separately (postgres.c:1945); it is not an
			// error thrown by the builtin. Check both that rejection and the
			// decoded prefix, rather than making recv enforce Bind framing.
			require.NoError(t, localErr, "payload: %x", data)
			require.Equal(t, len(data)-width, input.Remaining())
			var pgErr *pgconn.PgError
			require.ErrorAs(t, reference.Err, &pgErr)
			require.Equal(t, "22P03", pgErr.Code)
			require.Equal(t, "incorrect binary data format in bind parameter 1", pgErr.Message)
			require.Equal(t, "ERROR", pgErr.Severity)
			reference = exec(data[:width])
		}
		if reference.Err != nil {
			var pgErr *pgconn.PgError
			require.ErrorAs(t, reference.Err, &pgErr, "payload: %x", data)
			var diag *mterrors.PgDiagnostic
			require.ErrorAs(t, localErr, &diag, "payload: %x", data)
			require.Equal(t, pgErr.Code, diag.Code)
			require.Equal(t, pgErr.Message, diag.Message)
			require.Equal(t, pgErr.Severity, diag.Severity)
			require.Equal(t, len(data), input.Remaining(), "failed read must not advance")
			continue
		}
		require.NoError(t, localErr, "payload: %x", data)
		require.Len(t, reference.FieldDescriptions, 1)
		require.Equal(t, uint32(typ.Oid), reference.FieldDescriptions[0].DataTypeOID)
		require.Equal(t, int16(pgtype.BinaryFormatCode), reference.FieldDescriptions[0].Format)
		require.Len(t, reference.Rows, 1)
		require.Len(t, reference.Rows[0], 1)
		require.Equal(t, data == nil, fc.IsNull)
		if data == nil {
			require.Nil(t, reference.Rows[0][0])
			continue
		}
		require.Equal(t, len(data)-width, input.Remaining())
		codec, ok := conn.TypeMap().TypeForOID(uint32(typ.Oid))
		require.True(t, ok)
		want, err := codec.Codec.DecodeValue(conn.TypeMap(), uint32(typ.Oid), pgtype.BinaryFormatCode, reference.Rows[0][0])
		require.NoError(t, err)
		require.Equal(t, want, scalarResult(t, typ.Oid, local), "payload: %x", data)
	}
	return comparisons
}
