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

package preparedstatement

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/protoutil"
)

// buildTestPortalInfo wires a *PortalInfo for the given prepared SQL +
// binds using the same factories the real Bind path uses, keeping the
// test path byte-equivalent to a production portal.
func buildTestPortalInfo(t *testing.T, sql string, paramTypes []uint32, params [][]byte, paramFormats []int16) *PortalInfo {
	t.Helper()
	psi, err := NewPreparedStatementInfo(protoutil.NewPreparedStatement("stmt", sql, paramTypes))
	require.NoError(t, err)
	portal := protoutil.NewPortal("", "stmt", params, paramFormats, nil)
	return NewPortalInfo(psi, portal)
}

func paramRef(n int) *ast.ParamRef { return &ast.ParamRef{Number: n} }

// TestDecodeBindAsText_AcceptedOIDs covers the byte-trivial path: TEXT,
// VARCHAR, and InvalidOid (unspecified) all decode the raw bytes as
// UTF-8 text. Binary wire format is identical to text format for these
// type OIDs in PG, so a single branch covers both formats.
func TestDecodeBindAsText_AcceptedOIDs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		oid    uint32
		format int16
	}{
		{"text/text format", uint32(ast.TEXTOID), 0},
		{"text/binary format", uint32(ast.TEXTOID), 1},
		{"varchar/text format", uint32(ast.VARCHAROID), 0},
		{"unspecified/text format", uint32(ast.InvalidOid), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pi := buildTestPortalInfo(t, "SELECT $1", []uint32{tc.oid}, [][]byte{[]byte("public,extensions")}, []int16{tc.format})
			got, err := DecodeBindAsText(pi, paramRef(1), "test arg")
			require.NoError(t, err)
			assert.Equal(t, "public,extensions", got)
		})
	}
}

// TestDecodeBindAsText_RejectsUnsupportedOID — gateway must not invent
// type coercion. Int4 bytes can't be safely interpreted as text without
// PG-specific coercion semantics, so reject with a message that tells
// the client how to fix it.
func TestDecodeBindAsText_RejectsUnsupportedOID(t *testing.T) {
	const oidInt4 uint32 = 23
	pi := buildTestPortalInfo(t, "SELECT $1", []uint32{oidInt4}, [][]byte{[]byte("123")}, []int16{0})
	_, err := DecodeBindAsText(pi, paramRef(1), "test arg")
	require.Error(t, err)
	assertFeatureErr(t, err, "unsupported type oid=23")
	assertFeatureErr(t, err, "declare the parameter as text")
}

// TestDecodeBindAsBool_AcceptedSpellings — text-format bool mirrors PG's
// boolin spellings exactly. Each accepted spelling must decode to the
// right Go bool, case-insensitive, with leading/trailing whitespace
// trimmed (PG behavior).
func TestDecodeBindAsBool_AcceptedSpellings(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want bool
	}{
		{"true", true},
		{"TRUE", true},
		{"t", true},
		{"tr", true},
		{"y", true},
		{"ye", true},
		{"yes", true},
		{"on", true},
		{"1", true},
		{"  true  ", true},
		{"false", false},
		{"FALSE", false},
		{"f", false},
		{"fa", false},
		{"n", false},
		{"no", false},
		{"of", false},
		{"off", false},
		{"0", false},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			pi := buildTestPortalInfo(t, "SELECT $1", []uint32{uint32(ast.BOOLOID)}, [][]byte{[]byte(tc.raw)}, []int16{0})
			got, err := DecodeBindAsBool(pi, paramRef(1), "test arg")
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestDecodeBindAsBool_Binary — binary wire format encodes bool as a
// single byte, 0=false anything-else=true. Length must be exactly 1.
func TestDecodeBindAsBool_Binary(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
		want bool
	}{
		{"binary 0", []byte{0}, false},
		{"binary 1", []byte{1}, true},
		{"binary 255", []byte{255}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pi := buildTestPortalInfo(t, "SELECT $1", []uint32{uint32(ast.BOOLOID)}, [][]byte{tc.raw}, []int16{1})
			got, err := DecodeBindAsBool(pi, paramRef(1), "test arg")
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestDecodeBindAsBool_InvalidBinaryLength — binary bool must be exactly
// 1 byte. Anything else is a protocol violation by the client; reject
// rather than silently using the first byte.
func TestDecodeBindAsBool_InvalidBinaryLength(t *testing.T) {
	pi := buildTestPortalInfo(t, "SELECT $1", []uint32{uint32(ast.BOOLOID)}, [][]byte{{1, 2}}, []int16{1})
	_, err := DecodeBindAsBool(pi, paramRef(1), "test arg")
	require.Error(t, err)
	assertFeatureErr(t, err, "invalid binary bool length")
}

// TestDecodeBindAsBool_InvalidSpelling — text-format bool that isn't one
// of PG's boolin spellings must error rather than guess.
func TestDecodeBindAsBool_InvalidSpelling(t *testing.T) {
	for _, raw := range []string{"maybe", "o"} {
		t.Run(raw, func(t *testing.T) {
			pi := buildTestPortalInfo(t, "SELECT $1", []uint32{uint32(ast.BOOLOID)}, [][]byte{[]byte(raw)}, []int16{0})
			_, err := DecodeBindAsBool(pi, paramRef(1), "test arg")
			require.Error(t, err)
			assertFeatureErr(t, err, "invalid boolean value")
		})
	}
}

// TestDecodeBind_NullRejected covers both text and bool paths — NULL
// binds are rejected uniformly because the caller (set_config) is STRICT
// and the only safe gateway-side response is to refuse rather than
// silently desync with PG.
func TestDecodeBind_NullRejected(t *testing.T) {
	t.Run("text", func(t *testing.T) {
		pi := buildTestPortalInfo(t, "SELECT $1", []uint32{uint32(ast.TEXTOID)}, [][]byte{nil}, []int16{0})
		_, err := DecodeBindAsText(pi, paramRef(1), "test arg")
		require.Error(t, err)
		assertFeatureErr(t, err, "cannot be NULL")
	})
	t.Run("bool", func(t *testing.T) {
		pi := buildTestPortalInfo(t, "SELECT $1", []uint32{uint32(ast.BOOLOID)}, [][]byte{nil}, []int16{0})
		_, err := DecodeBindAsBool(pi, paramRef(1), "test arg")
		require.Error(t, err)
		assertFeatureErr(t, err, "cannot be NULL")
	})
}

// TestDecodeBind_OutOfRange — ParamRef.Number beyond the portal's bind
// count is a malformed client request or a planner bug. Surface
// explicitly rather than panicking on slice index.
func TestDecodeBind_OutOfRange(t *testing.T) {
	pi := buildTestPortalInfo(t, "SELECT $1", []uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("only one")}, []int16{0})
	_, err := DecodeBindAsText(pi, paramRef(2), "test arg")
	require.Error(t, err)
	assertFeatureErr(t, err, "but the portal carries 1 values")
}

// TestDecodeBind_NilPortal — defensive path for accidental nil portal.
// Should never happen in production (planner always emits BindRefs only
// when the executor has a portalInfo) but failing fast is safer than NPE.
func TestDecodeBind_NilPortal(t *testing.T) {
	_, err := DecodeBindAsText(nil, paramRef(1), "test arg")
	require.Error(t, err)
	assertFeatureErr(t, err, "cannot be resolved without a portal")
}

// TestParamFormatFor — the Bind message format-list shapes per PG spec:
// empty = all text, single = applies to all, per-param = use index.
func TestParamFormatFor(t *testing.T) {
	assert.Equal(t, int32(0), paramFormatFor(nil, 0), "empty defaults to text")
	assert.Equal(t, int32(0), paramFormatFor([]int32{}, 5), "empty defaults to text regardless of index")
	assert.Equal(t, int32(1), paramFormatFor([]int32{1}, 0), "single applies to all")
	assert.Equal(t, int32(1), paramFormatFor([]int32{1}, 99), "single applies to any index")
	assert.Equal(t, int32(0), paramFormatFor([]int32{0, 1, 0}, 0))
	assert.Equal(t, int32(1), paramFormatFor([]int32{0, 1, 0}, 1))
	assert.Equal(t, int32(0), paramFormatFor([]int32{0, 1}, 5), "out-of-range falls back to text")
}

// TestParamOidFor — declared param types may have fewer entries than $N;
// missing slots default to InvalidOid so unspecified-type clients still
// work.
func TestParamOidFor(t *testing.T) {
	assert.Equal(t, ast.InvalidOid, paramOidFor(nil, 0))
	assert.Equal(t, ast.InvalidOid, paramOidFor([]uint32{}, 0))
	assert.Equal(t, ast.TEXTOID, paramOidFor([]uint32{uint32(ast.TEXTOID)}, 0))
	assert.Equal(t, ast.InvalidOid, paramOidFor([]uint32{uint32(ast.TEXTOID)}, 1), "missing tail defaults to unspecified")
}

func assertFeatureErr(t *testing.T, err error, contains string) {
	t.Helper()
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "expected *mterrors.PgDiagnostic, got %T", err)
	assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
	assert.Contains(t, diag.Message, contains)
}
