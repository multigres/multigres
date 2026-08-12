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
	"fmt"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// DecodeBindAsText reads the wire-protocol bind value at pr.Number from
// portalInfo and returns it as a text string, rejecting a NULL bind. Callers
// that can act on a NULL (PostgreSQL semantics rather than an error) should
// use DecodeBindAsTextOrNull instead.
//
// Accepted OIDs: TEXT, VARCHAR, InvalidOid (unspecified — PG infers from
// the function signature at execute time). For text-like OIDs the binary
// wire format is byte-identical to the text format (raw UTF-8), so a
// single branch covers both formats.
//
// Other OIDs are rejected, and that rejection is deliberately FAIL-CLOSED
// rather than a pass-through to PostgreSQL. The declared parameter OID is
// client-controlled, so "if we cannot decode it, let PG handle it" would let
// a client bind a policy-relevant argument (a set_config name or search_path
// value) under an exotic-but-coercible OID — NAMEOID, say — to skip the
// gateway's guards while PostgreSQL happily coerces and applies it. That
// would reopen the pg_temp / gateway-managed / restricted-GUC bypasses those
// guards exist to close. Coercing arbitrary types to text here instead would
// diverge from PG's own coercion and silently corrupt whatever state the
// caller is mirroring, so the statement is refused.
//
// callSite is included in the error message verbatim. Pass something like
// "set_config name argument" so the error matches the planner's
// literal-rejection diagnostics for the same slot.
func DecodeBindAsText(portalInfo *PortalInfo, pr *ast.ParamRef, callSite string) (string, error) {
	value, isNull, err := DecodeBindAsTextOrNull(portalInfo, pr, callSite)
	if err != nil {
		return "", err
	}
	if isNull {
		return "", nullBindError(pr, callSite)
	}
	return value, nil
}

// DecodeBindAsTextOrNull is DecodeBindAsText except that a NULL bind is
// reported via the returned flag instead of an error, letting the caller
// mirror PostgreSQL's own NULL semantics for the slot.
//
// This exists because set_config is NOT strict (pg_proc.proisstrict = false):
// set_config(name, NULL, is_local) resets the parameter to its default and
// returns that default — verified against PostgreSQL 17 — so a caller
// tracking session state must record a reset, not fail the statement.
// The OID restriction above still applies to a non-NULL value.
func DecodeBindAsTextOrNull(portalInfo *PortalInfo, pr *ast.ParamRef, callSite string) (value string, isNull bool, err error) {
	raw, oid, format, isNull, err := lookupBind(portalInfo, pr, callSite)
	if err != nil || isNull {
		return "", isNull, err
	}
	switch oid {
	case ast.InvalidOid, ast.TEXTOID, ast.VARCHAROID:
		return string(raw), false, nil
	}
	return "", false, mterrors.NewFeatureNotSupported(
		fmt.Sprintf("%s bound parameter $%d has unsupported type oid=%d format=%d; declare the parameter as text",
			callSite, pr.Number, oid, format))
}

func nullBindError(pr *ast.ParamRef, callSite string) error {
	return mterrors.NewFeatureNotSupported(
		fmt.Sprintf("%s bound parameter $%d cannot be NULL", callSite, pr.Number))
}

// DecodeBindAsBool reads the wire-protocol bind value at pr.Number from
// portalInfo and returns it as a bool. callSite is the user-facing label
// used in error messages.
//
// Text format mirrors PG's boolin spellings, including unique prefixes.
// Binary format is a single byte where 0 means false and non-zero means true.
// Other OIDs are rejected.
func DecodeBindAsBool(portalInfo *PortalInfo, pr *ast.ParamRef, callSite string) (bool, error) {
	raw, oid, format, isNull, err := lookupBind(portalInfo, pr, callSite)
	if err != nil {
		return false, err
	}
	if isNull {
		// PostgreSQL coerces a NULL is_local to false, but the only slot the
		// gateway allows to be bound here belongs to a gateway-managed
		// variable, whose tracked value must be exact — refuse rather than
		// guess. Fail-closed; see DecodeBindAsText.
		return false, nullBindError(pr, callSite)
	}
	switch oid {
	case ast.InvalidOid, ast.BOOLOID:
	default:
		return false, mterrors.NewFeatureNotSupported(
			fmt.Sprintf("%s bound parameter $%d has unsupported type oid=%d; declare the parameter as bool",
				callSite, pr.Number, oid))
	}

	if format == 1 {
		if len(raw) != 1 {
			return false, mterrors.NewFeatureNotSupported(
				fmt.Sprintf("%s bound parameter $%d has invalid binary bool length %d",
					callSite, pr.Number, len(raw)))
		}
		return raw[0] != 0, nil
	}
	if b, ok := sqltypes.ParseBool(string(raw)); ok {
		return b, nil
	}
	return false, mterrors.NewFeatureNotSupported(
		fmt.Sprintf("%s bound parameter $%d has invalid boolean value", callSite, pr.Number))
}

// lookupBind resolves a ParamRef to its raw bytes, declared OID, and wire
// format from the portal. Centralizes the per-slot bookkeeping so the text
// and bool decoders share a single source of bind-level errors (out of
// range, missing portal info). A NULL bind is reported via isNull rather
// than an error so each decoder can apply its own NULL policy.
func lookupBind(portalInfo *PortalInfo, pr *ast.ParamRef, callSite string) (raw []byte, oid ast.Oid, format int32, isNull bool, err error) {
	if portalInfo == nil || portalInfo.Portal == nil || portalInfo.PreparedStatementInfo == nil {
		return nil, ast.InvalidOid, 0, false, mterrors.NewFeatureNotSupported(
			fmt.Sprintf("%s bound parameter $%d cannot be resolved without a portal", callSite, pr.Number))
	}
	params := sqltypes.ParamsFromProto(portalInfo.Portal.ParamLengths, portalInfo.Portal.ParamValues)
	slot := pr.Number - 1
	if slot < 0 || slot >= len(params) {
		return nil, ast.InvalidOid, 0, false, mterrors.NewFeatureNotSupported(
			fmt.Sprintf("%s references bound parameter $%d, but the portal carries %d values",
				callSite, pr.Number, len(params)))
	}
	raw = params[slot]
	if raw == nil {
		return nil, ast.InvalidOid, 0, true, nil
	}
	format = paramFormatFor(portalInfo.Portal.ParamFormats, slot)
	oid = paramOidFor(portalInfo.PreparedStatementInfo.PreparedStatement.GetParamTypes(), slot)
	return raw, oid, format, false, nil
}

// paramFormatFor returns the wire format code (0=text, 1=binary) for the
// parameter at index i. The Bind message permits an empty format list (all
// text), a single-element list (applies to all), or one per parameter.
func paramFormatFor(formats []int32, i int) int32 {
	switch len(formats) {
	case 0:
		return 0
	case 1:
		return formats[0]
	}
	if i < len(formats) {
		return formats[i]
	}
	return 0
}

// paramOidFor returns the declared parameter OID at index i, or
// ast.InvalidOid when the client did not declare it (Parse with an empty
// ParameterTypes list, or fewer entries than $N).
func paramOidFor(paramTypes []uint32, i int) ast.Oid {
	if i < len(paramTypes) {
		return ast.Oid(paramTypes[i])
	}
	return ast.InvalidOid
}
