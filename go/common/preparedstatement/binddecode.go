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
// portalInfo and returns it as a text string. callSite is the user-facing
// label (e.g. "set_config name") used in error messages so the
// diagnostic matches the caller's terminology.
//
// Accepted OIDs: TEXT, VARCHAR, InvalidOid (unspecified — PG infers from
// the function signature at execute time). For text-like OIDs the binary
// wire format is byte-identical to the text format (raw UTF-8), so a
// single branch covers both formats. Other OIDs are rejected — coercing
// arbitrary types to text at the caller would diverge from PG's own
// coercion and silently corrupt whatever state the caller is mirroring.
// NULL binds are rejected too; PG's set_config (the current sole caller)
// is STRICT and would no-op on NULL, leaving any gateway-side tracker out
// of sync.
//
// callSite is included in the error message verbatim. Pass something like
// "set_config name argument" so the error matches the planner's
// literal-rejection diagnostics for the same slot.
func DecodeBindAsText(portalInfo *PortalInfo, pr *ast.ParamRef, callSite string) (string, error) {
	raw, oid, format, err := lookupBind(portalInfo, pr, callSite)
	if err != nil {
		return "", err
	}
	switch oid {
	case ast.InvalidOid, ast.TEXTOID, ast.VARCHAROID:
		return string(raw), nil
	}
	return "", mterrors.NewFeatureNotSupported(
		fmt.Sprintf("%s bound parameter $%d has unsupported type oid=%d format=%d; declare the parameter as text",
			callSite, pr.Number, oid, format))
}

// DecodeBindAsBool reads the wire-protocol bind value at pr.Number from
// portalInfo and returns it as a bool. callSite is the user-facing label
// used in error messages.
//
// Text format mirrors PG's boolin spellings, including unique prefixes.
// Binary format is a single byte where 0 means false and non-zero means true.
// Other OIDs are rejected.
func DecodeBindAsBool(portalInfo *PortalInfo, pr *ast.ParamRef, callSite string) (bool, error) {
	raw, oid, format, err := lookupBind(portalInfo, pr, callSite)
	if err != nil {
		return false, err
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
// range, NULL, missing portal info).
func lookupBind(portalInfo *PortalInfo, pr *ast.ParamRef, callSite string) (raw []byte, oid ast.Oid, format int32, err error) {
	if portalInfo == nil || portalInfo.Portal == nil || portalInfo.PreparedStatementInfo == nil {
		return nil, ast.InvalidOid, 0, mterrors.NewFeatureNotSupported(
			fmt.Sprintf("%s bound parameter $%d cannot be resolved without a portal", callSite, pr.Number))
	}
	params := sqltypes.ParamsFromProto(portalInfo.Portal.ParamLengths, portalInfo.Portal.ParamValues)
	slot := pr.Number - 1
	if slot < 0 || slot >= len(params) {
		return nil, ast.InvalidOid, 0, mterrors.NewFeatureNotSupported(
			fmt.Sprintf("%s references bound parameter $%d, but the portal carries %d values",
				callSite, pr.Number, len(params)))
	}
	raw = params[slot]
	if raw == nil {
		return nil, ast.InvalidOid, 0, mterrors.NewFeatureNotSupported(
			fmt.Sprintf("%s bound parameter $%d cannot be NULL", callSite, pr.Number))
	}
	format = paramFormatFor(portalInfo.Portal.ParamFormats, slot)
	oid = paramOidFor(portalInfo.PreparedStatementInfo.PreparedStatement.GetParamTypes(), slot)
	return raw, oid, format, nil
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
