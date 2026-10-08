// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2026, Supabase, Inc
//
//	Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//	Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

package funcs

import (
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// The gateway represents cstring Datums as length-delimited BytesGetDatum
// payloads, without a required trailing NUL. If a NUL is present, input ends
// there, matching PG_GETARG_CSTRING. Protocol decoders remain responsible for
// rejecting embedded NUL in SQL text values before constructing a cstring.
func cstringArg(fcinfo fmgr.FunctionCallInfo) string {
	s := string(fcinfo.GetArgBytes(0))
	s, _, _ = strings.Cut(s, "\x00")
	return s
}

// int2in/int4in/int8in: int.c:63,287; int8.c:50. As elsewhere in the engine,
// errors are raised through pgerror, not the unported ErrorSaveContext API.
func integerIn[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	name, width := integerType[T]()
	return datum.Int64GetDatum(parseInteger(cstringArg(fcinfo), width, name))
}

// int2out/int4out/int8out: int.c:74,298; int8.c:61. strconv's base-10 output
// matches pg_itoa/pg_ltoa/pg_lltoa, including the most negative integer.
func integerOut[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.BytesGetDatum(strconv.AppendInt(nil, int64(integerArg[T](fcinfo, 0)), 10))
}

// parseInteger ports pg_strtoint{16,32,64}_safe's shared slow path
// (numutils.c:204-358,466-620,728-882, REL_17_6). A single scan handles all
// bases; retaining PG's *pre-digit* overflow check preserves which SQLSTATE
// wins when an overflowing number also has trailing invalid syntax.
// strconv.ParseInt alone is insufficient: PG treats a leading zero as decimal,
// allows separators in decimal input, and has different error precedence.
func parseInteger(input string, width int, typeName string) int64 {
	s := strings.TrimLeft(input, " \t\n\r\v\f")
	negative := false
	if len(s) > 0 && (s[0] == '-' || s[0] == '+') {
		negative = s[0] == '-'
		s = s[1:]
	}
	base := uint64(10)
	if len(s) >= 2 && s[0] == '0' {
		switch s[1] {
		case 'x', 'X':
			base = 16
		case 'o', 'O':
			base = 8
		case 'b', 'B':
			base = 2
		}
		if base != 10 {
			s = s[2:]
		}
	}
	maxMagnitude := uint64(1) << (width - 1)
	var magnitude uint64
	i := 0
	for i < len(s) {
		digit := integerDigit(s[i])
		if digit < base {
			if magnitude > maxMagnitude/base {
				integerInputRangeError(input, typeName)
			}
			magnitude = magnitude*base + digit
			i++
		} else if s[i] == '_' {
			// PG allows 0x_FF, 0o_77 and 0b_11, but not a leading
			// underscore in decimal. Every underscore must precede a digit.
			if (i == 0 && base == 10) || i+1 == len(s) || integerDigit(s[i+1]) >= base {
				integerInputSyntaxError(input, typeName)
			}
			i++
		} else {
			break
		}
	}
	if i == 0 || strings.Trim(s[i:], " \t\n\r\v\f") != "" {
		integerInputSyntaxError(input, typeName)
	}
	if negative {
		if magnitude > maxMagnitude {
			integerInputRangeError(input, typeName)
		}
		// Conversion and negation wrap at MinInt64, producing the intended
		// negative value without requiring a signed positive magnitude.
		return -int64(magnitude)
	}
	if magnitude >= maxMagnitude {
		integerInputRangeError(input, typeName)
	}
	return int64(magnitude)
}

func integerDigit(c byte) uint64 {
	switch {
	case c >= '0' && c <= '9':
		return uint64(c - '0')
	case c >= 'a' && c <= 'f':
		return uint64(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return uint64(c-'A') + 10
	default:
		return 16
	}
}

func integerInputSyntaxError(input, typeName string) {
	pgerror.Ereportf(mterrors.PgSSInvalidTextRepresentation,
		"invalid input syntax for type %s: \"%s\"", typeName, input)
}

func integerInputRangeError(input, typeName string) {
	pgerror.Ereportf(mterrors.PgSSNumericValueOutOfRange,
		"value \"%s\" is out of range for type %s", input, typeName)
}
