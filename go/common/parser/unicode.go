// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//  Portions Copyright (c) 2025, Supabase, Inc
//
//  Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//  Portions Copyright (c) 1994, The Regents of the University of California
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
//
/*
 * PostgreSQL Parser Lexer - Unicode Processing Support
 *
 * This file implements advanced Unicode processing functionality for the lexer,
 * including UTF-16 surrogate pair handling for extended string literals.
 * This provides PostgreSQL-compatible Unicode escape sequence processing.
 *
 * Ported from postgres/src/include/mb/pg_wchar.h and postgres/src/backend/parser/scan.l
 */

package parser

import "unicode/utf16"

// isUTF16SurrogateFirst checks if a Unicode code point is the first part of a UTF-16 surrogate pair
// Equivalent to postgres/src/include/mb/pg_wchar.h:541-544 (is_utf16_surrogate_first function)
func isUTF16SurrogateFirst(c rune) bool {
	return c >= 0xD800 && c <= 0xDBFF
}

// isUTF16SurrogateSecond checks if a Unicode code point is the second part of a UTF-16 surrogate pair
// Equivalent to postgres/src/include/mb/pg_wchar.h:547-550 (is_utf16_surrogate_second function)
func isUTF16SurrogateSecond(c rune) bool {
	return c >= 0xDC00 && c <= 0xDFFF
}

// isValidUnicodeCodepoint checks if a Unicode code point is valid
// Follows PostgreSQL's unicode validation rules
func isValidUnicodeCodepoint(c rune) bool {
	// Check for valid Unicode range (0 to 0x10FFFF)
	if c < 0 || c > 0x10FFFF {
		return false
	}

	// Surrogate pairs are not valid standalone code points
	if isUTF16SurrogateFirst(c) || isUTF16SurrogateSecond(c) {
		return false
	}

	return true
}

// validateSurrogatePair validates that two code points form a valid UTF-16 surrogate pair
// Returns the combined code point and whether the pair is valid
func validateSurrogatePair(first, second rune) (rune, bool) {
	if !isUTF16SurrogateFirst(first) {
		return 0, false
	}

	if !isUTF16SurrogateSecond(second) {
		return 0, false
	}

	combined := utf16.DecodeRune(first, second)
	return combined, isValidUnicodeCodepoint(combined)
}
