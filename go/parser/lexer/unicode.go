/*
 * PostgreSQL Parser Lexer - Unicode Processing Support
 *
 * This file implements advanced Unicode processing functionality for the lexer,
 * including UTF-16 surrogate pair handling for extended string literals.
 * This provides PostgreSQL-compatible Unicode escape sequence processing.
 * 
 * Ported from postgres/src/include/mb/pg_wchar.h and postgres/src/backend/parser/scan.l
 */

package lexer

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

// surrogatePairToCodepoint combines two UTF-16 surrogate code points into a single Unicode code point
// Equivalent to postgres/src/include/mb/pg_wchar.h:553-556 (surrogate_pair_to_codepoint function)
func surrogatePairToCodepoint(first, second rune) rune {
	return ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF)
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
	
	combined := surrogatePairToCodepoint(first, second)
	return combined, isValidUnicodeCodepoint(combined)
}