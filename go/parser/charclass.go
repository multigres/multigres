/*
 * PostgreSQL Parser Lexer - Character Classification System
 *
 * This file implements an optimized character classification system using
 * lookup tables instead of function calls for better performance.
 * Based on PostgreSQL's character class definitions in scan.l
 */

package parser

// CharClass represents character classification flags using bit fields
type CharClass uint16

const (
	// Character class flags - can be combined with bitwise OR
	ClassDigit       CharClass = 1 << iota // 0-9
	ClassAlpha                             // a-z, A-Z
	ClassIdentStart                        // Characters that can start identifiers
	ClassIdentCont                         // Characters that can continue identifiers
	ClassWhitespace                        // Whitespace characters
	ClassHexDigit                          // 0-9, a-f, A-F
	ClassOctalDigit                        // 0-7
	ClassBinaryDigit                       // 0-1
	ClassSelfChar                          // PostgreSQL "self" characters (single-char tokens)
	ClassOpChar                            // Characters that can be part of operators
	ClassHighBit                           // Characters with high bit set (>= 0x80)
)

// Character classification lookup table - pre-computed for all 256 byte values
// This provides O(1) character classification without function call overhead
var charClassTable [256]CharClass

// Initialize the character classification table
func init() {
	// Initialize all entries to zero
	for i := range charClassTable {
		charClassTable[i] = 0
	}

	// Digits: 0-9
	for b := byte('0'); b <= '9'; b++ {
		charClassTable[b] |= ClassDigit | ClassHexDigit | ClassIdentCont
		if b <= '7' {
			charClassTable[b] |= ClassOctalDigit
		}
		if b <= '1' {
			charClassTable[b] |= ClassBinaryDigit
		}
	}

	// Lowercase letters: a-z
	for b := byte('a'); b <= 'z'; b++ {
		charClassTable[b] |= ClassAlpha | ClassIdentStart | ClassIdentCont
		if b <= 'f' {
			charClassTable[b] |= ClassHexDigit
		}
	}

	// Uppercase letters: A-Z
	for b := byte('A'); b <= 'Z'; b++ {
		charClassTable[b] |= ClassAlpha | ClassIdentStart | ClassIdentCont
		if b <= 'F' {
			charClassTable[b] |= ClassHexDigit
		}
	}

	// Underscore: special identifier character
	charClassTable['_'] |= ClassIdentStart | ClassIdentCont

	// Dollar sign: can continue identifiers (PostgreSQL extension)
	charClassTable['$'] |= ClassIdentCont

	// High-bit characters (>= 0x80): can start/continue identifiers
	for b := 0x80; b <= 0xFF; b++ {
		charClassTable[b] |= ClassHighBit | ClassIdentStart | ClassIdentCont
	}

	// Whitespace characters: space, tab, newline, carriage return, form feed, vertical tab
	// Equivalent to PostgreSQL's space: [ \t\n\r\f\v] - postgres/src/backend/parser/scan.l:222
	charClassTable[' '] |= ClassWhitespace
	charClassTable['\t'] |= ClassWhitespace
	charClassTable['\n'] |= ClassWhitespace
	charClassTable['\r'] |= ClassWhitespace
	charClassTable['\f'] |= ClassWhitespace
	charClassTable['\v'] |= ClassWhitespace

	// PostgreSQL "self" characters (single-character tokens)
	// Equivalent to PostgreSQL's self: [,()\[\].;\:\+\-\*\/\%\^\<\>\=] - postgres/src/backend/parser/scan.l:380
	selfChars := ",()[].:;+-*/%^<>="
	for _, b := range []byte(selfChars) {
		charClassTable[b] |= ClassSelfChar
	}

	// PostgreSQL operator characters
	// Equivalent to PostgreSQL's op_chars: [\~\!\@\#\^\&\|\`\?\+\-\*\/\%\<\>\=] - postgres/src/backend/parser/scan.l:381
	opChars := "~!@#^&|`?+-*/%<>="
	for _, b := range []byte(opChars) {
		charClassTable[b] |= ClassOpChar
	}
}

// Optimized character classification functions using lookup table

// IsDigit checks if a byte is a decimal digit (0-9)
func IsDigit(b byte) bool {
	return charClassTable[b]&ClassDigit != 0
}

// IsAlpha checks if a byte is alphabetic (a-z, A-Z)
func IsAlpha(b byte) bool {
	return charClassTable[b]&ClassAlpha != 0
}

// IsIdentStart checks if a byte can start an identifier
// Equivalent to PostgreSQL's ident_start: [A-Za-z\200-\377_] - postgres/src/backend/parser/scan.l:346
func IsIdentStart(b byte) bool {
	return charClassTable[b]&ClassIdentStart != 0
}

// IsIdentCont checks if a byte can continue an identifier
// Equivalent to PostgreSQL's ident_cont: [A-Za-z\200-\377_0-9\$] - postgres/src/backend/parser/scan.l:347
func IsIdentCont(b byte) bool {
	return charClassTable[b]&ClassIdentCont != 0
}

// IsWhitespace checks if a byte is whitespace
// Equivalent to PostgreSQL's space: [ \t\n\r\f\v] - postgres/src/backend/parser/scan.l:222
func IsWhitespace(b byte) bool {
	return charClassTable[b]&ClassWhitespace != 0
}

// IsHexDigit checks if a byte is a hexadecimal digit (0-9, a-f, A-F)
func IsHexDigit(b byte) bool {
	return charClassTable[b]&ClassHexDigit != 0
}

// IsOctalDigit checks if a byte is an octal digit (0-7)
func IsOctalDigit(b byte) bool {
	return charClassTable[b]&ClassOctalDigit != 0
}

// IsBinaryDigit checks if a byte is a binary digit (0-1)
func IsBinaryDigit(b byte) bool {
	return charClassTable[b]&ClassBinaryDigit != 0
}

// IsSelfChar checks if a byte is a PostgreSQL "self" character (single-char token)
// Equivalent to PostgreSQL's self: [,()\[\].;\:\+\-\*\/\%\^\<\>\=] - postgres/src/backend/parser/scan.l:380
func IsSelfChar(b byte) bool {
	return charClassTable[b]&ClassSelfChar != 0
}

// IsOpChar checks if a byte can be part of a PostgreSQL operator
// Equivalent to PostgreSQL's op_chars: [\~\!\@\#\^\&\|\`\?\+\-\*\/\%\<\>\=] - postgres/src/backend/parser/scan.l:381
func IsOpChar(b byte) bool {
	return charClassTable[b]&ClassOpChar != 0
}

// IsAlphaNumeric checks if a byte is alphanumeric (for backward compatibility)
func IsAlphaNumeric(b byte) bool {
	return charClassTable[b]&(ClassAlpha|ClassDigit) != 0
}

// IsHighBit checks if a byte has the high bit set (>= 0x80)
func IsHighBit(b byte) bool {
	return charClassTable[b]&ClassHighBit != 0
}

// GetCharClass returns the character class flags for a byte
// This can be used for more complex character classification logic
func GetCharClass(b byte) CharClass {
	return charClassTable[b]
}

// HasCharClass checks if a byte has any of the specified character class flags
func HasCharClass(b byte, class CharClass) bool {
	return charClassTable[b]&class != 0
}
