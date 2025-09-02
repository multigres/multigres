// Package ast provides interval-related constants.
// Ported from postgres/src/include/utils/datetime.h and utils/timestamp.h
package ast

// Interval field constants - ported from postgres/src/include/utils/datetime.h:91-102
// These represent the different date/time fields that can be used in INTERVAL expressions
const (
	RESERV      = 0  // Reserved
	MONTH       = 1  // Month field
	YEAR        = 2  // Year field  
	DAY         = 3  // Day field
	JULIAN      = 4  // Julian day
	TZ          = 5  // Fixed-offset timezone abbreviation
	DTZ         = 6  // Fixed-offset timezone abbrev, DST
	DYNTZ       = 7  // Dynamic timezone abbreviation
	IGNORE_DTF  = 8  // Ignore DTF
	AMPM        = 9  // AM/PM indicator
	HOUR        = 10 // Hour field
	MINUTE      = 11 // Minute field
	SECOND      = 12 // Second field
	MILLISECOND = 13 // Millisecond field
	MICROSECOND = 14 // Microsecond field
	DOY         = 15 // Day of year
	DOW         = 16 // Day of week
	UNITS       = 17 // Units
	ADBC        = 18 // AD/BC indicator
	// These are only for relative dates
	AGO         = 19 // Ago modifier
	ABS_BEFORE  = 20 // Absolute before
	ABS_AFTER   = 21 // Absolute after
	// Generic fields to help with parsing
	ISODATE     = 22 // ISO date format
	ISOTIME     = 23 // ISO time format
	// These are only for parsing intervals
	WEEK        = 24 // Week field
	DECADE      = 25 // Decade field
	CENTURY     = 26 // Century field
)

// IntervalMask returns the bit mask for an interval field
// Ported from postgres/src/include/utils/timestamp.h:73
func IntervalMask(field int) int {
	return 1 << field
}

// Interval range and precision constants
// Ported from postgres/src/include/utils/timestamp.h:76-82
const (
	INTERVAL_FULL_RANGE     = 0x7FFF  // Full range mask
	INTERVAL_RANGE_MASK     = 0x7FFF  // Range mask for extracting range
	INTERVAL_FULL_PRECISION = 0xFFFF  // Full precision mask
	INTERVAL_PRECISION_MASK = 0xFFFF  // Precision mask for extracting precision
)

// IntervalTypmod creates a typmod value from precision and range
// Ported from postgres/src/include/utils/timestamp.h:80
func IntervalTypmod(precision, rangeVal int) int {
	return ((rangeVal & INTERVAL_RANGE_MASK) << 16) | (precision & INTERVAL_PRECISION_MASK)
}

// IntervalPrecision extracts precision from typmod
// Ported from postgres/src/include/utils/timestamp.h:81
func IntervalPrecision(typmod int) int {
	return typmod & INTERVAL_PRECISION_MASK
}

// IntervalRange extracts range from typmod
// Ported from postgres/src/include/utils/timestamp.h:82
func IntervalRange(typmod int) int {
	return (typmod >> 16) & INTERVAL_RANGE_MASK
}

// Common interval mask combinations for convenience
var (
	INTERVAL_MASK_YEAR   = IntervalMask(YEAR)     // 1 << 2 = 4
	INTERVAL_MASK_MONTH  = IntervalMask(MONTH)    // 1 << 1 = 2
	INTERVAL_MASK_DAY    = IntervalMask(DAY)      // 1 << 3 = 8
	INTERVAL_MASK_HOUR   = IntervalMask(HOUR)     // 1 << 10 = 1024
	INTERVAL_MASK_MINUTE = IntervalMask(MINUTE)   // 1 << 11 = 2048
	INTERVAL_MASK_SECOND = IntervalMask(SECOND)   // 1 << 12 = 4096
)