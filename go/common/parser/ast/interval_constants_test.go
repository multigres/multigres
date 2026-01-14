// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2025, Supabase, Inc
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
package ast

import (
	"testing"
)

func TestIntervalConstants(t *testing.T) {
	tests := []struct {
		name     string
		field    int
		expected int
	}{
		{"YEAR", YEAR, 2},
		{"MONTH", MONTH, 1},
		{"DAY", DAY, 3},
		{"HOUR", HOUR, 10},
		{"MINUTE", MINUTE, 11},
		{"SECOND", SECOND, 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.field != tt.expected {
				t.Errorf("Constant %s = %d, expected %d", tt.name, tt.field, tt.expected)
			}
		})
	}
}

func TestIntervalMask(t *testing.T) {
	tests := []struct {
		name     string
		field    int
		expected int
	}{
		{"YEAR", YEAR, 4},        // 1 << 2
		{"MONTH", MONTH, 2},      // 1 << 1
		{"DAY", DAY, 8},          // 1 << 3
		{"HOUR", HOUR, 1024},     // 1 << 10
		{"MINUTE", MINUTE, 2048}, // 1 << 11
		{"SECOND", SECOND, 4096}, // 1 << 12
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mask := IntervalMask(tt.field)
			if mask != tt.expected {
				t.Errorf("IntervalMask(%s) = %d, expected %d", tt.name, mask, tt.expected)
			}
		})
	}
}

func TestIntervalMaskConstants(t *testing.T) {
	tests := []struct {
		name     string
		mask     int
		expected int
	}{
		{"INTERVAL_MASK_YEAR", INTERVAL_MASK_YEAR, 4},
		{"INTERVAL_MASK_MONTH", INTERVAL_MASK_MONTH, 2},
		{"INTERVAL_MASK_DAY", INTERVAL_MASK_DAY, 8},
		{"INTERVAL_MASK_HOUR", INTERVAL_MASK_HOUR, 1024},
		{"INTERVAL_MASK_MINUTE", INTERVAL_MASK_MINUTE, 2048},
		{"INTERVAL_MASK_SECOND", INTERVAL_MASK_SECOND, 4096},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mask != tt.expected {
				t.Errorf("Constant %s = %d, expected %d", tt.name, tt.mask, tt.expected)
			}
		})
	}
}

func TestIntervalRange(t *testing.T) {
	// Test INTERVAL_FULL_RANGE constant
	if INTERVAL_FULL_RANGE != 0x7FFF {
		t.Errorf("INTERVAL_FULL_RANGE = %d, expected %d", INTERVAL_FULL_RANGE, 0x7FFF)
	}

	// Test IntervalTypmod function
	precision := 6
	rangeVal := 1023
	typmod := IntervalTypmod(precision, rangeVal)

	// Verify we can extract precision and range correctly
	extractedPrecision := IntervalPrecision(typmod)
	extractedRange := IntervalRange(typmod)

	if extractedPrecision != precision {
		t.Errorf("IntervalPrecision(%d) = %d, expected %d", typmod, extractedPrecision, precision)
	}

	if extractedRange != rangeVal {
		t.Errorf("IntervalRange(%d) = %d, expected %d", typmod, extractedRange, rangeVal)
	}
}

func TestIntervalMaskCombinations(t *testing.T) {
	// Test common combinations
	yearMonth := INTERVAL_MASK_YEAR | INTERVAL_MASK_MONTH
	expectedYearMonth := 4 | 2 // 6
	if yearMonth != expectedYearMonth {
		t.Errorf("YEAR | MONTH = %d, expected %d", yearMonth, expectedYearMonth)
	}

	dayHour := INTERVAL_MASK_DAY | INTERVAL_MASK_HOUR
	expectedDayHour := 8 | 1024 // 1032
	if dayHour != expectedDayHour {
		t.Errorf("DAY | HOUR = %d, expected %d", dayHour, expectedDayHour)
	}

	hourMinuteSecond := INTERVAL_MASK_HOUR | INTERVAL_MASK_MINUTE | INTERVAL_MASK_SECOND
	expectedHourMinuteSecond := 1024 | 2048 | 4096 // 7168
	if hourMinuteSecond != expectedHourMinuteSecond {
		t.Errorf("HOUR | MINUTE | SECOND = %d, expected %d", hourMinuteSecond, expectedHourMinuteSecond)
	}
}
