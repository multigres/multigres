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

package pgerror_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

func TestEreportfRecovered(t *testing.T) {
	err := pgerror.Recover(func() {
		pgerror.Ereportf("22003", "integer out of range")
	})
	require.Error(t, err)
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag))
	assert.Equal(t, "22003", diag.Code)
	assert.Equal(t, "integer out of range", diag.Message)
	assert.Equal(t, "ERROR", diag.Severity)
}

func TestEreportfFormatsMessage(t *testing.T) {
	err := pgerror.Recover(func() {
		pgerror.Ereportf("22P02", "bad value %q at position %d", "abc", 3)
	})
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag))
	assert.Equal(t, `bad value "abc" at position 3`, diag.Message)
	assert.Equal(t, "22P02", diag.Code)
}

// TestEreportWithDetailAndHint covers the fuller path: build the diagnostic,
// set the optional fields, raise it.
func TestEreportWithDetailAndHint(t *testing.T) {
	err := pgerror.Recover(func() {
		diag := mterrors.NewPgError("ERROR", "22012", "division by zero", "detail here")
		diag.Hint = "try harder"
		pgerror.Ereport(diag)
	})
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag))
	assert.Equal(t, "division by zero", diag.Message)
	assert.Equal(t, "detail here", diag.Detail)
	assert.Equal(t, "try harder", diag.Hint)
}

func TestElogf(t *testing.T) {
	err := pgerror.Recover(func() {
		pgerror.Elogf("function %d returned NULL", 177)
	})
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag))
	assert.Equal(t, mterrors.PgSSInternalError, diag.Code)
	assert.Equal(t, "function 177 returned NULL", diag.Message)
}

func TestRecoverNoPanicReturnsNil(t *testing.T) {
	ran := false
	err := pgerror.Recover(func() { ran = true })
	assert.NoError(t, err)
	assert.True(t, ran)
}

// TestRecoverRepanicsRealPanic proves a genuine Go bug is not disguised as a
// SQL error: only *mterrors.PgDiagnostic panics are converted to errors.
func TestRecoverRepanicsRealPanic(t *testing.T) {
	assert.PanicsWithValue(t, "genuine bug", func() {
		_ = pgerror.Recover(func() {
			panic("genuine bug")
		})
	})
}

func TestRecoverRepanicsRuntimePanic(t *testing.T) {
	assert.Panics(t, func() {
		_ = pgerror.Recover(func() {
			s := make([]int, 0)
			_ = s[index()] // runtime panic (index out of range), not an ereport
		})
	})
}

// index returns 5 through a function so the out-of-range access is a genuine
// runtime panic rather than one the static analyzer flags.
func index() int { return 5 }
