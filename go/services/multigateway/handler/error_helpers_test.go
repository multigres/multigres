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

package handler

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
)

func TestExtractSQLSTATE(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "nil error",
			err:  nil,
			want: "",
		},
		{
			name: "PgDiagnostic with real SQLSTATE",
			err:  mterrors.NewPgError("ERROR", "42P01", "relation does not exist", ""),
			want: "42P01",
		},
		{
			name: "PgDiagnostic with MT code",
			err:  mterrors.MTD01.New("something"),
			want: "MTD01",
		},
		{
			name: "wrapped PgDiagnostic",
			err:  fmt.Errorf("context: %w", mterrors.NewPgError("ERROR", "23505", "unique violation", "")),
			want: "23505",
		},
		{
			name: "non-PG error",
			err:  io.EOF,
			want: "XX000",
		},
		{
			name: "plain error",
			err:  errors.New("something broke"),
			want: "XX000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractSQLSTATE(tt.err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestClassifyErrorSource(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "nil error",
			err:  nil,
			want: "",
		},
		{
			name: "real PG SQLSTATE → backend",
			err:  mterrors.NewPgError("ERROR", "42P01", "relation does not exist", ""),
			want: "backend",
		},
		{
			name: "MT-prefixed code → internal",
			err:  mterrors.MTD01.New("bug"),
			want: "internal",
		},
		{
			name: "connection error (EOF) → routing",
			err:  io.EOF,
			want: "routing",
		},
		{
			name: "Class 08 connection exception → routing",
			err:  mterrors.NewPgError("FATAL", "08006", "connection failure", ""),
			want: "routing",
		},
		{
			name: "plain error → client",
			err:  errors.New("callback error"),
			want: "client",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyErrorSource(tt.err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestExtractOperationName(t *testing.T) {
	tests := []struct {
		name string
		stmt ast.Stmt
		want string
	}{
		{
			name: "nil stmt",
			stmt: nil,
			want: "UNKNOWN",
		},
		{
			name: "SELECT",
			stmt: ast.NewSelectStmt(),
			want: "SELECT",
		},
		{
			name: "INSERT",
			stmt: ast.NewInsertStmt(ast.NewRangeVar("t", "", "")),
			want: "INSERT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractOperationName(tt.stmt)
			require.Equal(t, tt.want, got)
		})
	}
}
