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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEmitQueryLog_NormalQuery(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	entry := queryLogEntry{
		User:          "testuser",
		Database:      "testdb",
		OperationName: "SELECT",
		Protocol:      "simple",
		TotalDuration: 50 * time.Millisecond,
		ParseDuration: 5 * time.Millisecond,
		ExecDuration:  45 * time.Millisecond,
		RowCount:      10,
	}

	emitQueryLog(context.Background(), logger, entry, time.Second)

	output := buf.String()
	require.Contains(t, output, "level=INFO")
	require.Contains(t, output, "query completed")
	require.Contains(t, output, "db.namespace=testdb")
	require.Contains(t, output, "db.operation.name=SELECT")
	require.Contains(t, output, "db.query.protocol=simple")
	require.Contains(t, output, "rows_returned=10")
	// Should not contain error fields for successful queries
	require.NotContains(t, output, "sqlstate")
}

func TestEmitQueryLog_ErrorQuery(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	entry := queryLogEntry{
		User:          "testuser",
		Database:      "testdb",
		OperationName: "SELECT",
		Protocol:      "simple",
		TotalDuration: 50 * time.Millisecond,
		Error:         errors.New("something failed"),
		SQLSTATE:      "XX000",
		ErrorSource:   "client",
	}

	emitQueryLog(context.Background(), logger, entry, time.Second)

	output := buf.String()
	require.Contains(t, output, "level=WARN")
	require.Contains(t, output, "sqlstate=XX000")
	require.Contains(t, output, "error.source=client")
}

func TestEmitQueryLog_SlowQuery(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	entry := queryLogEntry{
		User:          "testuser",
		Database:      "testdb",
		OperationName: "SELECT",
		Protocol:      "simple",
		TotalDuration: 2 * time.Second,
		RowCount:      1000,
	}

	emitQueryLog(context.Background(), logger, entry, time.Second)

	output := buf.String()
	require.Contains(t, output, "level=WARN")
}
