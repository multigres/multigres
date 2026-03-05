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
	"context"
	"log/slog"
	"time"
)

// queryLogEntry holds the fields emitted in a structured query log record.
type queryLogEntry struct {
	User          string
	Database      string
	OperationName string
	Protocol      string // "simple" or "extended"
	TotalDuration time.Duration
	ParseDuration time.Duration
	ExecDuration  time.Duration
	RowCount      int64
	Error         error
	SQLSTATE      string
	ErrorSource   string
}

// emitQueryLog writes a structured query log entry using slog.LogAttrs for
// minimal allocation on the latency-sensitive query path.
//
// Log levels:
//   - WARN  when the query errored or exceeded slowThreshold
//   - INFO  for normal queries
//
// The slog-OTel bridge (configured in telemetry.go) automatically injects
// trace_id and span_id from the context.
func emitQueryLog(ctx context.Context, logger *slog.Logger, entry queryLogEntry, slowThreshold time.Duration) {
	level := slog.LevelInfo
	if entry.Error != nil || entry.TotalDuration >= slowThreshold {
		level = slog.LevelWarn
	}

	attrs := []slog.Attr{
		slog.String("db.namespace", entry.Database),
		slog.String("db.operation.name", entry.OperationName),
		slog.String("db.query.protocol", entry.Protocol),
		slog.String("db.user", entry.User),
		slog.Float64("duration.total", entry.TotalDuration.Seconds()),
		slog.Float64("duration.parse", entry.ParseDuration.Seconds()),
		slog.Float64("duration.execute", entry.ExecDuration.Seconds()),
		slog.Int64("rows_returned", entry.RowCount),
	}

	if entry.Error != nil {
		attrs = append(attrs,
			slog.String("error", entry.Error.Error()),
			slog.String("sqlstate", entry.SQLSTATE),
			slog.String("error.source", entry.ErrorSource),
		)
	}

	if entry.TotalDuration >= slowThreshold {
		attrs = append(attrs, slog.Bool("slow_query", true))
	}

	logger.LogAttrs(ctx, level, "query completed", attrs...)
}
