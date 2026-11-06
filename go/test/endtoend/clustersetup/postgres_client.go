// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clustersetup

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/sqltypes"
)

// PostgresClient runs statements directly on PostgreSQL, for harnesses where
// the multipooler's query service is not reachable (minigres).
type PostgresClient struct {
	db *sql.DB
}

// NewPostgresClient connects to localhost:port as the test user.
func NewPostgresClient(port int) (*PostgresClient, error) {
	db, err := sql.Open("postgres", GetTestUserDSN("localhost", port, "sslmode=disable", "connect_timeout=5"))
	if err != nil {
		return nil, fmt.Errorf("open postgres on port %d: %w", port, err)
	}
	return &PostgresClient{db: db}, nil
}

// ExecuteQuery returns up to maxRows rows (0 for all) in PostgreSQL's text
// format, as the multipooler's query service does; NULL is nil.
func (c *PostgresClient) ExecuteQuery(ctx context.Context, query string, maxRows uint64) (*sqltypes.Result, error) {
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result := &sqltypes.Result{}
	for rows.Next() {
		if maxRows > 0 && uint64(len(result.Rows)) >= maxRows {
			break
		}
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		row := &sqltypes.Row{Values: make([]sqltypes.Value, len(columns))}
		for i, v := range values {
			if v != nil {
				row.Values[i] = sqltypes.Value(postgresText(v))
			}
		}
		result.Rows = append(result.Rows, row)
	}
	return result, rows.Err()
}

func (c *PostgresClient) Close() error {
	return c.db.Close()
}

// postgresText formats a driver-decoded value in PostgreSQL's text format.
func postgresText(v any) string {
	switch v := v.(type) {
	case []byte:
		return string(v)
	case bool:
		if v {
			return "t"
		}
		return "f"
	case time.Time:
		return v.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(v)
	}
}
