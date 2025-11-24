// Copyright 2025 Supabase, Inc.
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

package fakepgdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
)

// fakeDriver implements driver.Driver
type fakeDriver struct {
	db *DB
}

// Open returns a new connection to the fake database.
func (d *fakeDriver) Open(name string) (driver.Conn, error) {
	return &fakeConn{db: d.db}, nil
}

// fakeConn implements driver.Conn
type fakeConn struct {
	db *DB
}

// Prepare returns a prepared statement, bound to this connection.
func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{conn: c, query: query}, nil
}

// Close closes the connection.
func (c *fakeConn) Close() error {
	return nil
}

// Begin starts and returns a new transaction.
func (c *fakeConn) Begin() (driver.Tx, error) {
	return &fakeTx{conn: c}, nil
}

// QueryContext executes a query that may return rows.
func (c *fakeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// Convert args to []interface{} for compatibility
	iargs := make([]interface{}, len(args))
	for i, arg := range args {
		iargs[i] = arg.Value
	}

	result, err := c.db.handleQuery(query)
	if err != nil {
		return nil, err
	}

	return &fakeRows{
		columns: result.Columns,
		rows:    result.Rows,
		index:   0,
	}, nil
}

// ExecContext executes a query that doesn't return rows.
func (c *fakeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Convert args to []interface{} for compatibility
	iargs := make([]interface{}, len(args))
	for i, arg := range args {
		iargs[i] = arg.Value
	}

	result, err := c.db.handleQuery(query)
	if err != nil {
		return nil, err
	}

	rowsAffected := int64(len(result.Rows))
	return &fakeResult{rowsAffected: rowsAffected}, nil
}

// fakeStmt implements driver.Stmt
type fakeStmt struct {
	conn  *fakeConn
	query string
}

// Close closes the statement.
func (s *fakeStmt) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *fakeStmt) NumInput() int {
	return -1 // -1 means the driver doesn't know
}

// Exec executes a query that doesn't return rows.
func (s *fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	result, err := s.conn.db.handleQuery(s.query)
	if err != nil {
		return nil, err
	}

	rowsAffected := int64(len(result.Rows))
	return &fakeResult{rowsAffected: rowsAffected}, nil
}

// Query executes a query that may return rows.
func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	result, err := s.conn.db.handleQuery(s.query)
	if err != nil {
		return nil, err
	}

	return &fakeRows{
		columns: result.Columns,
		rows:    result.Rows,
		index:   0,
	}, nil
}

// fakeTx implements driver.Tx
type fakeTx struct {
	conn *fakeConn
}

// Commit commits the transaction.
func (tx *fakeTx) Commit() error {
	return nil
}

// Rollback aborts the transaction.
func (tx *fakeTx) Rollback() error {
	return nil
}

// fakeResult implements driver.Result
type fakeResult struct {
	lastInsertId int64
	rowsAffected int64
}

// LastInsertId returns the database's auto-generated ID.
func (r *fakeResult) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

// RowsAffected returns the number of rows affected by the query.
func (r *fakeResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// fakeRows implements driver.Rows
type fakeRows struct {
	columns []string
	rows    [][]interface{}
	index   int
}

// Columns returns the names of the columns.
func (r *fakeRows) Columns() []string {
	return r.columns
}

// Close closes the rows iterator.
func (r *fakeRows) Close() error {
	return nil
}

// Next is called to populate the next row of data into the provided slice.
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}

	row := r.rows[r.index]
	r.index++

	if len(dest) != len(row) {
		return errors.New("fakepgdb: destination slice length doesn't match row length")
	}

	for i, val := range row {
		dest[i] = val
	}

	return nil
}

// Ensure interfaces are implemented
var (
	_ driver.Driver         = (*fakeDriver)(nil)
	_ driver.Conn           = (*fakeConn)(nil)
	_ driver.QueryerContext = (*fakeConn)(nil)
	_ driver.ExecerContext  = (*fakeConn)(nil)
	_ driver.Stmt           = (*fakeStmt)(nil)
	_ driver.Tx             = (*fakeTx)(nil)
	_ driver.Result         = (*fakeResult)(nil)
	_ driver.Rows           = (*fakeRows)(nil)
)
