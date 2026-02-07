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

// Package sqltypes provides internal types for query results that preserve
// NULL vs empty string distinction. These types are used throughout the
// codebase, while the proto types are only used for gRPC serialization.
package sqltypes

import "github.com/multigres/multigres/go/pb/query"

// Value represents a nullable column value.
// nil means NULL, []byte{} means empty string.
type Value []byte

// IsNull returns true if the value is NULL.
func (v Value) IsNull() bool {
	return v == nil
}

// Row represents a row with nullable column values.
type Row struct {
	// Values contains the column values. nil entry means NULL.
	Values []Value
}

// Notice represents a PostgreSQL notice response (non-fatal messages).
type Notice struct {
	Severity         string
	Code             string
	Message          string
	Detail           string
	Hint             string
	Position         int32
	InternalPosition int32
	InternalQuery    string
	Where            string
	Schema           string
	Table            string
	Column           string
	DataType         string
	Constraint       string
}

// Result represents a query result with nullable values.
type Result struct {
	// Fields describes the columns in the result set.
	Fields []*query.Field

	// RowsAffected is the number of rows affected (INSERT, UPDATE, DELETE, etc.)
	RowsAffected uint64

	// Rows contains the actual data rows.
	Rows []*Row

	// CommandTag is the PostgreSQL command tag for this result set.
	// Examples: "SELECT 42", "INSERT 0 5", "UPDATE 10", "DELETE 3"
	CommandTag string

	// Notices contains any PostgreSQL notices received during query execution.
	Notices []*Notice

	// ParameterStatus contains ParameterStatus messages received from the backend
	// during query execution (e.g., from SET commands that change session variables).
	ParameterStatus map[string]string
}

// ToProto converts Result to proto format for gRPC serialization.
func (r *Result) ToProto() *query.QueryResult {
	if r == nil {
		return nil
	}
	protoRows := make([]*query.Row, len(r.Rows))
	for i, row := range r.Rows {
		protoRows[i] = row.ToProto()
	}
	protoNotices := make([]*query.Notice, len(r.Notices))
	for i, notice := range r.Notices {
		protoNotices[i] = NoticeToProto(notice)
	}
	return &query.QueryResult{
		Fields:          r.Fields,
		RowsAffected:    r.RowsAffected,
		Rows:            protoRows,
		CommandTag:      r.CommandTag,
		Notices:         protoNotices,
		ParameterStatus: r.ParameterStatus,
	}
}

// ResultFromProto converts proto QueryResult to sqltypes Result.
func ResultFromProto(pr *query.QueryResult) *Result {
	if pr == nil {
		return nil
	}
	rows := make([]*Row, len(pr.Rows))
	for i, row := range pr.Rows {
		rows[i] = RowFromProto(row)
	}
	notices := make([]*Notice, len(pr.Notices))
	for i, notice := range pr.Notices {
		notices[i] = NoticeFromProto(notice)
	}
	return &Result{
		Fields:          pr.Fields,
		RowsAffected:    pr.RowsAffected,
		Rows:            rows,
		CommandTag:      pr.CommandTag,
		Notices:         notices,
		ParameterStatus: pr.ParameterStatus,
	}
}

// NoticeToProto converts sqltypes Notice to proto format for gRPC serialization.
func NoticeToProto(n *Notice) *query.Notice {
	if n == nil {
		return nil
	}
	return &query.Notice{
		Severity:         n.Severity,
		Code:             n.Code,
		Message:          n.Message,
		Detail:           n.Detail,
		Hint:             n.Hint,
		Position:         n.Position,
		InternalPosition: n.InternalPosition,
		InternalQuery:    n.InternalQuery,
		Where:            n.Where,
		SchemaName:       n.Schema,
		TableName:        n.Table,
		ColumnName:       n.Column,
		DataTypeName:     n.DataType,
		ConstraintName:   n.Constraint,
	}
}

// NoticeFromProto converts proto Notice to sqltypes Notice.
func NoticeFromProto(pn *query.Notice) *Notice {
	if pn == nil {
		return nil
	}
	return &Notice{
		Severity:         pn.Severity,
		Code:             pn.Code,
		Message:          pn.Message,
		Detail:           pn.Detail,
		Hint:             pn.Hint,
		Position:         pn.Position,
		InternalPosition: pn.InternalPosition,
		InternalQuery:    pn.InternalQuery,
		Where:            pn.Where,
		Schema:           pn.SchemaName,
		Table:            pn.TableName,
		Column:           pn.ColumnName,
		DataType:         pn.DataTypeName,
		Constraint:       pn.ConstraintName,
	}
}

// ToProto converts Row to proto format (lengths+values) for gRPC serialization.
// Encoding: -1 = NULL, 0 = empty string, >0 = actual length.
func (r *Row) ToProto() *query.Row {
	if r == nil {
		return nil
	}

	lengths := make([]int64, len(r.Values))
	var totalLen int
	for i, v := range r.Values {
		if v == nil {
			lengths[i] = -1
		} else {
			lengths[i] = int64(len(v))
			totalLen += len(v)
		}
	}

	values := make([]byte, 0, totalLen)
	for _, v := range r.Values {
		if v != nil {
			values = append(values, v...)
		}
	}

	return &query.Row{
		Lengths: lengths,
		Values:  values,
	}
}

// RowFromProto converts proto Row (lengths+values) to sqltypes Row.
// Decoding: -1 = NULL, 0 = empty string, >0 = actual length.
func RowFromProto(pr *query.Row) *Row {
	if pr == nil {
		return nil
	}

	values := make([]Value, len(pr.Lengths))
	offset := 0
	for i, length := range pr.Lengths {
		switch length {
		case -1:
			values[i] = nil // NULL
		case 0:
			values[i] = []byte{} // empty string, not NULL
		default:
			values[i] = pr.Values[offset : offset+int(length)]
			offset += int(length)
		}
	}

	return &Row{Values: values}
}

// MakeRow creates a new Row from a slice of byte slices.
// nil entries represent NULL values.
func MakeRow(values [][]byte) *Row {
	row := &Row{
		Values: make([]Value, len(values)),
	}
	for i, v := range values {
		if v == nil {
			row.Values[i] = nil
		} else {
			row.Values[i] = Value(v)
		}
	}
	return row
}
